// +build linux

package overlay

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"

	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/chrootarchive"
	"github.com/docker/docker/pkg/directory"
	"github.com/docker/docker/pkg/idtools"
	"github.com/docker/docker/pkg/parsers/kernel"

	"github.com/docker/docker/pkg/mount"
	"github.com/opencontainers/runc/libcontainer/label"
)

var (
	// untar defines the untar method
	untar = chrootarchive.UntarUncompressed
)

// This backend uses the overlay union filesystem for containers
// plus hard link file sharing for images.

// Each container/image can have a "root" subdirectory which is a plain
// filesystem hierarchy, or they can use overlay.

// If they use overlay there is a "diff" directory and a "lower" file,
// as well as "merged" and "work" directories. The "diff" directory
// has the upper layer of the overlay and is used to capture any
// changes to the layer. The "lower" file contains all the lower layer
// mounts separated by ":" and ordered from uppermost to lowermost
// layers. The overlay itself is mounted in the "merged" directory,
// and the "work" dir is needed for overlay to work.

// When the list of "lower" directories gets too long to use for a
// single overlay mount, the driver will merge multiple "diff"
// directories into an "upper" directory. These merged layers will
// still contain the diff directory and use a "lower-id" file which
// is compatible with the overlay driver implementation used in
// Docker 1.11 and before.

// If a kernel older than 4.0.0 is in use or the "nomultilower" option
// is given, the overlay driver will behave the same as in Docker
// 1.11 and before. In this mode, overlay does not allow multiple
// "diff" directories to be used as overlay lower directories.
// Overlay directories will contain an "upper" directory and a
// "lower-id" file. The "upper" directory has the upper layer of the
// overlay, and "lower-id" contains the id of the parent whose "root"
// directory shall be used as the lower layer in the overlay. As
// in the other, the overlay itself is mounted in the "merged"
// directory, and the "work" dir is needed for overlay to work.

// When a overlay layer is created there are two cases, either the
// parent has a "root" dir, then we start out with a empty "upper"
// directory overlaid on the parents root. This is typically the
// case with the init layer of a container which is based on an image.
// If there is no "root" in the parent, we inherit the lower-id from
// the parent and start by making a copy in the parent's "upper" dir.
// This is typically the case for a container layer which copies
// its parent -init upper layer.

// Additionally we also have a custom implementation of ApplyLayer
// which makes a recursive copy of the parent "root" layer using
// hardlinks to share file data, and then applies the layer on top
// of that. This means all child images share file (but not directory)
// data with the parent.

// Driver contains information about the home directory and the list of active mounts that are created using this driver.
type Driver struct {
	home          string
	pathCacheLock sync.Mutex
	pathCache     map[string]string
	uidMaps       []idtools.IDMap
	gidMaps       []idtools.IDMap
	ctr           *graphdriver.RefCounter
	multiLower    bool
	squashLevel   int

	naiveDiffDriver graphdriver.Driver
}

var backingFs = "<unknown>"

func init() {
	graphdriver.Register("overlay", Init)
}

// Init returns the NaiveDiffDriver, a native diff driver for overlay filesystem.
// If overlay filesystem is not supported on the host, graphdriver.ErrNotSupported is returned as error.
// If a overlay filesystem is not supported over a existing filesystem then error graphdriver.ErrIncompatibleFS is returned.
func Init(home string, options []string, uidMaps, gidMaps []idtools.IDMap) (graphdriver.Driver, error) {

	if err := supportsOverlay(); err != nil {
		return nil, graphdriver.ErrNotSupported
	}

	fsMagic, err := graphdriver.GetFSMagic(home)
	if err != nil {
		return nil, err
	}
	if fsName, ok := graphdriver.FsNames[fsMagic]; ok {
		backingFs = fsName
	}

	// check if they are running over btrfs, aufs, zfs or overlay
	switch fsMagic {
	case graphdriver.FsMagicBtrfs:
		logrus.Error("'overlay' is not supported over btrfs.")
		return nil, graphdriver.ErrIncompatibleFS
	case graphdriver.FsMagicAufs:
		logrus.Error("'overlay' is not supported over aufs.")
		return nil, graphdriver.ErrIncompatibleFS
	case graphdriver.FsMagicZfs:
		logrus.Error("'overlay' is not supported over zfs.")
		return nil, graphdriver.ErrIncompatibleFS
	case graphdriver.FsMagicOverlay:
		logrus.Error("'overlay' is not supported over overlay.")
		return nil, graphdriver.ErrIncompatibleFS
	}

	rootUID, rootGID, err := idtools.GetRootUIDGID(uidMaps, gidMaps)
	if err != nil {
		return nil, err
	}
	// Create the driver home dir
	if err := idtools.MkdirAllAs(home, 0700, rootUID, rootGID); err != nil && !os.IsExist(err) {
		return nil, err
	}

	if err := mount.MakePrivate(home); err != nil {
		return nil, err
	}

	d := &Driver{
		home:      home,
		pathCache: make(map[string]string),
		uidMaps:   uidMaps,
		gidMaps:   gidMaps,
		ctr:       graphdriver.NewRefCounter(),
	}

	if !noMultiLowerOption(options) {
		v, err := kernel.GetKernelVersion()
		if err != nil {
			return nil, err
		}
		if kernel.CompareKernelVersion(*v, kernel.VersionInfo{Kernel: 4, Major: 0, Minor: 0}) >= 0 {
			d.multiLower = true

			// TODO: Make squash level configurable
			// layerArgSize represents the id size (64 bytes), plus diff directory and separators
			layerArgSize := 71 + len(d.home)
			// Add buffer for layer mount key names, upper and work directories, and mount label
			d.squashLevel = (syscall.Getpagesize() - 256 - 2*layerArgSize) / layerArgSize
			logrus.Debugf("Setting squash level to %d", d.squashLevel)
		}
	}

	d.naiveDiffDriver = graphdriver.NewNaiveDiffDriver(d, uidMaps, gidMaps)

	return d, nil
}

func noMultiLowerOption(options []string) bool {
	for _, option := range options {
		if option == "nomultilower" {
			return true
		}
	}
	return false
}

func supportsOverlay() error {
	// We can try to modprobe overlay first before looking at
	// proc/filesystems for when overlay is supported
	exec.Command("modprobe", "overlay").Run()

	f, err := os.Open("/proc/filesystems")
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if s.Text() == "nodev\toverlay" {
			return nil
		}
	}
	logrus.Error("'overlay' not found as a supported filesystem on this host. Please ensure kernel is new enough and has overlay support loaded.")
	return graphdriver.ErrNotSupported
}

func (d *Driver) String() string {
	return "overlay"
}

// Status returns current driver information in a two dimensional string array.
// Output contains "Backing Filesystem" used in this implementation.
func (d *Driver) Status() [][2]string {
	return [][2]string{
		{"Backing Filesystem", backingFs},
	}
}

// GetMetadata returns meta data about the overlay driver such as root, LowerDir, UpperDir, WorkDir and MergeDir used to store data.
func (d *Driver) GetMetadata(id string) (map[string]string, error) {
	dir := d.dir(id)
	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}

	metadata := make(map[string]string)

	// If id has a root, it is an image
	rootDir := path.Join(dir, "root")
	if _, err := os.Stat(rootDir); err == nil {
		metadata["RootDir"] = rootDir
		return metadata, nil
	}

	// Check if has lower-dirs
	metadata["WorkDir"] = path.Join(dir, "work")
	metadata["MergedDir"] = path.Join(dir, "merged")

	lowerID, err := ioutil.ReadFile(path.Join(dir, "lower-id"))
	if err != nil {
		return nil, err
	}

	diffDir := path.Join(dir, "diff")
	if _, err := os.Lstat(diffDir); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		metadata["LowerDir"] = path.Join(d.dir(string(lowerID)), "root")
		metadata["UpperDir"] = path.Join(dir, "upper")

		return metadata, nil
	}

	lowerDirs, err := d.getLowerDirs(id, string(lowerID))
	if err != nil {
		return nil, err
	}
	metadata["LowerDir"] = strings.Join(lowerDirs, ":")
	metadata["UpperDir"] = path.Join(dir, "diff")

	return metadata, nil
}

// Cleanup any state created by overlay which should be cleaned when daemon
// is being shutdown. For now, we just have to unmount the bind mounted
// we had created.
func (d *Driver) Cleanup() error {
	return mount.Unmount(d.home)
}

// CreateReadWrite creates a layer that is writable for use as a container
// file system.
func (d *Driver) CreateReadWrite(id, parent, mountLabel string, storageOpt map[string]string) error {
	return d.Create(id, parent, mountLabel, storageOpt)
}

// Create is used to create the upper, lower, and merge directories required for overlay fs for a given id.
// The parent filesystem is used to configure these directories for the overlay.
func (d *Driver) Create(id, parent, mountLabel string, storageOpt map[string]string) (retErr error) {

	if len(storageOpt) != 0 {
		return fmt.Errorf("--storage-opt is not supported for overlay")
	}

	dir := d.dir(id)

	rootUID, rootGID, err := idtools.GetRootUIDGID(d.uidMaps, d.gidMaps)
	if err != nil {
		return err
	}
	if err := idtools.MkdirAllAs(path.Dir(dir), 0700, rootUID, rootGID); err != nil {
		return err
	}
	if err := idtools.MkdirAs(dir, 0700, rootUID, rootGID); err != nil {
		return err
	}

	defer func() {
		// Clean up on failure
		if retErr != nil {
			os.RemoveAll(dir)
		}
	}()

	// Toplevel images are just a "root" dir
	if parent == "" {
		if err := idtools.MkdirAs(path.Join(dir, "root"), 0755, rootUID, rootGID); err != nil {
			return err
		}
		return nil
	}

	parentDir := d.dir(parent)

	// Ensure parent exists
	if _, err := os.Lstat(parentDir); err != nil {
		return err
	}

	if err := idtools.MkdirAs(path.Join(dir, "work"), 0700, rootUID, rootGID); err != nil {
		return err
	}
	if err := idtools.MkdirAs(path.Join(dir, "merged"), 0700, rootUID, rootGID); err != nil {
		return err
	}

	// If parent has a root, just do a overlay to it
	parentRoot := path.Join(parentDir, "root")
	if s, err := os.Lstat(parentRoot); err == nil {
		if !d.multiLower {
			if err := idtools.MkdirAs(path.Join(dir, "upper"), s.Mode(), rootUID, rootGID); err != nil {
				return err
			}
		} else {
			if err := idtools.MkdirAs(path.Join(dir, "diff"), s.Mode(), rootUID, rootGID); err != nil {
				return err
			}
		}

		if err := ioutil.WriteFile(path.Join(dir, "lower-id"), []byte(parent), 0666); err != nil {
			return err
		}
		return nil
	}

	// Otherwise, create lower file or copy the upper and the lower-id from the parent

	lowerID, err := ioutil.ReadFile(path.Join(parentDir, "lower-id"))
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(path.Join(dir, "lower-id"), lowerID, 0666); err != nil {
		return err
	}

	if !d.multiLower {
		parentUpperDir := path.Join(parentDir, "upper")
		s, err := os.Lstat(parentUpperDir)
		if err != nil {
			return err
		}

		upperDir := path.Join(dir, "upper")
		if err := idtools.MkdirAs(upperDir, s.Mode(), rootUID, rootGID); err != nil {
			return err
		}

		return copyDir(parentUpperDir, upperDir, 0)
	}

	lower, err := d.getLower(parent)
	if err != nil {
		return err
	}
	if lower != "" {
		if err := ioutil.WriteFile(path.Join(dir, "lower"), []byte(lower), 0666); err != nil {
			return err
		}
	}
	if err := idtools.MkdirAs(path.Join(dir, "diff"), 0755, rootUID, rootGID); err != nil {
		return err
	}

	return nil
}

func (d *Driver) squashLayers(layerDirs []string) (string, error) {
	st, err := os.Lstat(path.Join(d.home, layerDirs[0]))
	if err != nil {
		return "", err
	}
	layerid := path.Base(path.Dir(layerDirs[0]))

	dir := d.dir(layerid)
	upperDir := path.Join(d.dir(layerid), "upper")
	if _, err := os.Stat(upperDir); err == nil {
		return path.Join(layerid, "upper"), nil
	}

	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("unable to get raw syscall.Stat_t data for %s", layerDirs[0])
	}

	tmpUpperDir, err := ioutil.TempDir(dir, "tmpupper")
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			os.RemoveAll(tmpUpperDir)
		}
	}()
	if err = os.Chown(tmpUpperDir, int(stat.Uid), int(stat.Gid)); err != nil {
		return "", err
	}
	if err = os.Chmod(tmpUpperDir, st.Mode()); err != nil {
		return "", err
	}
	for i := len(layerDirs) - 1; i >= 0; i-- {
		// Use hardlinks to copy since this upper directory will not be written
		// to and copying will increase disk space and copy time. If this directory
		// is mounted in the future the diff directory will still be used protecting
		// writes to this upper layer. Should the directory be named differently to
		// ensure that a downgrade cannot write here or express this upper could
		// get out of sync with the diff directory?
		if err = copyDir(path.Join(d.home, layerDirs[i]), tmpUpperDir, copyHardlink&overwrite); err != nil {
			return "", err
		}
	}
	if err = os.Rename(tmpUpperDir, upperDir); err != nil {
		return "", err
	}
	return path.Join(layerid, "upper"), nil
}

func (d *Driver) getLower(parent string) (string, error) {
	parentDir := d.dir(parent)

	// Ensure parent exists
	if _, err := os.Lstat(parentDir); err != nil {
		return "", err
	}

	// If parent has a root, no lower directories
	parentRoot := path.Join(parentDir, "root")
	if _, err := os.Lstat(parentRoot); err == nil {
		return "", nil
	}

	// If parent has diff directory, use with lower file
	diffDir := path.Join(parentDir, "diff")
	if _, err := os.Lstat(diffDir); err == nil {
		lowersArray := []string{path.Join(parent, "diff")}
		lowers, err := ioutil.ReadFile(path.Join(parentDir, "lower"))
		if err == nil {
			parentLowers := strings.Split(string(lowers), ":")
			if len(parentLowers) >= d.squashLevel {
				squashIndex := int(0.8 * float64(d.squashLevel))
				lowersArray = append(lowersArray, parentLowers[:squashIndex]...)
				squashed, err := d.squashLayers(parentLowers[squashIndex:])
				if err != nil {
					return "", err
				}
				logrus.Debugf("Squashing %d layer into %s", len(parentLowers[squashIndex:]), squashed)
				lowersArray = append(lowersArray, squashed)
			} else {
				lowersArray = append(lowersArray, parentLowers...)
			}
		}
		return strings.Join(lowersArray, ":"), nil
	}

	// Otherwise handle legacy overlay upper directory, copy the upper and the lower-id from the parent

	parentUpperDir := path.Join(parentDir, "upper")
	if _, err := os.Lstat(parentUpperDir); err != nil {
		return "", err
	}

	return path.Join(parent, "upper"), nil
}

func (d *Driver) dir(id string) string {
	return path.Join(d.home, id)
}

func (d *Driver) getLowerDirs(id, lowerID string) ([]string, error) {
	var lowersArray []string
	lowers, err := ioutil.ReadFile(path.Join(d.dir(id), "lower"))
	if err == nil {
		for _, s := range strings.Split(string(lowers), ":") {
			lowersArray = append(lowersArray, path.Join(d.home, s))
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	lowersArray = append(lowersArray, path.Join(d.dir(lowerID), "root"))
	return lowersArray, nil
}

// Remove cleans the directories that are created for this id.
func (d *Driver) Remove(id string) error {
	if err := os.RemoveAll(d.dir(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	d.pathCacheLock.Lock()
	delete(d.pathCache, id)
	d.pathCacheLock.Unlock()
	return nil
}

// Get creates and mounts the required file system for the given id and returns the mount path.
func (d *Driver) Get(id string, mountLabel string) (string, error) {
	dir := d.dir(id)
	if _, err := os.Stat(dir); err != nil {
		return "", err
	}

	// If id has a root, just return it
	rootDir := path.Join(dir, "root")
	if _, err := os.Stat(rootDir); err == nil {
		d.pathCacheLock.Lock()
		d.pathCache[id] = rootDir
		d.pathCacheLock.Unlock()
		return rootDir, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	lowerID, err := ioutil.ReadFile(path.Join(dir, "lower-id"))
	if err != nil {
		return "", err
	}

	var (
		lowerDirs string
		upperDir  string
	)

	// If has diff directory, mount diff and lower diff directories
	diffDir := path.Join(dir, "diff")
	if _, err := os.Stat(diffDir); err == nil {
		lowers, err := d.getLowerDirs(id, string(lowerID))
		if err != nil {
			return "", err
		}
		lowerDirs = strings.Join(lowers, ":")
		upperDir = path.Join(dir, "diff")
	} else if os.IsNotExist(err) {
		// Handle legacy overlay directories
		lowerDirs = path.Join(d.dir(string(lowerID)), "root")
		upperDir = path.Join(dir, "upper")
	} else {
		return "", err
	}

	workDir := path.Join(dir, "work")
	mergedDir := path.Join(dir, "merged")

	if count := d.ctr.Increment(id); count > 1 {
		return mergedDir, nil
	}

	// if it's mounted already, just return
	mounted, err := d.mounted(mergedDir)
	if err != nil {
		d.ctr.Decrement(id)
		return "", err
	}
	if mounted {
		d.ctr.Decrement(id)
		return mergedDir, nil
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerDirs, upperDir, workDir)
	mountLabel = label.FormatMountLabel(opts, mountLabel)
	if len(mountLabel) > syscall.Getpagesize() {
		d.ctr.Decrement(id)
		return "", fmt.Errorf("cannot mount layer, mount label too large %d", len(mountLabel))
	}
	if err := syscall.Mount("overlay", mergedDir, "overlay", 0, mountLabel); err != nil {
		d.ctr.Decrement(id)
		return "", fmt.Errorf("error creating overlay mount to %s: %v", mergedDir, err)
	}
	// chown "workdir/work" to the remapped root UID/GID. Overlay fs inside a
	// user namespace requires this to move a directory from lower to upper.
	rootUID, rootGID, err := idtools.GetRootUIDGID(d.uidMaps, d.gidMaps)
	if err != nil {
		d.ctr.Decrement(id)
		syscall.Unmount(mergedDir, 0)
		return "", err
	}

	if err := os.Chown(path.Join(workDir, "work"), rootUID, rootGID); err != nil {
		d.ctr.Decrement(id)
		syscall.Unmount(mergedDir, 0)
		return "", err
	}

	d.pathCacheLock.Lock()
	d.pathCache[id] = mergedDir
	d.pathCacheLock.Unlock()

	return mergedDir, nil
}

func (d *Driver) mounted(dir string) (bool, error) {
	return graphdriver.Mounted(graphdriver.FsMagicOverlay, dir)
}

// Put unmounts the mount path created for the give id.
func (d *Driver) Put(id string) error {
	if count := d.ctr.Decrement(id); count > 0 {
		return nil
	}
	d.pathCacheLock.Lock()
	mountpoint, exists := d.pathCache[id]
	d.pathCacheLock.Unlock()

	if !exists {
		logrus.Debugf("Put on a non-mounted device %s", id)
		// but it might be still here
		if d.Exists(id) {
			mountpoint = path.Join(d.dir(id), "merged")
		}

		d.pathCacheLock.Lock()
		d.pathCache[id] = mountpoint
		d.pathCacheLock.Unlock()
	}

	if mounted, err := d.mounted(mountpoint); mounted || err != nil {
		if err = syscall.Unmount(mountpoint, 0); err != nil {
			logrus.Debugf("Failed to unmount %s overlay: %v", id, err)
		}
		return err
	}
	return nil
}

// Exists checks to see if the id is already mounted.
func (d *Driver) Exists(id string) bool {
	_, err := os.Stat(d.dir(id))
	return err == nil
}

func (d *Driver) classicApplyDiff(id string, parent string, diff archive.Reader) (size int64, err error) {
	dir := d.dir(id)

	if parent == "" {
		return d.naiveDiffDriver.ApplyDiff(id, parent, diff)
	}

	parentRootDir := path.Join(d.dir(parent), "root")
	if _, err := os.Stat(parentRootDir); err != nil {
		return d.naiveDiffDriver.ApplyDiff(id, parent, diff)
	}

	// We now know there is a parent, and it has a "root" directory containing
	// the full root filesystem. We can just hardlink it and apply the
	// layer. This relies on two things:
	// 1) ApplyDiff is only run once on a clean (no writes to upper layer) container
	// 2) ApplyDiff doesn't do any in-place writes to files (would break hardlinks)
	// These are all currently true and are not expected to break

	tmpRootDir, err := ioutil.TempDir(dir, "tmproot")
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			os.RemoveAll(tmpRootDir)
		} else {
			os.RemoveAll(path.Join(dir, "upper"))
			os.RemoveAll(path.Join(dir, "work"))
			os.RemoveAll(path.Join(dir, "merged"))
			os.RemoveAll(path.Join(dir, "lower-id"))
		}
	}()

	if err = copyDir(parentRootDir, tmpRootDir, copyHardlink); err != nil {
		return 0, err
	}

	options := &archive.TarOptions{UIDMaps: d.uidMaps, GIDMaps: d.gidMaps}
	if size, err = graphdriver.ApplyUncompressedLayer(tmpRootDir, diff, options); err != nil {
		return 0, err
	}

	rootDir := path.Join(dir, "root")
	if err := os.Rename(tmpRootDir, rootDir); err != nil {
		return 0, err
	}

	return
}

// ApplyDiff applies the new layer into a root
func (d *Driver) ApplyDiff(id string, parent string, diff archive.Reader) (size int64, err error) {
	if !d.multiLower {
		return d.classicApplyDiff(id, parent, diff)
	}
	dir := d.dir(id)

	var applyDir string
	if parent == "" {
		applyDir = path.Join(path.Join(dir, "root"))
	} else {
		applyDir = path.Join(path.Join(dir, "diff"))

	}

	logrus.Debugf("Applying tar in %s", applyDir)
	// Overlay doesn't need the parent id to apply the diff
	if err := untar(diff, applyDir, &archive.TarOptions{
		UIDMaps:        d.uidMaps,
		GIDMaps:        d.gidMaps,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
	}); err != nil {
		return 0, err
	}

	return d.DiffSize(id, parent)
}

func (d *Driver) getDiffPath(id, parent string) string {
	dir := d.dir(id)

	diffPath := path.Join(dir, "diff")
	if _, err := os.Stat(diffPath); err == nil {
		return diffPath
	}

	// Check if upper and lower-id == parent
	diffPath = path.Join(dir, "upper")
	if _, err := os.Stat(diffPath); err == nil {
		lowerID, err := ioutil.ReadFile(path.Join(dir, "lower-id"))
		if err != nil {
			return ""
		}
		if parent == string(lowerID) {
			return diffPath
		}
	}

	return ""
}

// DiffSize calculates the changes between the specified id
// and its parent and returns the size in bytes of the changes
// relative to its base filesystem directory.
func (d *Driver) DiffSize(id, parent string) (size int64, err error) {
	diffPath := d.getDiffPath(id, parent)
	if diffPath != "" {
		return directory.Size(diffPath)
	}

	return d.naiveDiffDriver.DiffSize(id, parent)
}

// Diff produces an archive of the changes between the specified
// layer and its parent layer which may be "".
func (d *Driver) Diff(id, parent string) (archive.Archive, error) {
	diffPath := d.getDiffPath(id, parent)
	if diffPath != "" {
		logrus.Debugf("Tar with options on %s", diffPath)
		return archive.TarWithOptions(diffPath, &archive.TarOptions{
			Compression:    archive.Uncompressed,
			UIDMaps:        d.uidMaps,
			GIDMaps:        d.gidMaps,
			WhiteoutFormat: archive.OverlayWhiteoutFormat,
		})
	}

	return d.naiveDiffDriver.Diff(id, parent)
}

func (d *Driver) getParentLayerPaths(id string) ([]string, error) {
	lowerID, err := ioutil.ReadFile(path.Join(d.dir(id), "lower-id"))
	if err != nil {
		return nil, err
	}

	return d.getLowerDirs(id, string(lowerID))
}

// Changes produces a list of changes between the specified layer
// and its parent layer. If parent is "", then all changes will be ADD changes.
func (d *Driver) Changes(id, parent string) ([]archive.Change, error) {
	// Overlay doesn't have snapshots, so we need to get changes from all parent
	// layers.
	diffPath := d.getDiffPath(id, parent)
	if diffPath != "" {
		layers, err := d.getParentLayerPaths(id)
		if err != nil {
			return nil, err
		}

		return archive.OverlayChanges(layers, diffPath)
	}

	return d.naiveDiffDriver.Changes(id, parent)
}
