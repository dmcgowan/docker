// +build linux

package overlay2

import (
	"bufio"
	"errors"
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
// with diff directories for each layer.

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

const (
	driverName = "overlay2"
	linkDir    = "l"
	lowerFile  = "lower"
	maxDepth   = 128

	// idLength represents the number of random characters
	// which can be used to create the unique link identifer
	// for every layer. If this value is too long then the
	// page size limit for the mount command may be exceeded.
	// The idLength should be selected such that following equation
	// is true (512 is a buffer for label metadata).
	// ((idLength + len(linkDir) + 1) * maxDepth) <= (pageSize - 512)
	idLength = 26
)

// Driver contains information about the home directory and the list of active mounts that are created using this driver.
type Driver struct {
	home          string
	pathCacheLock sync.Mutex
	pathCache     map[string]string
	uidMaps       []idtools.IDMap
	gidMaps       []idtools.IDMap
	ctr           *graphdriver.RefCounter
}

var backingFs = "<unknown>"

func init() {
	graphdriver.Register(driverName, Init)
}

// Init returns the a native diff driver for overlay filesystem.
// If overlay filesystem is not supported on the host, graphdriver.ErrNotSupported is returned as error.
// If a overlay filesystem is not supported over a existing filesystem then error graphdriver.ErrIncompatibleFS is returned.
func Init(home string, options []string, uidMaps, gidMaps []idtools.IDMap) (graphdriver.Driver, error) {

	if err := supportsOverlay(); err != nil {
		return nil, graphdriver.ErrNotSupported
	}

	v, err := kernel.GetKernelVersion()
	if err != nil {
		return nil, err
	}
	if kernel.CompareKernelVersion(*v, kernel.VersionInfo{Kernel: 4, Major: 0, Minor: 0}) < 0 {
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
	if err := idtools.MkdirAllAs(path.Join(home, linkDir), 0700, rootUID, rootGID); err != nil && !os.IsExist(err) {
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

	return d, nil
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
	return driverName
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

	metadata := map[string]string{
		"WorkDir":   path.Join(dir, "work"),
		"MergedDir": path.Join(dir, "merged"),
		"UpperDir":  path.Join(dir, "diff"),
	}

	lowerDirs, err := d.getLowerDirs(id)
	if err != nil {
		return nil, err
	}
	if len(lowerDirs) > 0 {
		metadata["LowerDir"] = strings.Join(lowerDirs, ":")
	}

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

	if err := idtools.MkdirAs(path.Join(dir, "diff"), 0755, rootUID, rootGID); err != nil {
		return err
	}

	lid := generateID(idLength)
	if err := os.Symlink(path.Join("..", id, "diff"), path.Join(d.home, linkDir, lid)); err != nil {
		return err
	}

	// Write link id to link file
	if err := ioutil.WriteFile(path.Join(dir, "link"), []byte(lid), 0644); err != nil {
		return err
	}

	// if no parent directory, done
	if parent == "" {
		return nil
	}

	if err := idtools.MkdirAs(path.Join(dir, "work"), 0700, rootUID, rootGID); err != nil {
		return err
	}
	if err := idtools.MkdirAs(path.Join(dir, "merged"), 0700, rootUID, rootGID); err != nil {
		return err
	}

	lower, err := d.getLower(parent)
	if err != nil {
		return err
	}
	if lower != "" {
		if err := ioutil.WriteFile(path.Join(dir, lowerFile), []byte(lower), 0666); err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) getLower(parent string) (string, error) {
	parentDir := d.dir(parent)

	// Ensure parent exists
	if _, err := os.Lstat(parentDir); err != nil {
		return "", err
	}

	// Read Parent link fileA
	parentLink, err := ioutil.ReadFile(path.Join(parentDir, "link"))
	if err != nil {
		return "", err
	}
	lowers := []string{path.Join(linkDir, string(parentLink))}

	parentLower, err := ioutil.ReadFile(path.Join(parentDir, lowerFile))
	if err == nil {
		parentLowers := strings.Split(string(parentLower), ":")
		lowers = append(lowers, parentLowers...)
	}
	if len(lowers) > maxDepth {
		return "", errors.New("max depth exceeded")
	}
	return strings.Join(lowers, ":"), nil
}

func (d *Driver) dir(id string) string {
	return path.Join(d.home, id)
}

func (d *Driver) getLowerDirs(id string) ([]string, error) {
	var lowersArray []string
	lowers, err := ioutil.ReadFile(path.Join(d.dir(id), lowerFile))
	if err == nil {
		for _, s := range strings.Split(string(lowers), ":") {
			lp, err := os.Readlink(path.Join(d.home, s))
			if err != nil {
				return nil, err
			}
			lowersArray = append(lowersArray, path.Clean(path.Join(d.home, lp)))
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
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

	diffDir := path.Join(dir, "diff")
	lowers, err := ioutil.ReadFile(path.Join(dir, lowerFile))
	if err != nil {
		// If no lower, just return diff directory
		if os.IsNotExist(err) {
			d.pathCacheLock.Lock()
			d.pathCache[id] = diffDir
			d.pathCacheLock.Unlock()
			return diffDir, nil
		}
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

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", string(lowers), path.Join(id, "diff"), path.Join(id, "work"))
	mountLabel = label.FormatMountLabel(opts, mountLabel)
	if len(mountLabel) > syscall.Getpagesize() {
		d.ctr.Decrement(id)
		return "", fmt.Errorf("cannot mount layer, mount label too large %d", len(mountLabel))
	}

	// TODO: Call mount with a reexec instead of using mount binary
	cmd := exec.Cmd{
		Path: "/usr/bin/mount",
		Args: []string{"mount", "-t", "overlay", "overlay", "-o", mountLabel, path.Join(id, "merged")},
		Dir:  d.home,
	}
	if err := cmd.Run(); err != nil {
		d.ctr.Decrement(id)
		return "", fmt.Errorf("error creating overlay mount to %s: %v", mergedDir, err)
	}

	//if err := syscall.Mount("overlay", mergedDir, "overlay", 0, mountLabel); err != nil {
	//	d.ctr.Decrement(id)
	//	return "", fmt.Errorf("error creating overlay mount to %s: %v", mergedDir, err)
	//}

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

// ApplyDiff applies the new layer into a root
func (d *Driver) ApplyDiff(id string, parent string, diff archive.Reader) (size int64, err error) {
	applyDir := d.getDiffPath(id)

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

func (d *Driver) getDiffPath(id string) string {
	dir := d.dir(id)

	return path.Join(dir, "diff")
}

// DiffSize calculates the changes between the specified id
// and its parent and returns the size in bytes of the changes
// relative to its base filesystem directory.
func (d *Driver) DiffSize(id, parent string) (size int64, err error) {
	return directory.Size(d.getDiffPath(id))
}

// Diff produces an archive of the changes between the specified
// layer and its parent layer which may be "".
func (d *Driver) Diff(id, parent string) (archive.Archive, error) {
	diffPath := d.getDiffPath(id)
	logrus.Debugf("Tar with options on %s", diffPath)
	return archive.TarWithOptions(diffPath, &archive.TarOptions{
		Compression:    archive.Uncompressed,
		UIDMaps:        d.uidMaps,
		GIDMaps:        d.gidMaps,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
	})
}

// Changes produces a list of changes between the specified layer
// and its parent layer. If parent is "", then all changes will be ADD changes.
func (d *Driver) Changes(id, parent string) ([]archive.Change, error) {
	// Overlay doesn't have snapshots, so we need to get changes from all parent
	// layers.
	diffPath := d.getDiffPath(id)
	layers, err := d.getLowerDirs(id)
	if err != nil {
		return nil, err
	}

	return archive.OverlayChanges(layers, diffPath)
}
