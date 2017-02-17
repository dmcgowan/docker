package snapshot

// Open questions/issues
//  - Why no unmount all
//  - Is it possible to mix different backends
//  - Why call mount subcommand (do we want to rely on external binaries at all, re-exec?)
//  - (note) How will we handle mount from (cd + mount to allow fitting arguments in page size) (mount should, not snapshot driver)
//  - Will snapshotting enforce a layer depth limit like layer store does (125 in layer store)
//  - What about a naive driver with the equivalent of "--reflink=auto"
//  - Should names contain ':'
//  - What is timeline to vendor current containerd
//  - Is prepare key intended to be target mount path
//  - When should prepare be called
//  - (issue) Prepare should be able to be safely called multiple times
//  - snapshot removal missing from overlayfs implementation
//  - What is the use case for calling `Parent` in the manager?
//  - Changes interface could be awkward if mount required, different requirements
//  - Can changes be calculated while mounting
//  - Could we have a `DiffView` that produces 2 sets of mounts (potential optimization for overlayfs and aufs)
//     - Additionally allows getting changes from more than just the immediate parent (for squash use cases)
//  - How will opaque directories be supported (opaque directories are not necessary for read only snapshots)
//  - How will whiteouts be translated (they will not, whiteouts always applied onto snapshots)
//    - if tar is done at higher layer, how will it know how to interpret whiteouts
//    - some drivers apply tars onto existing directories, applying whiteout actions immediately

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/dmcgowan/containerd"
	"github.com/dmcgowan/containerd/archive"
	"github.com/dmcgowan/containerd/snapshot"
	"github.com/dmcgowan/containerd/snapshot/overlay"
	"github.com/docker/distribution"
	"github.com/docker/docker/layer"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"github.com/vbatts/tar-split/tar/asm"
	"github.com/vbatts/tar-split/tar/storage"
)

const maxLayerDepth = 125

type layerStore struct {
	snapshotter snapshot.Snapshotter
	store       *fileMetadataStore
	mountDir    string

	layerMap map[layer.ChainID]*roLayer
	layerL   sync.Mutex

	mounts map[string]*mountedLayer
	mountL sync.Mutex
}

func NewSnapshotStore(root string) (layer.Store, error) {
	managerDir := filepath.Join(root, "snapshot-overlay")
	if err := os.MkdirAll(managerDir, 0750); err != nil {
		return nil, err
	}
	manager, err := overlay.NewSnapshotter(managerDir)
	if err != nil {
		return nil, err
	}

	mountDir := filepath.Join(root, "mount")
	if err := os.MkdirAll(mountDir, 0750); err != nil {
		return nil, err
	}
	metaDir := filepath.Join(root, "metadata")
	if err := os.MkdirAll(filepath.Join(metaDir, "mounts"), 0750); err != nil {
		return nil, err
	}

	ls := &layerStore{
		snapshotter: manager,
		store: &fileMetadataStore{
			root: metaDir,
		},
		mountDir: mountDir,
		layerMap: map[layer.ChainID]*roLayer{},
		mounts:   map[string]*mountedLayer{},
	}

	if err := ls.load(); err != nil {
		return nil, errors.Wrap(err, "failed to load layer store")
	}

	return ls, nil
}

func (ls *layerStore) load() error {
	ids, mounts, err := ls.store.List()
	if err != nil {
		return err
	}

	for _, id := range ids {
		l, err := ls.loadLayer(id)
		if err != nil {
			logrus.Debugf("Failed to load layer %s: %s", id, err)
			continue
		}
		if l.parent != nil {
			l.parent.referenceCount++
		}
	}

	for _, mount := range mounts {
		if err := ls.loadMount(mount); err != nil {
			logrus.Debugf("Failed to load mount %s: %s", mount, err)
		}
	}

	return nil
}

func (ls *layerStore) loadLayer(l layer.ChainID) (*roLayer, error) {
	cl, ok := ls.layerMap[l]
	if ok {
		return cl, nil
	}

	diff, err := ls.store.GetDiffID(l)
	if err != nil {
		return nil, fmt.Errorf("failed to get diff id for %s: %s", l, err)
	}

	size, err := ls.store.GetSize(l)
	if err != nil {
		return nil, fmt.Errorf("failed to get size for %s: %s", l, err)
	}

	parent, err := ls.store.GetParent(l)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent for %s: %s", l, err)
	}

	descriptor, err := ls.store.GetDescriptor(l)
	if err != nil {
		return nil, fmt.Errorf("failed to get descriptor for %s: %s", l, err)
	}

	cl = &roLayer{
		chainID:    l,
		diffID:     diff,
		size:       size,
		layerStore: ls,
		references: map[layer.Layer]struct{}{},
		descriptor: descriptor,
	}

	if parent != "" {
		p, err := ls.loadLayer(parent)
		if err != nil {
			return nil, err
		}
		cl.parent = p
	}

	ls.layerMap[cl.chainID] = cl

	return cl, nil
}

func (ls *layerStore) loadMount(mount string) error {
	if _, ok := ls.mounts[mount]; ok {
		return nil
	}

	parent, err := ls.store.GetMountParent(mount)
	if err != nil {
		return err
	}

	ml := &mountedLayer{
		name:       mount,
		layerStore: ls,
		references: map[layer.RWLayer]*referencedRWLayer{},
	}

	if parent != "" {
		p, err := ls.loadLayer(parent)
		if err != nil {
			return err
		}
		ml.parent = p

		p.referenceCount++
	}

	ml.mounts, err = ls.snapshotter.Mounts(ml.mountPath())
	if err != nil {
		return err
	}

	ls.mounts[ml.name] = ml

	return nil
}

func (ls *layerStore) applyTar(tx *fileMetadataTransaction, ts io.Reader, parent, rl *roLayer) error {
	digester := digest.Canonical.Digester()
	tr := io.TeeReader(ts, digester.Hash())

	tsw, err := tx.TarSplitWriter(true)
	if err != nil {
		return errors.Wrap(err, "failed to create tar split writer")
	}
	metaPacker := storage.NewJSONPacker(tsw)
	defer tsw.Close()

	// we're passing nil here for the file putter, because we will
	// directory untar the contents into mountDir
	rdr, err := asm.NewInputTarStream(tr, metaPacker, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create new tar input stream")
	}

	mountDir, err := ioutil.TempDir(ls.mountDir, "apply-tar-")
	if err != nil {
		return errors.Wrap(err, "failed to create temp tar directory for mount")
	}

	// TODO: capture error
	defer os.RemoveAll(mountDir)

	mounts, err := ls.snapshotter.Prepare(mountDir, parent.snapshotName())
	if err != nil {
		return errors.Wrap(err, "failed to prepare snapshot")
	}

	logrus.Debugf("Mounting: %#v", mounts)

	if err := containerd.MountAll(mounts, mountDir); err != nil {
		return errors.Wrapf(err, "failed to mount %s", mountDir)
	}

	// Use uid and gid maps
	start := time.Now().UTC()
	logrus.Debug("Start untar layer")
	size, err := archive.Apply(context.Background(), mountDir, rdr)
	if err != nil {
		syscall.Unmount(mountDir, 0)
		// TODO: log unmount error
		return errors.Wrap(err, "apply tar failed")
	}
	logrus.Debugf("Untar time: %vs", time.Now().UTC().Sub(start).Seconds())

	// Discard trailing data but ensure metadata is picked up to reconstruct stream
	io.Copy(ioutil.Discard, rdr) // ignore error as reader may be closed

	if err := syscall.Unmount(mountDir, 0); err != nil {
		return errors.Wrap(err, "failed to unmount apply tar mount")
	}

	rl.size = size
	rl.diffID = layer.DiffID(digester.Digest())
	rl.chainID = createChainIDFromParent(parent.ChainID(), rl.diffID)

	// TODO: Get directory
	if err := ls.snapshotter.Commit(rl.snapshotName(), mountDir); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}

	logrus.Debugf("Applied tar %s to %s, size: %d", rl.diffID, rl.snapshotName(), size)

	return nil
}

func (ls *layerStore) Register(ts io.Reader, parent layer.ChainID) (layer.Layer, error) {
	return ls.registerWithDescriptor(ts, parent, distribution.Descriptor{})
}

func (ls *layerStore) registerWithDescriptor(ts io.Reader, parent layer.ChainID, descriptor distribution.Descriptor) (layer.Layer, error) {
	// err is used to hold the error which will always trigger
	// cleanup of creates sources but may not be an error returned
	// to the caller (already exists).
	var err error
	var p *roLayer
	if string(parent) != "" {
		p = ls.get(parent)
		if p == nil {
			return nil, layer.ErrLayerDoesNotExist
		}

		// Release parent chain if error
		defer func() {
			if err != nil {
				ls.layerL.Lock()
				ls.releaseLayer(p)
				ls.layerL.Unlock()
			}
		}()
		if p.depth() >= maxLayerDepth {
			err = layer.ErrMaxDepthExceeded
			return nil, err
		}
	}

	// Create new roLayer
	rl := &roLayer{
		parent:         p,
		referenceCount: 1,
		layerStore:     ls,
		references:     map[layer.Layer]struct{}{},
		descriptor:     descriptor,
	}

	tx, err := ls.store.StartTransaction()
	if err != nil {
		return nil, errors.Wrap(err, "failed to start transaction")
	}

	if err = ls.applyTar(tx, ts, p, rl); err != nil {
		return nil, errors.Wrap(err, "apply tar failed")
	}

	if rl.parent == nil {
		rl.chainID = layer.ChainID(rl.diffID)
	} else {
		rl.chainID = createChainIDFromParent(rl.parent.chainID, rl.diffID)
	}

	defer func() {
		if err != nil {
			// TODO: make sure blob store does not have manifest id or tar split id held
			if err := tx.Cancel(); err != nil {
				logrus.Errorf("Error canceling metadata transaction %q: %s", tx.String(), err)
			}
		}
	}()

	if err = storeLayer(tx, rl); err != nil {
		return nil, errors.Wrap(err, "failed to store layer metadata")
	}

	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	if existingLayer := ls.getWithoutLock(rl.chainID); existingLayer != nil {
		// Set error for cleanup, but do not return the error
		err = errors.New("layer already exists")
		return existingLayer.getReference(), nil
	}

	if err = tx.Commit(rl.chainID); err != nil {
		return nil, errors.Wrap(err, "failed to commit layer metadata")
	}

	ls.layerMap[rl.chainID] = rl

	return rl.getReference(), nil
}

func (ls *layerStore) assembleTarTo(chainID layer.ChainID, metadata io.ReadCloser, w io.Writer) error {
	md, err := ioutil.TempDir(ls.mountDir, "assemble-tar-")
	if err != nil {
		return errors.Wrap(err, "failed to create assemble tar mount dir")
	}
	mounts, err := ls.snapshotter.Prepare(md, chainID.String())
	if err != nil {
		return errors.Wrapf(err, "failed to prepare snapshot for %v", chainID.String())
	}
	defer ls.snapshotter.Remove(md)

	if err := containerd.MountAll(mounts, md); err != nil {
		return errors.Wrap(err, "failed to mount")
	}
	// TODO: Need to do unmount all....
	defer syscall.Unmount(md, 0)

	metaUnpacker := storage.NewJSONUnpacker(metadata)
	defer metadata.Close()

	logrus.Debugf("Assembling tar data for %v", chainID)
	if err := asm.WriteOutputTarStream(storage.NewPathFileGetter(md), metaUnpacker, w); err != nil {
		return errors.Wrap(err, "failed to assemble tar data")
	}

	return nil
}

func (ls *layerStore) getWithoutLock(layerID layer.ChainID) *roLayer {
	l, ok := ls.layerMap[layerID]
	if !ok {
		return nil
	}

	l.referenceCount++

	return l
}

func (ls *layerStore) get(layerID layer.ChainID) *roLayer {
	ls.layerL.Lock()
	defer ls.layerL.Unlock()
	return ls.getWithoutLock(layerID)
}

func (ls *layerStore) Get(layerID layer.ChainID) (layer.Layer, error) {
	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	l := ls.getWithoutLock(layerID)
	if l == nil {
		return nil, layer.ErrLayerDoesNotExist
	}

	return l.getReference(), nil
}

func (ls *layerStore) Map() map[layer.ChainID]layer.Layer {
	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	layers := map[layer.ChainID]layer.Layer{}

	for k, v := range ls.layerMap {
		layers[k] = v
	}

	return layers
}

func createChainIDFromParent(parent layer.ChainID, dgsts ...layer.DiffID) layer.ChainID {
	if len(dgsts) == 0 {
		return parent
	}
	if parent == "" {
		return createChainIDFromParent(layer.ChainID(dgsts[0]), dgsts[1:]...)
	}
	// H = "H(n-1) SHA256(n)"
	dgst := digest.FromBytes([]byte(string(parent) + " " + string(dgsts[0])))
	return createChainIDFromParent(layer.ChainID(dgst), dgsts[1:]...)
}

func (ls *layerStore) deleteLayer(rl *roLayer, metadata *layer.Metadata) error {
	var err error
	err = ls.snapshotter.Remove(rl.snapshotName())
	if err != nil {
		return errors.Wrap(err, "failed to delete snapshot")
	}
	metadata.DiffID = rl.diffID
	metadata.ChainID = rl.chainID
	metadata.Size, err = rl.Size()
	if err != nil {
		return errors.Wrap(err, "failed to get size")
	}
	metadata.DiffSize = rl.size

	return nil
}

func (ls *layerStore) releaseLayer(rl *roLayer) ([]layer.Metadata, error) {
	depth := 0
	removed := []layer.Metadata{}
	for {
		if rl.referenceCount == 0 {
			panic("layer not retained")
		}
		rl.referenceCount--
		if rl.referenceCount != 0 {
			return removed, nil
		}

		if len(removed) == 0 && depth > 0 {
			panic("cannot remove layer with child")
		}
		if rl.hasReferences() {
			panic("cannot delete referenced layer")
		}
		var metadata layer.Metadata
		if err := ls.deleteLayer(rl, &metadata); err != nil {
			return nil, err
		}

		delete(ls.layerMap, rl.chainID)
		removed = append(removed, metadata)

		if rl.parent == nil {
			return removed, nil
		}

		depth++
		rl = rl.parent
	}
}

func (ls *layerStore) Release(l layer.Layer) ([]layer.Metadata, error) {
	ls.layerL.Lock()
	defer ls.layerL.Unlock()
	rl, ok := ls.layerMap[l.ChainID()]
	if !ok {
		return []layer.Metadata{}, nil
	}
	if !rl.hasReference(l) {
		return nil, layer.ErrLayerNotRetained
	}

	rl.deleteReference(l)

	return ls.releaseLayer(rl)
}

func (ls *layerStore) CreateRWLayer(name string, parent layer.ChainID, opts *layer.CreateRWLayerOpts) (layer.RWLayer, error) {
	var (
		storageOpt map[string]string
		initFunc   layer.MountInit
		mountLabel string
	)

	if opts != nil {
		mountLabel = opts.MountLabel
		storageOpt = opts.StorageOpt
		initFunc = opts.InitFunc
	}

	ls.mountL.Lock()
	defer ls.mountL.Unlock()
	m, ok := ls.mounts[name]
	if ok {
		return nil, layer.ErrMountNameConflict
	}

	var err error
	var p *roLayer
	if string(parent) != "" {
		p = ls.get(parent)
		if p == nil {
			return nil, layer.ErrLayerDoesNotExist
		}

		// Release parent chain if error
		defer func() {
			if err != nil {
				ls.layerL.Lock()
				ls.releaseLayer(p)
				ls.layerL.Unlock()
			}
		}()
	}

	m = &mountedLayer{
		name:       name,
		parent:     p,
		layerStore: ls,
		references: map[layer.RWLayer]*referencedRWLayer{},
	}

	var parentName string
	if initFunc != nil {
		initName, err := ls.initMount(m.name, p, mountLabel, initFunc, storageOpt)
		if err != nil {
			return nil, err
		}
		m.initName = initName
		if err := ls.store.SetInitName(m.name, m.initName); err != nil {
			return nil, err
		}
		parentName = initName
	} else {
		parentName = p.snapshotName()
	}

	if m.parent != nil {
		if err := ls.store.SetMountParent(m.name, m.parent.chainID); err != nil {
			return nil, err
		}
	}

	m.mounts, err = ls.snapshotter.Prepare(m.mountPath(), parentName)
	if err != nil {
		return nil, err
	}

	ls.mounts[m.name] = m

	logrus.Debugf("Creating RW Layer with name %s", m.name)

	return m.getReference(), nil
}

func (ls *layerStore) GetRWLayer(id string) (layer.RWLayer, error) {
	ls.mountL.Lock()
	defer ls.mountL.Unlock()
	mount, ok := ls.mounts[id]
	if !ok {
		return nil, layer.ErrMountDoesNotExist
	}

	return mount.getReference(), nil
}

func (ls *layerStore) GetMountID(id string) (string, error) {
	ls.mountL.Lock()
	defer ls.mountL.Unlock()
	mount, ok := ls.mounts[id]
	if !ok {
		return "", layer.ErrMountDoesNotExist
	}
	logrus.Debugf("GetMountID id: %s -> mountID: %s", id, mount.name)

	return mount.name, nil
}

func (ls *layerStore) ReleaseRWLayer(l layer.RWLayer) ([]layer.Metadata, error) {
	ls.mountL.Lock()
	defer ls.mountL.Unlock()
	m, ok := ls.mounts[l.Name()]
	if !ok {
		return []layer.Metadata{}, nil
	}

	if err := m.deleteReference(l); err != nil {
		return nil, err
	}

	if m.hasReferences() {
		return []layer.Metadata{}, nil
	}

	if err := ls.snapshotter.Remove(m.mountPath()); err != nil {
		logrus.Errorf("Error removing mounted layer %s: %s", m.name, err)
		m.retakeReference(l)
		return nil, err
	}

	if err := ls.store.RemoveMount(m.name); err != nil {
		logrus.Errorf("Error removing mount metadata: %s: %s", m.name, err)
		m.retakeReference(l)
		return nil, err
	}

	delete(ls.mounts, m.Name())

	ls.layerL.Lock()
	defer ls.layerL.Unlock()
	if m.parent != nil {
		// TODO: checkin parent (may cause local cleanup)
		return ls.releaseLayer(m.parent)
	}

	return []layer.Metadata{}, nil
}

func (ls *layerStore) initMount(name string, parent *roLayer, mountLabel string, initFunc layer.MountInit, storageOpt map[string]string) (string, error) {
	mountDir, err := ioutil.TempDir(ls.mountDir, "init-")
	if err != nil {
		return "", err
	}

	// TODO: capture error
	defer os.RemoveAll(mountDir)

	mounts, err := ls.snapshotter.Prepare(mountDir, parent.snapshotName())
	if err != nil {
		return "", err
	}

	// TODO: use mount label
	if err := containerd.MountAll(mounts, mountDir); err != nil {
		return "", err
	}

	if err := initFunc(mountDir); err != nil {
		// TODO: log error
		syscall.Unmount(mountDir, 0)
		return "", err
	}

	if err := syscall.Unmount(mountDir, 0); err != nil {
		return "", err
	}

	initName := fmt.Sprintf("init-%s", name)

	if err := ls.snapshotter.Commit(initName, mountDir); err != nil {
		return "", errors.Wrap(err, "failed to commit init snapshot")
	}

	return initName, nil
}

func (ls *layerStore) Cleanup() error {
	return nil
}

func (ls *layerStore) DriverStatus() [][2]string {
	return [][2]string{}
}

func (ls *layerStore) DriverName() string {
	return "snapshot-overlay"
}
