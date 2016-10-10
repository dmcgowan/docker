package continuitystore

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution"
	"github.com/docker/distribution/digest"
	"github.com/docker/docker/layer"
	"github.com/pkg/errors"
	"github.com/stevvooe/continuity"
	"github.com/vbatts/tar-split/tar/asm"
)

// TODO: implement this interface
//// Store represents a backend for managing both
//// read-only and read-write layers.
//type Store interface {
//	Release(Layer) ([]Metadata, error)
//
//	CreateRWLayer(id string, parent ChainID, mountLabel string, initFunc MountInit, storageOpt map[string]string) (RWLayer, error)
//	GetRWLayer(id string) (RWLayer, error)
//	GetMountID(id string) (string, error)
//	ReleaseRWLayer(RWLayer) ([]Metadata, error)
//
//	Cleanup() error
//	DriverStatus() [][2]string
//	DriverName() string
//}

const maxLayerDepth = 125

type layerStore struct {
	//store  MetadataStore

	blobs *BlobStore

	// TODO: replace with write capture driver
	writeDir string

	layerMap map[layer.ChainID]*roLayer
	layerL   sync.Mutex

	mounts map[string]*mountedLayer
	mountL sync.Mutex
}

func NewContinuityStore(bs *BlobStore, dir string) layer.Store {
	return &layerStore{
		blobs:    bs,
		writeDir: dir,
		layerMap: map[layer.ChainID]*roLayer{},
		mounts:   map[string]*mountedLayer{},
	}
}

func (ls *layerStore) getManifest(md digest.Digest) (*continuity.Manifest, error) {
	r, err := ls.blobs.Reader(md)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get manifest blob")
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read manifest bytes")
	}
	m, err := continuity.Unmarshal(b)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal manifest")
	}

	return m, nil
}

func (ls *layerStore) putManifest(m *continuity.Manifest) (digest.Digest, error) {
	b, err := continuity.Marshal(m)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal manifest")
	}
	d, err := ls.blobs.Digest(bytes.NewReader(b))
	if err != nil {
		return "", errors.Wrap(err, "failed to save manifest bytes")
	}
	return d, nil
}

func (ls *layerStore) applyTar(ts io.Reader, parent, rl *roLayer) error {
	digester := digest.Canonical.New()
	tr := io.TeeReader(ts, digester.Hash())

	tsErrC := make(chan error, 1)
	pr, pw := io.Pipe()
	go func() {
		dgst, err := ls.blobs.Digest(pr)
		if err != nil {
			pr.CloseWithError(err)
		} else {
			rl.tarSplitID = dgst
		}
		tsErrC <- errors.Wrap(err, "failed to get tar split digest")
	}()

	metaPacker := newManifestPacker(newJSONPacker(pw))

	filePutter := newBlobFilePutter(ls.blobs)

	if err := asm.DisassembleTarStream(tr, metaPacker, filePutter); err != nil {
		pw.CloseWithError(err)
		return err
	}

	pw.Close()
	if err := <-tsErrC; err != nil {
		return err
	}

	manifest, err := metaPacker.GetManifest()
	if err != nil {
		return errors.Wrap(err, "failed to get diff manifest")
	}

	// merge on top of parent manifest if has one
	if parent != nil {
		parentManifest, err := ls.getManifest(parent.manifestID)
		if err != nil {
			return errors.Wrap(err, "failed to get parent manifest")
		}

		manifest = continuity.MergeAUFS(parentManifest, manifest)
	}

	rl.manifestID, err = ls.putManifest(manifest)
	if err != nil {
		return errors.Wrap(err, "failed to save manifest")
	}

	// TODO: do apply size calculation
	var applySize int64

	rl.size = applySize
	rl.diffID = layer.DiffID(digester.Digest())

	logrus.Debugf("Applied tar %s to %s, size: %d", rl.diffID, rl.manifestID, applySize)

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

	if err = ls.applyTar(ts, p, rl); err != nil {
		return nil, err
	}

	if rl.parent == nil {
		rl.chainID = layer.ChainID(rl.diffID)
	} else {
		rl.chainID = createChainIDFromParent(rl.parent.chainID, rl.diffID)
	}

	//tx, err := ls.store.StartTransaction()
	//if err != nil {
	//	return nil, err
	//}

	defer func() {
		if err != nil {
			// TODO: make sure blob store does not have manifest id or tar split id held
			//if err := tx.Cancel(); err != nil {
			//	logrus.Errorf("Error canceling metadata transaction %q: %s", tx.String(), err)
			//}
		}
	}()

	//if err = storeLayer(tx, rl); err != nil {
	//	return nil, err
	//}

	ls.layerL.Lock()
	defer ls.layerL.Unlock()

	if existingLayer := ls.getWithoutLock(rl.chainID); existingLayer != nil {
		// Set error for cleanup, but do not return the error
		err = errors.New("layer already exists")
		return existingLayer.getReference(), nil
	}

	//if err = tx.Commit(rl.chainID); err != nil {
	//	return nil, err
	//}

	ls.layerMap[rl.chainID] = rl

	return rl.getReference(), nil
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
	//err = ls.store.Remove(rl.chainID)
	//if err != nil {
	//	return err
	//}
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

func (ls *layerStore) CreateRWLayer(name string, parent layer.ChainID, mountLabel string, initFunc layer.MountInit, storageOpt map[string]string) (layer.RWLayer, error) {
	ls.mountL.Lock()
	defer ls.mountL.Unlock()
	m, ok := ls.mounts[name]
	if ok {
		return nil, layer.ErrMountNameConflict
	}

	var err error
	var lower string
	var p *roLayer
	if string(parent) != "" {
		p = ls.get(parent)
		if p == nil {
			return nil, layer.ErrLayerDoesNotExist
		}

		lower = filepath.Join(ls.writeDir, "checkout", p.manifestID.Algorithm().String(), p.manifestID.Hex())
		//TODO(dmcgowan): only checkout if does not exist

		if err := os.MkdirAll(lower, 0755); err != nil {
			return nil, errors.Wrap(err, "failed to make checkout directory")
		}

		options := continuity.ContextOptions{
			Provider: ls.blobs,
		}
		context, err := continuity.NewContextWithOptions(lower, options)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get continuity context")
		}

		parentManifest, err := ls.getManifest(p.manifestID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get parent manifest")
		}

		if err := continuity.ApplyManifest(context, parentManifest); err != nil {
			return nil, errors.Wrap(err, "failed to checkout manifest")
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

	wc, err := newWriteCapturer(filepath.Join(ls.writeDir, "capture", name), lower)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create write capture layer")
	}

	m = &mountedLayer{
		name:       name,
		parent:     p,
		capture:    wc,
		references: map[layer.RWLayer]*referencedRWLayer{},
	}

	if initFunc != nil {
		if err := wc.init(initFunc, mountLabel, storageOpt); err != nil {
			return nil, errors.Wrap(err, "failed to initialize write capture layer")
		}
	}

	//if err = ls.saveMount(m); err != nil {
	//	return nil, err
	//}

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

	// TODO: Call cleanup on write capturer
	//if err := ls.driver.Remove(m.mountID); err != nil {
	//	logrus.Errorf("Error removing mounted layer %s: %s", m.name, err)
	//	m.retakeReference(l)
	//	return nil, err
	//}

	///if err := ls.store.RemoveMount(m.name); err != nil {
	///	logrus.Errorf("Error removing mount metadata: %s: %s", m.name, err)
	///	m.retakeReference(l)
	///	return nil, err
	///}

	delete(ls.mounts, m.Name())

	ls.layerL.Lock()
	defer ls.layerL.Unlock()
	if m.parent != nil {
		// TODO: checkin parent (may cause local cleanup)
		return ls.releaseLayer(m.parent)
	}

	return []layer.Metadata{}, nil
}

func (ls *layerStore) Cleanup() error {
	return nil
}

func (ls *layerStore) DriverStatus() [][2]string {
	return [][2]string{}
}

func (ls *layerStore) DriverName() string {
	return "continuity"
}
