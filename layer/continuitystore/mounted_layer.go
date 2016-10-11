package continuitystore

import (
	"io"

	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/archive"
	"github.com/pkg/errors"
)

//TODO(dmcgowan): Needs separate driver interface to implement write capture

type mountedLayer struct {
	name       string
	mountID    string
	initID     string
	parent     *roLayer
	path       string
	layerStore *layerStore

	references map[layer.RWLayer]*referencedRWLayer
}

func (ml *mountedLayer) TarStream() (io.ReadCloser, error) {
	return nil, errors.New("not implemented")

	//archiver, err := ml.layerStore.driver.Diff(ml.mountID, ml.cacheParent())
	//if err != nil {
	//	return nil, err
	//}
	//return archiver, nil
}

func (ml *mountedLayer) Name() string {
	return ml.name
}

func (ml *mountedLayer) Parent() layer.Layer {
	if ml.parent != nil {
		return ml.parent
	}

	// Return a nil interface instead of an interface wrapping a nil
	// pointer.
	return nil
}

func (ml *mountedLayer) Size() (int64, error) {
	return 0, errors.New("not implemented")
	//return ml.layerStore.driver.DiffSize(ml.mountID, ml.cacheParent())
}

func (ml *mountedLayer) Changes() ([]archive.Change, error) {
	return nil, errors.New("not implemented")
	//return ml.layerStore.driver.Changes(ml.mountID, ml.cacheParent())
}

func (ml *mountedLayer) Metadata() (map[string]string, error) {
	return nil, errors.New("not implemented")
	//return ml.layerStore.driver.GetMetadata(ml.mountID)
}

func (ml *mountedLayer) getReference() layer.RWLayer {
	ref := &referencedRWLayer{
		mountedLayer: ml,
	}
	ml.references[ref] = ref

	return ref
}

func (ml *mountedLayer) hasReferences() bool {
	return len(ml.references) > 0
}

func (ml *mountedLayer) deleteReference(ref layer.RWLayer) error {
	if _, ok := ml.references[ref]; !ok {
		return layer.ErrLayerNotRetained
	}
	delete(ml.references, ref)
	return nil
}

func (ml *mountedLayer) retakeReference(r layer.RWLayer) {
	if ref, ok := r.(*referencedRWLayer); ok {
		ml.references[ref] = ref
	}
}

type referencedRWLayer struct {
	*mountedLayer
}

func (rl *referencedRWLayer) Mount(mountLabel string) (string, error) {
	return "", errors.New("not implemented")
	//return rl.layerStore.driver.Get(rl.mountedLayer.mountID, mountLabel)
}

// Unmount decrements the activity count and unmounts the underlying layer
// Callers should only call `Unmount` once per call to `Mount`, even on error.
func (rl *referencedRWLayer) Unmount() error {
	return errors.New("not implemented")
	//return rl.layerStore.driver.Put(rl.mountedLayer.mountID)
}
