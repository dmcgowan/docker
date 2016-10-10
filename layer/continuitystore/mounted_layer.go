package continuitystore

import (
	"io"

	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/archive"
)

type mountedLayer struct {
	name    string
	parent  *roLayer
	path    string
	capture *writeCapturer

	references map[layer.RWLayer]*referencedRWLayer
}

func (ml *mountedLayer) TarStream() (io.ReadCloser, error) {
	return ml.capture.diff()
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
	return ml.capture.size()
}

func (ml *mountedLayer) Changes() ([]archive.Change, error) {
	return ml.capture.changes()
}

func (ml *mountedLayer) Metadata() (map[string]string, error) {
	return map[string]string{
		"lower": ml.capture.lower,
		"root":  ml.capture.root,
	}, nil
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
	return rl.capture.mount(mountLabel)
}

// Unmount decrements the activity count and unmounts the underlying layer
// Callers should only call `Unmount` once per call to `Mount`, even on error.
func (rl *referencedRWLayer) Unmount() error {
	return rl.capture.unmount()
}
