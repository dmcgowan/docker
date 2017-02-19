package snapshot

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/dmcgowan/containerd"
	"github.com/dmcgowan/containerd/archive"
	"github.com/docker/docker/layer"
	dockerarchive "github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/pkg/errors"
)

type mountedLayer struct {
	name       string
	parent     *roLayer
	initName   string
	mountCount int
	mountL     sync.Mutex
	mounts     []containerd.Mount
	layerStore *layerStore

	references map[layer.RWLayer]*referencedRWLayer
}

func (ml *mountedLayer) TarStream() (r io.ReadCloser, err error) {
	// Create parent temp
	td, err := ioutil.TempDir("", "mount-differ")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create temp dir")
	}
	defer func() {
		if err != nil {
			os.RemoveAll(td)
		}
	}()

	var (
		diffKey      = filepath.Join(td, "diff")
		parentKey    string
		parentMounts []containerd.Mount
	)

	if err := os.Mkdir(diffKey, 0700); err != nil {
		return nil, errors.Wrap(err, "failed to make diff directory")
	}

	parent := ml.initName
	if parent == "" && ml.parent != nil {
		parent = ml.parent.chainID.String()
	}
	if parent != "" {
		parentKey = filepath.Join(td, "parent")
		if err := os.Mkdir(parentKey, 0700); err != nil {
			return nil, errors.Wrap(err, "failed to make parent directory")
		}
		parentMounts, err = ml.layerStore.snapshotter.View(parentKey, ml.initName)
		if err != nil {
			return nil, errors.Wrap(err, "failed to prepare parent directory")
		}
	}

	if err := containerd.MountAll(ml.mounts, diffKey); err != nil {
		return nil, errors.Wrap(err, "failed to mount diff")
	}
	if parentKey != "" {
		if err := containerd.MountAll(parentMounts, parentKey); err != nil {
			syscall.Unmount(diffKey, 0)
			return nil, errors.Wrap(err, "failed to mount parent")
		}
	}

	ar := archive.Diff(context.Background(), parentKey, diffKey)

	return ioutils.NewReadCloserWrapper(ar, func() error {
		// TODO: log errors
		if parentKey != "" {
			syscall.Unmount(parentKey, 0)
			ml.layerStore.snapshotter.Remove(parentKey)
		}
		syscall.Unmount(diffKey, 0)

		os.RemoveAll(td)
		return ar.Close()
	}), nil

}

func (ml *mountedLayer) Name() string {
	return ml.name
}

func (ml *mountedLayer) mountPath() string {
	return filepath.Join(ml.layerStore.mountDir, fmt.Sprintf("rw-%s", ml.name))
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
	return 0, errors.New("not supported")
}

func (ml *mountedLayer) Changes() ([]dockerarchive.Change, error) {
	return nil, errors.New("not supported")
}

func (ml *mountedLayer) Metadata() (map[string]string, error) {
	// TODO: fill in relevant metadata
	return map[string]string{}, nil
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
	rl.mountL.Lock()
	defer rl.mountL.Unlock()
	if rl.mountCount > 0 {
		rl.mountCount++
		return rl.mountPath(), nil
	}

	mountPath := rl.mountPath()

	// Get mounts
	if err := os.MkdirAll(mountPath, 0750); err != nil {
		return "", errors.Wrap(err, "failed to make mount directory")
	}

	// TODO: apply mount label to mounts
	if err := containerd.MountAll(rl.mounts, mountPath); err != nil {
		return "", errors.Wrap(err, "failed to mount")
	}

	rl.mountCount++
	return mountPath, nil
}

// Unmount decrements the activity count and unmounts the underlying layer
// Callers should only call `Unmount` once per call to `Mount`, even on error.
func (rl *referencedRWLayer) Unmount() error {
	rl.mountL.Lock()
	defer rl.mountL.Unlock()
	if rl.mountCount == 0 {
		return errors.New("not mounted")
	}
	rl.mountCount--
	if rl.mountCount > 0 {
		return nil
	}
	return syscall.Unmount(rl.mountPath(), 0)
}
