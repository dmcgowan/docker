package continuitystore

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/directory"
	"github.com/opencontainers/runc/libcontainer/label"
	"github.com/pkg/errors"
)

type writeCapturer struct {
	root  string
	lower string
}

func newWriteCapturer(root, lower string) (*writeCapturer, error) {
	if err := os.MkdirAll(filepath.Join(root, "init"), 0755); err != nil {
		return nil, errors.Wrap(err, "unable to create init directory")
	}
	if err := os.Mkdir(filepath.Join(root, "upper"), 0755); err != nil {
		return nil, errors.Wrap(err, "unable to create upper directory")
	}
	if err := os.Mkdir(filepath.Join(root, "work"), 0755); err != nil {
		return nil, errors.Wrap(err, "unable to create work directory")
	}
	if err := os.Mkdir(filepath.Join(root, "merged"), 0755); err != nil {
		return nil, errors.Wrap(err, "unable to create merged directory")
	}
	return &writeCapturer{
		root:  root,
		lower: lower,
	}, nil
}

func (wc *writeCapturer) init(initFunc layer.MountInit, mountLabel string, storageOpt map[string]string) (err error) {
	initDir := filepath.Join(wc.root, "init")
	if wc.lower != "" {
		opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", wc.lower, initDir, path.Join(wc.root, "work"))
		mountData := label.FormatMountLabel(opts, mountLabel)
		initDir = path.Join(wc.root, "merged")

		if err := syscall.Mount("overlay", initDir, "overlay", 0, mountData); err != nil {
			return errors.Wrap(err, "failed to mount init layer")
		}

		defer func() {
			uerr := syscall.Unmount(initDir, 0)
			if err == nil {
				err = uerr
			}
		}()
	}

	if err := initFunc(initDir); err != nil {
		return errors.Wrap(err, "failure calling initialize on write capture")
	}
	return nil
}

func (wc *writeCapturer) mount(mountLabel string) (string, error) {
	lower := filepath.Join(wc.root, "init")
	if wc.lower != "" {
		lower = wc.lower + ":" + lower
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lower, path.Join(wc.root, "upper"), path.Join(wc.root, "work"))
	mountData := label.FormatMountLabel(opts, mountLabel)
	mergedDir := path.Join(wc.root, "merged")

	if err := syscall.Mount("overlay", mergedDir, "overlay", 0, mountData); err != nil {
		return "", errors.Wrap(err, "failed to mount layer")
	}

	return mergedDir, nil
}

func (wc *writeCapturer) unmount() error {
	return syscall.Unmount(filepath.Join(wc.root, "merged"), 0)
}

func (wc *writeCapturer) diff() (io.ReadCloser, error) {
	diffPath := filepath.Join(wc.root, "upper")
	logrus.Debugf("Tar with options on %s", diffPath)
	return archive.TarWithOptions(diffPath, &archive.TarOptions{
		Compression:    archive.Uncompressed,
		WhiteoutFormat: archive.OverlayWhiteoutFormat,
	})
}

func (wc *writeCapturer) size() (int64, error) {
	return directory.Size(filepath.Join(wc.root, "upper"))

}

func (wc *writeCapturer) changes() ([]archive.Change, error) {
	layers := []string{}
	if wc.lower != "" {
		layers = append(layers, wc.lower)
	}
	return archive.OverlayChanges(layers, filepath.Join(wc.root, "upper"))
}
