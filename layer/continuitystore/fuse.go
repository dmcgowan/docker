package continuitystore

import (
	"errors"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Sirupsen/logrus"
	"github.com/stevvooe/continuity"
	"github.com/stevvooe/continuity/continuityfs"
)

type fuseMounter struct {
	root string
	fs   fs.FS
	conn *fuse.Conn

	errL sync.Mutex
	err  error
}

func (fm *fuseMounter) Mount() error {
	c, err := fuse.Mount(
		fm.root,
		fuse.ReadOnly(),
		// Remove this once mounts properly managed
		fuse.AllowNonEmptyMount(),
		fuse.FSName("manifest"),
		fuse.Subtype("continuity"),
		// OSX Only options
		fuse.LocalVolume(),
		fuse.VolumeName("Docker Continuity Layer Store"),
	)
	if err != nil {
		return err
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		c.Close()
		return err
	}

	go func() {
		// TODO: Create server directory to use context
		err = fs.Serve(c, fm.fs)
		if err != nil {
			logrus.Errorf("Server error: %v", err)
			fm.errL.Lock()
			fm.err = err
			fm.errL.Unlock()
		}
	}()
	fm.conn = c

	return nil
}

func (fm *fuseMounter) Unmount() error {
	if fm.conn == nil {
		return nil
	}
	c := fm.conn
	fm.conn = nil

	closeC := make(chan error)
	go func() {
		if err := c.Close(); err != nil {
			closeC <- err
		}
		close(closeC)
	}()

	var closeErr error
	timeoutC := time.After(time.Second)

	select {
	case <-timeoutC:
		closeErr = errors.New("close timed out")
	case closeErr = <-closeC:
	}

	if closeErr != nil {
		logrus.Errorf("Unable to close connection: %v", closeErr)
	}

	if err := fuse.Unmount(fm.root); err != nil {
		logrus.Errorf("Error unmounting %s: %v", fm.root, err)
		return err
	}

	fm.errL.Lock()
	defer fm.errL.Unlock()
	return fm.err
}

func newFuseMounter(root string, manifest *continuity.Manifest, provider continuityfs.FileContentProvider) (*fuseMounter, error) {
	manifestFS, err := continuityfs.NewFSFromManifest(manifest, root, provider)
	if err != nil {
		return nil, err
	}
	return &fuseMounter{
		root: root,
		fs:   manifestFS,
	}, nil
}
