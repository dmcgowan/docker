package continuitystore

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/docker/distribution/digest"
)

type BlobStore struct {
	blobDir   string
	tmpDir    string
	algorithm digest.Algorithm
}

func NewBlobStore(root string) (*BlobStore, error) {
	blobDir := filepath.Join(root, "sha256")
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		return nil, err
	}
	tmpDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, err
	}
	return &BlobStore{
		blobDir:   blobDir,
		tmpDir:    tmpDir,
		algorithm: digest.SHA256,
	}, nil
}

func (bs *BlobStore) Digest(r io.Reader) (d digest.Digest, err error) {
	tf, err := ioutil.TempFile(bs.tmpDir, "blob-")
	if err != nil {
		return "", err
	}
	defer func() {
		blob := tf.Name()
		if err == nil {
			if _, statErr := os.Stat(blob); err != nil {
				err = statErr
				return
			}
			target := filepath.Join(bs.blobDir, d.Hex())
			err = os.Rename(blob, target)
		} else {
			os.Remove(blob)
		}
	}()
	defer func() {
		closeErr := tf.Close()
		if err == nil {
			err = closeErr
		}
	}()

	dgstr := bs.algorithm.New()
	w := io.MultiWriter(tf, dgstr.Hash())

	if _, err := io.Copy(w, r); err != nil {
		return "", err
	}
	return dgstr.Digest(), nil
}

func (bs *BlobStore) Reader(d digest.Digest) (io.ReadCloser, error) {
	return bs.Open("", d)
}

func (bs *BlobStore) Path(path string, d digest.Digest) (string, error) {
	if d.Algorithm() != digest.SHA256 {
		return "", digest.ErrDigestUnsupported
	}
	return filepath.Join(bs.blobDir, d.Hex()), nil
}

func (bs *BlobStore) Open(path string, d digest.Digest) (io.ReadCloser, error) {
	if d.Algorithm() != digest.SHA256 {
		return nil, digest.ErrDigestUnsupported
	}
	return os.Open(filepath.Join(bs.blobDir, d.Hex()))
}

type closeWrapper struct {
	r io.ReadCloser
}

func (cw closeWrapper) Read(p []byte) (n int, err error) {
	n, err = cw.r.Read(p)
	if err == io.EOF {
		cw.r.Close()
	}
	return
}
