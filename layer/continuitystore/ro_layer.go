package continuitystore

import (
	"fmt"
	"io"

	"github.com/docker/distribution"
	"github.com/docker/distribution/digest"
	"github.com/docker/docker/layer"
	"github.com/pkg/errors"
	"github.com/vbatts/tar-split/tar/asm"
)

type roLayer struct {
	chainID layer.ChainID
	diffID  layer.DiffID
	parent  *roLayer
	size    int64

	tarSplitID digest.Digest
	manifestID digest.Digest

	layerStore *layerStore
	descriptor distribution.Descriptor

	referenceCount int
	references     map[layer.Layer]struct{}
}

func (rl *roLayer) TarStream() (io.ReadCloser, error) {
	tsr, err := rl.layerStore.blobs.Reader(rl.tarSplitID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get content for tar split")
	}

	pr, pw := io.Pipe()
	go func() {
		fileGetter := newBlobFileGetter(rl.layerStore.blobs)
		unpacker := newJSONUnpacker(tsr)
		err := asm.WriteOutputTarStream(fileGetter, unpacker, pw)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
	}()
	rc, err := newVerifiedReadCloser(pr, digest.Digest(rl.diffID))
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func (rl *roLayer) ChainID() layer.ChainID {
	return rl.chainID
}

func (rl *roLayer) DiffID() layer.DiffID {
	return rl.diffID
}

func (rl *roLayer) Parent() layer.Layer {
	if rl.parent == nil {
		return nil
	}
	return rl.parent
}

func (rl *roLayer) Size() (size int64, err error) {
	if rl.parent != nil {
		size, err = rl.parent.Size()
		if err != nil {
			return
		}
	}

	return size + rl.size, nil
}

func (rl *roLayer) DiffSize() (size int64, err error) {
	return rl.size, nil
}

func (rl *roLayer) Metadata() (map[string]string, error) {
	return map[string]string{
		"manifest":  rl.manifestID.String(),
		"tar-split": rl.tarSplitID.String(),
	}, nil
}

type referencedCacheLayer struct {
	*roLayer
}

func (rl *roLayer) getReference() layer.Layer {
	ref := &referencedCacheLayer{
		roLayer: rl,
	}
	rl.references[ref] = struct{}{}

	return ref
}

func (rl *roLayer) hasReference(ref layer.Layer) bool {
	_, ok := rl.references[ref]
	return ok
}

func (rl *roLayer) hasReferences() bool {
	return len(rl.references) > 0
}

func (rl *roLayer) deleteReference(ref layer.Layer) {
	delete(rl.references, ref)
}

func (rl *roLayer) depth() int {
	if rl.parent == nil {
		return 1
	}
	return rl.parent.depth() + 1
}

//func storeLayer(tx MetadataTransaction, layer *roLayer) error {
//	if err := tx.SetDiffID(layer.diffID); err != nil {
//		return err
//	}
//	if err := tx.SetSize(layer.size); err != nil {
//		return err
//	}
//	if err := tx.SetCacheID(layer.cacheID); err != nil {
//		return err
//	}
//	// Do not store empty descriptors
//	if layer.descriptor.Digest != "" {
//		if err := tx.SetDescriptor(layer.descriptor); err != nil {
//			return err
//		}
//	}
//	if layer.parent != nil {
//		if err := tx.SetParent(layer.parent.chainID); err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

func newVerifiedReadCloser(rc io.ReadCloser, dgst digest.Digest) (io.ReadCloser, error) {
	verifier, err := digest.NewDigestVerifier(dgst)
	if err != nil {
		return nil, err
	}
	return &verifiedReadCloser{
		rc:       rc,
		dgst:     dgst,
		verifier: verifier,
	}, nil
}

type verifiedReadCloser struct {
	rc       io.ReadCloser
	dgst     digest.Digest
	verifier digest.Verifier
}

func (vrc *verifiedReadCloser) Read(p []byte) (n int, err error) {
	n, err = vrc.rc.Read(p)
	if n > 0 {
		if n, err := vrc.verifier.Write(p[:n]); err != nil {
			return n, err
		}
	}
	if err == io.EOF {
		if !vrc.verifier.Verified() {
			err = fmt.Errorf("could not verify layer data for: %s. This may be caused by layer metadata on disk being corrupted. Re-pulling or rebuilding this image may resolve the issue", vrc.dgst)
		}
	}
	return
}
func (vrc *verifiedReadCloser) Close() error {
	return vrc.rc.Close()
}
