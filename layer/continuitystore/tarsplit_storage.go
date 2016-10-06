package continuitystore

import (
	"encoding/json"
	"hash"
	"hash/crc64"
	"io"
	"unicode/utf8"

	"github.com/docker/distribution/digest"
	"github.com/pkg/errors"
	"github.com/stevvooe/continuity"
	"github.com/vbatts/tar-split/tar/storage"
)

type manifestPacker struct {
	packer storage.Packer
	cctx   *continuity.TarContext
}

func newManifestPacker(p storage.Packer) *manifestPacker {
	cc := continuity.NewTarContext()
	return &manifestPacker{
		packer: p,
		cctx:   cc,
	}
}

func (mp *manifestPacker) AddEntry(e storage.Entry) (int, error) {
	dgst, err := digest.ParseDigest(e.Name)
	if err == nil {

		hdr := e.GetTarHeader()
		if hdr == nil {
			return 0, errors.New("missing tar header")
		}

		if err := mp.cctx.AddTarHeader(hdr, []digest.Digest{dgst}); err != nil {
			return 0, errors.Wrap(err, "unable to add tar header to continuity context")
		}
	}

	return mp.packer.AddEntry(e)
}

func (mp *manifestPacker) GetManifest() (*continuity.Manifest, error) {
	return mp.cctx.BuildManifest()
}

type blobFileGetter struct {
	provider continuity.ContentProvider
}

func newBlobFileGetter(p continuity.ContentProvider) storage.FileGetter {
	return blobFileGetter{
		provider: p,
	}
}

func (g blobFileGetter) Get(filename string) (io.ReadCloser, error) {
	dgst, err := digest.ParseDigest(filename)
	if err != nil {
		return nil, errors.Wrap(err, "filename could not be interpreted as digest")
	}
	output, err := g.provider.Reader(dgst)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get reader for digest")
	}
	return output, nil
}

type blobFilePutter struct {
	digester continuity.Digester
}

func newBlobFilePutter(d continuity.Digester) storage.FilePutter {
	return blobFilePutter{
		digester: d,
	}
}

func (p blobFilePutter) Put(filename string, input io.Reader) (string, int64, []byte, error) {
	c := newChecksumCalculator()
	dgst, err := p.digester.Digest(io.TeeReader(input, c))
	if err != nil {
		return "", 0, nil, errors.Wrap(err, "unable to save input")
	}
	return dgst.String(), c.Size(), c.Sum(), nil
}

type checksumCalculator struct {
	size int64
	hash hash.Hash64
}

func newChecksumCalculator() *checksumCalculator {
	c := crc64.New(storage.CRCTable)
	return &checksumCalculator{
		hash: c,
	}
}

func (c *checksumCalculator) Write(b []byte) (n int, err error) {
	n, err = c.hash.Write(b)
	c.size = c.size + int64(n)
	return
}

func (c *checksumCalculator) Size() int64 {
	return c.size
}

func (c *checksumCalculator) Sum() []byte {
	return c.hash.Sum(nil)
}

type jsonPacker struct {
	w   io.Writer
	e   *json.Encoder
	pos int
}

func (jp *jsonPacker) AddEntry(e storage.Entry) (int, error) {
	// if Name is not valid utf8, switch it to raw first.
	if e.Name != "" {
		if !utf8.ValidString(e.Name) {
			e.NameRaw = []byte(e.Name)
			e.Name = ""
		}
	}

	e.Position = jp.pos
	err := jp.e.Encode(e)
	if err != nil {
		return -1, err
	}

	// made it this far, increment now
	jp.pos++
	return e.Position, nil
}

func newJSONPacker(w io.Writer) storage.Packer {
	return &jsonPacker{
		w: w,
		e: json.NewEncoder(w),
	}
}

type jsonUnpacker struct {
	dec *json.Decoder
}

func (jup *jsonUnpacker) Next() (*storage.Entry, error) {
	var e storage.Entry
	err := jup.dec.Decode(&e)
	if err != nil {
		return nil, err
	}

	return &e, err
}

func newJSONUnpacker(r io.Reader) storage.Unpacker {
	return &jsonUnpacker{
		dec: json.NewDecoder(r),
	}
}
