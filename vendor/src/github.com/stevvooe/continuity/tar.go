package continuity

import (
	"archive/tar"
	"os"
	"sort"
	"strconv"

	"github.com/docker/distribution/digest"
)

type TarContext struct {
	resources []Resource
	hardLinks []hardLink
}

type hardLink struct {
	source string
	target string
}

func NewTarContext() *TarContext {
	return &TarContext{
		resources: []Resource{},
		hardLinks: []hardLink{},
	}
}

func (tc *TarContext) BuildManifest() (*Manifest, error) {
	// TODO: collect by path name
	// TODO: add hard links
	sort.Stable(ByPath(tc.resources))
	return &Manifest{
		Resources: tc.resources,
	}, nil
}

func (tc *TarContext) AddTarHeader(h *tar.Header, dgsts []digest.Digest) error {
	// Unused header fields
	//	Uname      string    // user name of owner
	//	Gname      string    // group name of owner
	//	ChangeTime time.Time // status change time
	// Unused tar types
	//	TypeCont          = '7'    // reserved
	//	TypeXHeader       = 'x'    // extended header
	//	TypeXGlobalHeader = 'g'    // global extended header
	//	TypeGNULongName   = 'L'    // Next file has a long name
	//	TypeGNULongLink   = 'K'    // Next file symlinks to a file w/ a long name
	//	TypeGNUSparse     = 'S'    // sparse file

	xattrs := make(map[string][]byte, len(h.Xattrs))
	for k, v := range xattrs {
		xattrs[k] = []byte(v)
	}

	r := resource{
		paths:  []string{h.Name},
		mode:   os.FileMode(h.Mode),
		uid:    strconv.Itoa(h.Uid),
		gid:    strconv.Itoa(h.Gid),
		xattrs: xattrs,
		mtime:  h.ModTime,
		atime:  h.AccessTime,
	}

	switch h.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		tc.resources = append(tc.resources, &regularFile{
			resource: r,
			size:     h.Size,
			digests:  dgsts,
		})
	case tar.TypeDir:
		r.mode |= os.ModeDir
		tc.resources = append(tc.resources, &directory{r})
	case tar.TypeLink:
		tc.hardLinks = append(tc.hardLinks, hardLink{
			source: h.Name,
			target: h.Linkname,
		})
	case tar.TypeSymlink:
		r.mode |= os.ModeSymlink
		tc.resources = append(tc.resources, &symLink{
			resource: r,
			target:   h.Linkname,
		})
	case tar.TypeChar:
		r.mode |= os.ModeCharDevice
		tc.resources = append(tc.resources, &device{
			resource: r,
			major:    uint64(h.Devmajor),
			minor:    uint64(h.Devminor),
		})
	case tar.TypeBlock:
		r.mode |= os.ModeDevice
		tc.resources = append(tc.resources, &device{
			resource: r,
			major:    uint64(h.Devmajor),
			minor:    uint64(h.Devminor),
		})
	case tar.TypeFifo:
		r.mode |= os.ModeNamedPipe
		tc.resources = append(tc.resources, &namedPipe{
			resource: r,
		})

	}
	return nil
}
