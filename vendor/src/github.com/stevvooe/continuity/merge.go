package continuity

import (
	"log"
	"path/filepath"
	"sort"
	"strings"
)

// MergeOverlay merges a manifest created from an overlay diff
// onto another manifest. Only the diff manifest should contain
// whiteout information.
func MergeOverlay(manifest, diff *Manifest) *Manifest {
	r1 := manifest.Resources
	sort.Sort(ByPath(r1))
	r2 := diff.Resources
	sort.Sort(ByPath(r2))
	return mergeResources(r1, r2, overlayWhiteout, pathLess)
}

// MergeOverlay merges a manifest created from an aufs diff
// onto another manifest. Only the diff manifest should contain
// whiteout information.
func MergeAUFS(manifest, diff *Manifest) *Manifest {
	r1 := manifest.Resources
	sort.Sort(byAUFSPath(r1))
	r2 := diff.Resources
	sort.Sort(byAUFSPath(r2))
	return mergeResources(r1, r2, aufsWhiteout, aufsPathLess)
}

func overlayWhiteout(r Resource) (string, bool, Resource) {
	switch resource := r.(type) {
	case Directory:
		if xattr, ok := resource.XAttrs()["trusted.overlay.opaque"]; ok {
			if len(xattr) == 1 && xattr[0] == 'y' {
				// TODO: strip opaque flag
				return resource.Path(), true, resource
			}
		}

	case Device:
		if resource.Major() == 0 && resource.Minor() == 0 {
			return resource.Path(), false, nil
		}
	}
	return "", false, nil
}

const (
	aufsWhiteoutPrefix = ".wh."
	aufsOpaqueDir      = ".wh..wh..opq"
)

func aufsWhiteout(r Resource) (string, bool, Resource) {
	if _, ok := r.(RegularFile); ok {
		dir, fname := filepath.Split(r.Path())
		if fname == aufsOpaqueDir {
			log.Printf("Opaque: %s", dir)
			return dir, true, nil
		}
		if strings.HasPrefix(fname, aufsWhiteoutPrefix) {
			log.Printf("Whiteout: %s", dir+fname[4:])
			return dir + fname[4:], false, nil
		}
	}
	return "", false, nil
}

func pathLess(p1, p2 string) bool {
	return p1 < p2
}

type byAUFSPath []Resource

func (bp byAUFSPath) Len() int      { return len(bp) }
func (bp byAUFSPath) Swap(i, j int) { bp[i], bp[j] = bp[j], bp[i] }
func (bp byAUFSPath) Less(i, j int) bool {
	return aufsPathLess(bp[i].Path(), bp[j].Path())
}

func aufsPathLess(p1, p2 string) bool {
	d1, n1 := filepath.Split(p1)
	d2, n2 := filepath.Split(p2)
	if d1 == d2 {
		return aufsLess(n1, n2)
	}
	if len(d1) < len(d2) && strings.HasPrefix(d2, d1) {
		return aufsLess(p1[len(d1):], p2[len(d1):])
	}
	if len(d1) > len(d2) && strings.HasPrefix(d1, d2) {
		return aufsLess(p1[len(d2):], p2[len(d2):])
	}

	return p1 < p2
}

func aufsLess(n1, n2 string) bool {
	if strings.HasPrefix(n1, ".wh.") {
		if strings.HasPrefix(n2, ".wh.") {
			return aufsLess(n1[4:], n2[4:])
		}
		return n2 != ""
	} else if strings.HasPrefix(n2, ".wh.") {
		return n1 == ""
	}
	return n1 < n2
}

// whiteoutContext stores all whiteouts and allows checking
// resource paths for whiteout matches.
// TODO: optimize lookup by using tree structure
type whiteoutContext struct {
	whiteoutFiles []string
	whiteoutDirs  []string
}

// addWhiteout adds the given name as a whiteout,
// to be treated as a whited out file as well as
// excluded directory.
// TODO: add terminal node (any value under is whited out)
// TODO: add terminal record in parent node (any value with same name in parent is whited out)
func (wc *whiteoutContext) addWhiteout(name string) {
	wc.whiteoutFiles = append(wc.whiteoutFiles, name)
	wc.whiteoutDirs = append(wc.whiteoutFiles, asDir(name))
}

// isWhitedOut checks whether the given path name has a corresponding
// whiteout. Input processing should be ordered to handle whiteouts
// before files which may be whited out.
// TODO: optimize lookup, currently method is brute force search
func (wc *whiteoutContext) isWhitedOut(name string) bool {
	for _, f := range wc.whiteoutFiles {
		if f == name {
			return true
		}
	}
	for _, f := range wc.whiteoutDirs {
		if strings.HasPrefix(name, f) {
			return true
		}
	}
	return false
}

type whiteout func(Resource) (string, bool, Resource)

// mergeResources merges two ordered set of resources into manifest
// using the provided whiteout and comparison function.
// TODO(dmcgowan): Handle handlinks
func mergeResources(r1, r2 []Resource, wof whiteout, less func(string, string) bool) *Manifest {
	result := make([]Resource, 0, len(r1))
	wc := whiteoutContext{}

	i1 := 0
	i2 := 0

	for i1 < len(r1) && i2 < len(r2) {
		p1 := r1[i1].Path()
		p2 := r2[i2].Path()

		switch {
		case less(p1, p2):
			if !wc.isWhitedOut(p1) {
				result = append(result, r1[i1])
			}
			i1++
		case p1 == p2:
			// p1 will be replaced by p2
			i1++
			fallthrough
		default:
			wo, opaque, r := wof(r2[i2])
			if wo != "" {
				if opaque {
					wod := asDir(wo)
					for i1 < len(r1) && strings.HasPrefix(r1[i1].Path(), wod) {
						// Ignore resource in opaque directory
						i1++
					}
					// Check if opaque resource should be added
					if r != nil {
						result = append(result, r)
					}
				} else {
					wc.addWhiteout(wo)
				}
			} else {
				result = append(result, r2[i2])
			}
			i2++
		}
	}

	for i1 < len(r1) {
		if !wc.isWhitedOut(r1[i1].Path()) {
			result = append(result, r1[i1])
		}
		i1++

	}
	for i2 < len(r2) {
		wo, opaque, r := wof(r2[i2])
		if wo != "" {
			// Don't add further whiteouts unless opaque directory
			if opaque && r != nil {
				result = append(result, r)
			}

		} else {
			result = append(result, r2[i2])
		}
		i2++
	}

	return &Manifest{
		Resources: result,
	}
}

func asDir(name string) string {
	if name == "" || name[len(name)-1] != '/' {
		return name + "/"
	}
	return name
}
