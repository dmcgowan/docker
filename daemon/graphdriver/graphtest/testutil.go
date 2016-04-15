package graphtest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"

	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/pkg/stringid"
)

func randomContent(size int, seed int64) []byte {
	s := rand.NewSource(seed)
	content := make([]byte, size)

	for i := 0; i < len(content); i += 7 {
		val := s.Int63()
		for j := 0; i+j < len(content) && j < 7; j++ {
			content[i+j] = byte(val)
			val >>= 8
		}
	}

	return content
}

func addFiles(drv graphdriver.Driver, layer string, seed int64) error {
	root, err := drv.Get(layer, "")
	if err != nil {
		return err
	}
	defer drv.Put(layer)

	if err := ioutil.WriteFile(path.Join(root, "file-a"), randomContent(64, seed), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(path.Join(root, "dir-b"), 0755); err != nil {
		return err
	}
	if err := ioutil.WriteFile(path.Join(root, "dir-b", "file-b"), randomContent(128, seed+1), 0755); err != nil {
		return err
	}

	return ioutil.WriteFile(path.Join(root, "file-c"), randomContent(128*128, seed+2), 0755)
}

func checkFile(drv graphdriver.Driver, layer, filename string, content []byte) error {
	root, err := drv.Get(layer, "")
	if err != nil {
		return err
	}
	defer drv.Put(layer)

	fileContent, err := ioutil.ReadFile(path.Join(root, filename))
	if err != nil {
		return err
	}

	if bytes.Compare(fileContent, content) != 0 {
		return fmt.Errorf("mismatched file content %v, expecting %v", fileContent, content)
	}

	return nil
}

func addFile(drv graphdriver.Driver, layer, filename string, content []byte) error {
	root, err := drv.Get(layer, "")
	if err != nil {
		return err
	}
	defer drv.Put(layer)

	return ioutil.WriteFile(path.Join(root, filename), content, 0755)
}

func addManyFiles(drv graphdriver.Driver, layer string, count int, seed int64) error {
	root, err := drv.Get(layer, "")
	if err != nil {
		return err
	}
	defer drv.Put(layer)

	for i := 0; i < count; i += 100 {
		dir := path.Join(root, fmt.Sprintf("directory-%d", i))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		for j := 0; i+j < count && j < 100; j++ {
			file := path.Join(dir, fmt.Sprintf("file-%d", i+j))
			if err := ioutil.WriteFile(file, randomContent(64, seed+int64(i+j)), 0755); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkManyFiles(drv graphdriver.Driver, layer string, count int, seed int64) error {
	root, err := drv.Get(layer, "")
	if err != nil {
		return err
	}
	defer drv.Put(layer)

	for i := 0; i < count; i += 100 {
		dir := path.Join(root, fmt.Sprintf("directory-%d", i))
		for j := 0; i+j < count && j < 100; j++ {
			file := path.Join(dir, fmt.Sprintf("file-%d", i+j))
			fileContent, err := ioutil.ReadFile(file)
			if err != nil {
				return err
			}

			content := randomContent(64, seed+int64(i+j))

			if bytes.Compare(fileContent, content) != 0 {
				return fmt.Errorf("mismatched file content %v, expecting %v", fileContent, content)
			}
		}
	}

	return nil
}

func addLayerFiles(drv graphdriver.Driver, layer, parent string, i int) error {
	root, err := drv.Get(layer, "")
	if err != nil {
		return err
	}
	defer drv.Put(layer)

	if err := ioutil.WriteFile(path.Join(root, "top-id"), []byte(layer), 0755); err != nil {
		return err
	}
	layerDir := path.Join(root, fmt.Sprintf("layer-%d", i))
	if err := os.MkdirAll(layerDir, 0755); err != nil {
		return err
	}
	if err := ioutil.WriteFile(path.Join(layerDir, "layer-id"), []byte(layer), 0755); err != nil {
		return err
	}
	if err := ioutil.WriteFile(path.Join(layerDir, "parent-id"), []byte(parent), 0755); err != nil {
		return err
	}

	return nil
}

func addManyLayers(drv graphdriver.Driver, baseLayer string, count int) (string, error) {
	lastLayer := baseLayer
	for i := 1; i <= count; i++ {
		nextLayer := stringid.GenerateRandomID()
		if err := drv.Create(nextLayer, lastLayer, "", nil); err != nil {
			return "", err
		}
		if err := addLayerFiles(drv, nextLayer, lastLayer, i); err != nil {
			return "", err
		}

		lastLayer = nextLayer

	}
	return lastLayer, nil
}

func checkManyLayers(drv graphdriver.Driver, layer string, count int) error {
	root, err := drv.Get(layer, "")
	if err != nil {
		return err
	}
	defer drv.Put(layer)

	layerIDBytes, err := ioutil.ReadFile(path.Join(root, "top-id"))
	if err != nil {
		return err
	}

	if bytes.Compare(layerIDBytes, []byte(layer)) != 0 {
		return fmt.Errorf("mismatched file content %v, expecting %v", layerIDBytes, []byte(layer))
	}

	for i := count; i > 0; i-- {
		layerDir := path.Join(root, fmt.Sprintf("layer-%d", i))

		thisLayerIDBytes, err := ioutil.ReadFile(path.Join(layerDir, "layer-id"))
		if err != nil {
			return err
		}
		if bytes.Compare(thisLayerIDBytes, layerIDBytes) != 0 {
			return fmt.Errorf("mismatched file content %v, expecting %v", thisLayerIDBytes, layerIDBytes)
		}
		layerIDBytes, err = ioutil.ReadFile(path.Join(layerDir, "parent-id"))
		if err != nil {
			return err
		}
	}
	return nil
}

// readDir reads a directory just like ioutil.ReadDir()
// then hides specific files (currently "lost+found")
// so the tests don't "see" it
func readDir(dir string) ([]os.FileInfo, error) {
	a, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	b := a[:0]
	for _, x := range a {
		if x.Name() != "lost+found" { // ext4 always have this dir
			b = append(b, x)
		}
	}

	return b, nil
}
