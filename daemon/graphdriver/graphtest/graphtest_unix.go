// +build linux freebsd

package graphtest

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/pkg/stringid"
)

var (
	drv *Driver
)

// Driver conforms to graphdriver.Driver interface and
// contains information such as root and reference count of the number of clients using it.
// This helps in testing drivers added into the framework.
type Driver struct {
	graphdriver.Driver
	root     string
	refCount int
}

func newDriver(t testing.TB, name string, options []string) *Driver {
	root, err := ioutil.TempDir("", "docker-graphtest-")
	if err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}

	d, err := graphdriver.GetDriver(name, root, options, nil, nil)
	if err != nil {
		t.Logf("graphdriver: %v\n", err)
		if err == graphdriver.ErrNotSupported || err == graphdriver.ErrPrerequisites || err == graphdriver.ErrIncompatibleFS {
			t.Skipf("Driver %s not supported", name)
		}
		t.Fatal(err)
	}
	return &Driver{d, root, 1}
}

func cleanup(t testing.TB, d *Driver) {
	if err := drv.Cleanup(); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(d.root)
}

// GetDriver create a new driver with given name or return a existing driver with the name updating the reference count.
func GetDriver(t testing.TB, name string, options ...string) graphdriver.Driver {
	if drv == nil {
		drv = newDriver(t, name, options)
	} else {
		drv.refCount++
	}
	return drv
}

// PutDriver removes the driver if it is no longer used and updates the reference count.
func PutDriver(t testing.TB) {
	if drv == nil {
		t.Skip("No driver to put!")
	}
	drv.refCount--
	if drv.refCount == 0 {
		cleanup(t, drv)
		drv = nil
	}
}

// DriverTestCreateEmpty creates an new image and verifies it is empty and the right metadata
func DriverTestCreateEmpty(t testing.TB, drivername string, driverOptions ...string) {
	driver := GetDriver(t, drivername, driverOptions...)
	defer PutDriver(t)

	if err := driver.Create("empty", "", "", nil); err != nil {
		t.Fatal(err)
	}

	if !driver.Exists("empty") {
		t.Fatal("Newly created image doesn't exist")
	}

	dir, err := driver.Get("empty", "")
	if err != nil {
		t.Fatal(err)
	}

	verifyFile(t, dir, 0755|os.ModeDir, 0, 0)

	// Verify that the directory is empty
	fis, err := readDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(fis) != 0 {
		t.Fatal("New directory not empty")
	}

	driver.Put("empty")

	if err := driver.Remove("empty"); err != nil {
		t.Fatal(err)
	}

}

// DriverTestCreateBase create a base driver and verify.
func DriverTestCreateBase(t testing.TB, drivername string, driverOptions ...string) {
	driver := GetDriver(t, drivername, driverOptions...)
	defer PutDriver(t)

	createBase(t, driver, "Base")
	verifyBase(t, driver, "Base")

	if err := driver.Remove("Base"); err != nil {
		t.Fatal(err)
	}
}

// DriverTestCreateSnap Create a driver and snap and verify.
func DriverTestCreateSnap(t testing.TB, drivername string, driverOptions ...string) {
	driver := GetDriver(t, drivername, driverOptions...)
	defer PutDriver(t)

	createBase(t, driver, "Base")

	if err := driver.Create("Snap", "Base", "", nil); err != nil {
		t.Fatal(err)
	}

	verifyBase(t, driver, "Snap")

	if err := driver.Remove("Snap"); err != nil {
		t.Fatal(err)
	}

	if err := driver.Remove("Base"); err != nil {
		t.Fatal(err)
	}
}

// DriverTestDeepLayerRead reads a file from a lower layer under a given number of layers
func DriverTestDeepLayerRead(t testing.TB, layerCount int, drivername string, driverOptions ...string) {
	driver := GetDriver(t, drivername, driverOptions...)
	defer PutDriver(t)

	base := stringid.GenerateRandomID()

	if err := driver.Create(base, "", "", nil); err != nil {
		t.Fatal(err)
	}

	content := []byte("test content")
	if err := addFile(driver, base, "testfile.txt", content); err != nil {
		t.Fatal(err)
	}

	topLayer, err := addManyLayers(driver, base, layerCount)
	if err != nil {
		t.Fatal(err)
	}

	err = checkManyLayers(driver, topLayer, layerCount)
	if err != nil {
		t.Fatal(err)
	}

	if err := checkFile(driver, topLayer, "testfile.txt", content); err != nil {
		t.Fatal(err)
	}
}

// DriverTestDiffApply tests diffing and applying produces the same layer
func DriverTestDiffApply(t testing.TB, fileCount int, drivername string, driverOptions ...string) {
	driver := GetDriver(t, drivername, driverOptions...)
	defer PutDriver(t)
	base := stringid.GenerateRandomID()
	upper := stringid.GenerateRandomID()

	if err := driver.Create(base, "", "", nil); err != nil {
		t.Fatal(err)
	}

	if err := addManyFiles(driver, base, fileCount, 3); err != nil {
		t.Fatal(err)
	}

	if err := driver.Create(upper, base, "", nil); err != nil {
		t.Fatal(err)
	}

	if err := addManyFiles(driver, upper, fileCount, 6); err != nil {
		t.Fatal(err)
	}
	diffSize, err := driver.DiffSize(upper, "")
	if err != nil {
		t.Fatal(err)
	}

	diff := stringid.GenerateRandomID()
	if err := driver.Create(diff, base, "", nil); err != nil {
		t.Fatal(err)
	}

	if err := checkManyFiles(driver, diff, fileCount, 3); err != nil {
		t.Fatal(err)
	}

	arch, err := driver.Diff(upper, base)
	if err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	if _, err := buf.ReadFrom(arch); err != nil {
		t.Fatal(err)
	}

	applyDiffSize, err := driver.ApplyDiff(diff, base, bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	if applyDiffSize != diffSize {
		t.Fatalf("Apply diff size different, got %d, expected %s", applyDiffSize, diffSize)
	}
	if err := checkManyFiles(driver, diff, fileCount, 6); err != nil {
		t.Fatal(err)
	}
}
