// +build linux

package overlay

import (
	"testing"

	"github.com/docker/docker/daemon/graphdriver"
	"github.com/docker/docker/daemon/graphdriver/graphtest"
	"github.com/docker/docker/pkg/archive"
)

func init() {
	// Do not sure chroot to speed run time and allow archive
	// errors or hangs to be debugged directly from the test process.
	untar = archive.UntarUncompressed
	graphdriver.ApplyUncompressedLayer = archive.ApplyUncompressedLayer
}

// This avoids creating a new driver for each test if all tests are run
// Make sure to put new tests between TestOverlaySetup and TestOverlayTeardown
func TestOverlaySetup(t *testing.T) {
	graphtest.GetDriver(t, "overlay")
}

func TestOverlayCreateEmpty(t *testing.T) {
	graphtest.DriverTestCreateEmpty(t, "overlay")
}

func TestOverlayCreateBase(t *testing.T) {
	graphtest.DriverTestCreateBase(t, "overlay")
}

func TestOverlayCreateSnap(t *testing.T) {
	graphtest.DriverTestCreateSnap(t, "overlay")
}

func TestOverlay50LayerRead(t *testing.T) {
	graphtest.DriverTestDeepLayerRead(t, 50, "overlay")
}

func TestOverlayDiffApply10Files(t *testing.T) {
	graphtest.DriverTestDiffApply(t, 10, "overlay")
}

func TestOverlayTeardown(t *testing.T) {
	graphtest.PutDriver(t)
}

// Run tests with no multi lower option (legacy mode)
func TestOverlaySetupNoML(t *testing.T) {
	graphtest.GetDriver(t, "overlay", "nomultilower")
}

func TestOverlayCreateEmptyNoML(t *testing.T) {
	graphtest.DriverTestCreateEmpty(t, "overlay", "nomultilower")
}

func TestOverlayCreateBaseNoML(t *testing.T) {
	graphtest.DriverTestCreateBase(t, "overlay", "nomultilower")
}

func TestOverlayCreateSnapNoML(t *testing.T) {
	graphtest.DriverTestCreateSnap(t, "overlay", "nomultilower")
}

func TestOverlay50LayerReadNoML(t *testing.T) {
	graphtest.DriverTestDeepLayerRead(t, 50, "overlay", "nomultilower")
}

func TestOverlayDiffApply10FilesNoML(t *testing.T) {
	graphtest.DriverTestDiffApply(t, 10, "overlay", "nomultilower")
}

func TestOverlayTeardownNoML(t *testing.T) {
	graphtest.PutDriver(t)
}

// Benchmarks should always setup new driver

func BenchmarkExists(b *testing.B) {
	graphtest.DriverBenchExists(b, "overlay")
}

func BenchmarkGetEmpty(b *testing.B) {
	graphtest.DriverBenchGetEmpty(b, "overlay")
}

func BenchmarkDiffBase(b *testing.B) {
	graphtest.DriverBenchDiffBase(b, "overlay")
}

func BenchmarkDiffSmallUpper(b *testing.B) {
	graphtest.DriverBenchDiffN(b, 10, 10, "overlay")
}

func BenchmarkDiff10KFileUpper(b *testing.B) {
	graphtest.DriverBenchDiffN(b, 10, 10000, "overlay")
}

func BenchmarkDiff10KFilesBottom(b *testing.B) {
	graphtest.DriverBenchDiffN(b, 10000, 10, "overlay")
}

func BenchmarkDiffApply100(b *testing.B) {
	graphtest.DriverBenchDiffApplyN(b, 100, "overlay")
}

func BenchmarkDiff20Layers(b *testing.B) {
	graphtest.DriverBenchDeepLayerDiff(b, 20, "overlay")
}

func BenchmarkRead20Layers(b *testing.B) {
	graphtest.DriverBenchDeepLayerRead(b, 20, "overlay")
}

func BenchmarkExistsNoML(b *testing.B) {
	graphtest.DriverBenchExists(b, "overlay", "nomultilower")
}

func BenchmarkGetEmptyNoML(b *testing.B) {
	graphtest.DriverBenchGetEmpty(b, "overlay", "nomultilower")
}

func BenchmarkDiffBaseNoML(b *testing.B) {
	graphtest.DriverBenchDiffBase(b, "overlay", "nomultilower")
}

func BenchmarkDiffSmallUpperNoML(b *testing.B) {
	graphtest.DriverBenchDiffN(b, 10, 10, "overlay", "nomultilower")
}

func BenchmarkDiff10KFileUpperNoML(b *testing.B) {
	graphtest.DriverBenchDiffN(b, 10, 10000, "overlay", "nomultilower")
}

func BenchmarkDiff10KFilesBottomNoML(b *testing.B) {
	graphtest.DriverBenchDiffN(b, 10000, 10, "overlay", "nomultilower")
}

func BenchmarkDiffApply100NoML(b *testing.B) {
	graphtest.DriverBenchDiffApplyN(b, 100, "overlay", "nomultilower")
}

func BenchmarkDiff20LayersNoML(b *testing.B) {
	graphtest.DriverBenchDeepLayerDiff(b, 20, "overlay", "nomultilower")
}

func BenchmarkRead20LayersNoML(b *testing.B) {
	graphtest.DriverBenchDeepLayerRead(b, 20, "overlay", "nomultilower")
}
