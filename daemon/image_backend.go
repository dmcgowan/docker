package daemon

import (
	"io"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/image"
	"github.com/docker/docker/layer"
	digest "github.com/opencontainers/go-digest"
)

type ImageBackend interface {
	// Layerstore
	CreateRWLayer(id string, parent ChainID, opts *CreateRWLayerOpts) (RWLayer, error)
	GetRWLayer(id string) (RWLayer, error)
	ReleaseRWLayer(layer.RWLayer) ([]layer.Metadata, error)
	Register(io.Reader, layer.ChainID) (layer.Layer, error)
	GetRWLayer(id string) (layer.RWLayer, error)
	DriverName() string // To name graph driver
	GetMountID(id string) (string, error)
	Cleanup() error
	Map() map[layer.ChainID]layer.Layer // For usage
	Get(layer.ChainID) (layer.Layer, error)
	Register(io.Reader, layer.ChainID) (layer.Layer, error)

	// References
	References(id digest.Digest) []reference.Named
	Get(ref reference.Named) (digest.Digest, error)
	Delete(ref reference.Named) (bool, error)
	AddTag(ref reference.Named, id digest.Digest, force bool) error

	// From image package (everything gets called)
	Get(id image.ID) (*image.Image, error)
	Create(config []byte) (image.ID, error)
	SetParent(id image.ID, parent image.ID) error
	GetParent(id image.ID) (image.ID, error)
	Map() map[image.ID]*image.Image
	Heads() map[image.ID]*image.Image
	Search(partialID string) (image.ID, error)
	Children(id image.ID) []ID
	Delete(id image.ID) ([]layer.Metadata, error)

	// Pull
	//func (daemon *Daemon) PullImage(ctx context.Context, image, tag string, metaHeaders map[string][]string, authConfig *types.AuthConfig, outStream io.Writer) error {
	//func (daemon *Daemon) GetRepository(ctx context.Context, ref reference.NamedTagged, authConfig *types.AuthConfig) (dist.Repository, bool, error) {
	//func (daemon *Daemon) PullOnBuild(ctx context.Context, name string, authConfigs map[string]types.AuthConfig, output io.Writer) (builder.Image, error) {
	//func (daemon *Daemon) PushImage(ctx context.Context, image, tag string, metaHeaders map[string][]string, authConfig *types.AuthConfig, outStream io.Writer) error {
}

// Needs to incorporate this
// func (daemon *Daemon) MakeImageCache(sourceRefs []string) builder.ImageCache {
// v1.Migrate(config.Root, graphDriver, d.layerStore, d.imageStore, referenceStore, distributionMetadataStore); err != nil {
// imageExporter := tarexport.NewTarExporter(daemon.imageStore, daemon.layerStore, daemon.referenceStore, daemon)
// distribution.NewImageConfigStoreFromStore(daemon.imageStore),
// defer layer.ReleaseAndLog(daemon.layerStore, l)
// d.downloadManager = xfer.NewLayerDownloadManager(d.layerStore, *config.MaxConcurrentDownloads)
// creation of distribution.Config
