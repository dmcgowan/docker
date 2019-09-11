package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/plugin"
	srvconfig "github.com/containerd/containerd/services/server/config"
	"github.com/containerd/containerd/snapshots"
	"github.com/docker/docker/daemon/config"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// initPlugins initializes plugins using containerd's plugin system,
// registering any services with the daemon's grpc servers
func (cli *DaemonCli) initPlugins(ctx context.Context, c *config.Config) error {
	if c.ContainerdAddr != "" {
		//gopts := []grpc.DialOption{
		//	grpc.WithInsecure(),
		//	grpc.WithBackoffMaxDelay(3 * time.Second),
		//	grpc.WithDialer(dialer.Dialer),

		//	// TODO(stevvooe): We may need to allow configuration of this on the client.
		//	grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaults.DefaultMaxRecvMsgSize)),
		//	grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(defaults.DefaultMaxSendMsgSize)),
		//}

		//containerdCli, err := containerd.New(c.ContainerdAddr, containerd.WithDefaultNamespace(c.ContainerdNamespace), containerd.WithDialOpts(gopts), containerd.WithTimeout(60*time.Second))
		//if err != nil {
		//	return nil, errors.Wrapf(err, "failed to dial %q", c.ContainerdAddr)
		//}

		// TODO: Initialize containerd plugins
		// Support embedded containerd mode
	}
	registerPlugins()

	// TODO: Get disabled plugins from config
	// This is updated to a filter in a new version of containerd
	var disabled []string
	if cli.grpcServer == nil {
		disabled = []string{
			"io.containerd.grpc.v1.containers",
			"io.containerd.grpc.v1.content",
			"io.containerd.grpc.v1.diff",
			"io.containerd.grpc.v1.events",
			"io.containerd.grpc.v1.healthcheck",
			"io.containerd.grpc.v1.images",
			"io.containerd.grpc.v1.leases",
			"io.containerd.grpc.v1.namespaces",
			"io.containerd.internal.v1.opt",
			"io.containerd.grpc.v1.snapshots",
			"io.containerd.grpc.v1.tasks",
			"io.containerd.grpc.v1.version",
			"io.containerd.grpc.v1.introspection",
		}
	}

	plugins := plugin.Graph(disabled)
	initialized := plugin.NewPluginSet()
	for _, p := range plugins {
		id := p.URI()
		log.G(ctx).WithField("type", p.Type).Infof("loading plugin %q...", id)

		initContext := plugin.NewContext(
			ctx,
			p,
			initialized,
			c.Root,
			"",
		)

		// load the plugin specific configuration if it is provided
		if p.Config != nil {
			cc, ok := c.Plugins[id]
			if ok {
				err := json.Unmarshal([]byte(cc), p.Config)
				if err != nil {
					return err
				}
			}
			initContext.Config = p.Config
		}
		result := p.Init(initContext)
		if err := initialized.Add(result); err != nil {
			return errors.Wrapf(err, "could not add plugin result to plugin set")
		}

		instance, err := result.Instance()
		if err != nil {
			if plugin.IsSkipPlugin(err) {
				log.G(ctx).WithError(err).WithField("type", p.Type).Infof("skip loading plugin %q...", id)
			} else {
				log.G(ctx).WithError(err).Warnf("failed to load plugin %s", id)
			}

			// TODO: Check if required
			continue
		}

		if svc, ok := instance.(plugin.Service); ok {
			if cli.grpcServer != nil {
				svc.Register(cli.grpcServer)
			} else {
				log.G(ctx).Warnf("no gRPC server to register %s", id)
			}
		}
		if svc, ok := instance.(plugin.TCPService); ok {
			svc.RegisterTCP(cli.tcpServer)
		}

		cli.plugins = append(cli.plugins, result)
	}
	// TODO: Check if any unloaded requried plugins

	return nil
}

func registerPlugins() {
	// load additional plugins that don't automatically register themselves
	plugin.Register(&plugin.Registration{
		Type: plugin.ContentPlugin,
		ID:   "content",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Exports["root"] = ic.Root
			return local.NewStore(ic.Root)
		},
	})
	plugin.Register(&plugin.Registration{
		Type: plugin.MetadataPlugin,
		ID:   "bolt",
		Requires: []plugin.Type{
			plugin.ContentPlugin,
			plugin.SnapshotPlugin,
		},
		Config: &srvconfig.BoltConfig{
			ContentSharingPolicy: srvconfig.SharingPolicyShared,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			if err := os.MkdirAll(ic.Root, 0711); err != nil {
				return nil, err
			}
			cs, err := ic.Get(plugin.ContentPlugin)
			if err != nil {
				return nil, err
			}

			snapshottersRaw, err := ic.GetByType(plugin.SnapshotPlugin)
			if err != nil {
				return nil, err
			}

			snapshotters := make(map[string]snapshots.Snapshotter)
			for name, sn := range snapshottersRaw {
				sn, err := sn.Instance()
				if err != nil {
					if !plugin.IsSkipPlugin(err) {
						log.G(ic.Context).WithError(err).
							Warnf("could not use snapshotter %v in metadata plugin", name)
					}
					continue
				}
				snapshotters[name] = sn.(snapshots.Snapshotter)
			}

			shared := true
			ic.Meta.Exports["policy"] = srvconfig.SharingPolicyShared
			if cfg, ok := ic.Config.(*srvconfig.BoltConfig); ok {
				if cfg.ContentSharingPolicy != "" {
					if err := cfg.Validate(); err != nil {
						return nil, err
					}
					if cfg.ContentSharingPolicy == srvconfig.SharingPolicyIsolated {
						ic.Meta.Exports["policy"] = srvconfig.SharingPolicyIsolated
						shared = false
					}

					log.L.WithField("policy", cfg.ContentSharingPolicy).Info("metadata content store policy set")
				}
			}

			path := filepath.Join(ic.Root, "meta.db")
			ic.Meta.Exports["path"] = path

			db, err := bolt.Open(path, 0644, nil)
			if err != nil {
				return nil, err
			}

			var dbopts []metadata.DBOpt
			if !shared {
				dbopts = append(dbopts, metadata.WithPolicyIsolated)
			}
			mdb := metadata.NewDB(db, cs.(content.Store), snapshotters, dbopts...)
			if err := mdb.Init(ic.Context); err != nil {
				return nil, err
			}
			return mdb, nil
		},
	})
}
