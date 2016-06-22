// +build experimental

package plugin

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/client"
	"github.com/docker/docker/cli"
	"github.com/docker/docker/registry"
	"github.com/spf13/cobra"
)

func newPushCommand(dockerCli *client.DockerCli) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push a plugin",
		Args:  cli.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPush(dockerCli, args[0])
		},
	}
	return cmd
}

func runPush(dockerCli *client.DockerCli, name string) error {
	named, err := reference.NormalizedName(name)
	if err != nil {
		return err
	}
	if _, isCanonical := named.(reference.Canonical); isCanonical {
		return fmt.Errorf("invalid name: %s", named.String())
	}
	if reference.IsNameOnly(named) {
		named = reference.EnsureTagged(named)
	}

	ctx := context.Background()

	repoInfo, err := registry.ParseRepositoryInfo(named)
	authConfig := dockerCli.ResolveAuthConfig(ctx, repoInfo.Index)

	encodedAuth, err := client.EncodeAuthToBase64(authConfig)
	if err != nil {
		return err
	}
	return dockerCli.Client().PluginPush(ctx, ref.String(), encodedAuth)
}
