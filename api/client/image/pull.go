package image

import (
	"errors"
	"fmt"

	"golang.org/x/net/context"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/client"
	"github.com/docker/docker/cli"
	"github.com/docker/docker/registry"
	"github.com/spf13/cobra"
)

type pullOptions struct {
	remote string
	all    bool
}

// NewPullCommand creates a new `docker pull` command
func NewPullCommand(dockerCli *client.DockerCli) *cobra.Command {
	var opts pullOptions

	cmd := &cobra.Command{
		Use:   "pull [OPTIONS] NAME[:TAG|@DIGEST]",
		Short: "Pull an image or a repository from a registry",
		Args:  cli.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.remote = args[0]
			return runPull(dockerCli, opts)
		},
	}

	flags := cmd.Flags()

	flags.BoolVarP(&opts.all, "all-tags", "a", false, "Download all tagged images in the repository")
	client.AddTrustedFlags(flags, true)

	return cmd
}

func runPull(dockerCli *client.DockerCli, opts pullOptions) error {
	distributionRef, err := reference.NormalizedName(opts.remote)
	if err != nil {
		return err
	}
	if opts.all && !reference.IsNameOnly(distributionRef) {
		return errors.New("tag can't be used with --all-tags/-a")
	}

	if !opts.all && reference.IsNameOnly(distributionRef) {
		nt := reference.EnsureTagged(distributionRef)
		fmt.Fprintf(dockerCli.Out(), "Using default tag: %s\n", nt.Tag())
		distributionRef = nt
	}

	var tag string
	switch x := distributionRef.(type) {
	case reference.Canonical:
		tag = x.Digest().String()
	case reference.NamedTagged:
		tag = x.Tag()
	}

	registryRef := registry.ParseReference(tag)

	// Resolve the Repository name from fqn to RepositoryInfo
	repoInfo, err := registry.ParseRepositoryInfo(distributionRef)
	if err != nil {
		return err
	}

	ctx := context.Background()

	authConfig := dockerCli.ResolveAuthConfig(ctx, repoInfo.Index)
	requestPrivilege := dockerCli.RegistryAuthenticationPrivilegedFunc(repoInfo.Index, "pull")

	if client.IsTrusted() && !registryRef.HasDigest() {
		// Check if tag is digest
		return dockerCli.TrustedPull(ctx, repoInfo, registryRef, authConfig, requestPrivilege)
	}

	return dockerCli.ImagePullPrivileged(ctx, authConfig, distributionRef.String(), requestPrivilege, opts.all)

}
