package client

import (
	"net/url"

	"github.com/docker/distribution/reference"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// ImageTag tags an image in the docker host
func (cli *Client) ImageTag(ctx context.Context, source, target string) error {
	if _, err := reference.ParseNormalizedNamed(source); err != nil {
		return errors.Wrapf(err, "Error parsing reference: %q is not a valid repository/tag", source)
	}

	ref, err := reference.ParseNormalizedNamed(target)
	if err != nil {
		return errors.Wrapf(err, "Error parsing reference: %q is not a valid repository/tag", target)
	}

	if _, isCanonical := ref.(reference.Canonical); isCanonical {
		return errors.New("refusing to create a tag with a digest reference")
	}

	taggedRef := reference.EnsureTagged(ref)
	tag := taggedRef.Tag()

	query := url.Values{}
	query.Set("repo", reference.FamiliarName(taggedRef))
	query.Set("tag", tag)

	resp, err := cli.post(ctx, "/images/"+source+"/tag", query, nil, nil)
	ensureReaderClosed(resp)
	return err
}
