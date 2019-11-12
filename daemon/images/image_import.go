package images // import "github.com/docker/docker/daemon/images"

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/containerd/containerd/archive/compression"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/images/archive"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/builder/remotecontext"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/layer"
	"github.com/docker/docker/pkg/progress"
	"github.com/docker/docker/pkg/streamformatter"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// ImportImage imports an image, getting the archived layer data either from
// inConfig (if src is "-"), or from a URI specified in src. Progress output is
// written to outStream. Repository and tag names can optionally be given in
// the repo and tag arguments, respectively.
func (i *ImageService) ImportImage(
	ctx context.Context,
	src string,
	repository string,
	tag string,
	platform *ocispec.Platform,
	msg string,
	inConfig io.ReadCloser,
	outStream io.Writer,
	changes []string,
) error {
	var (
		rc     io.ReadCloser
		resp   *http.Response
		newRef reference.Named
	)

	if repository != "" {
		var err error
		newRef, err = reference.ParseNormalizedNamed(repository)
		if err != nil {
			return errdefs.InvalidParameter(err)
		}
		if _, isCanonical := newRef.(reference.Canonical); isCanonical {
			return errdefs.InvalidParameter(errors.New("cannot import digest reference"))
		}

		if tag != "" {
			_, err = reference.WithTag(newRef, tag)
			if err != nil {
				return errdefs.InvalidParameter(err)
			}
		}
	}

	if src == "-" {
		rc = inConfig
	} else {
		inConfig.Close()
		if strings.Count(src, "://") == 0 {
			src = "http://" + src
		}
		u, err := url.Parse(src)
		if err != nil {
			return errdefs.InvalidParameter(err)
		}

		resp, err = remotecontext.GetWithStatusError(u.String())
		if err != nil {
			return err
		}
		outStream.Write(streamformatter.FormatStatus("", "Downloading from %s", u))
		progressOutput := streamformatter.NewJSONProgressOutput(outStream, true)
		rc = progress.NewProgressReader(resp.Body, progressOutput, resp.ContentLength, "", "Importing")
	}

	defer rc.Close()
	if len(msg) == 0 {
		msg = "Imported from " + src
	}

	// TODO: doing this so that I can quickly test how this works, but we probably
	// don't want to buffer this entire thing into memory. Use a pipe or similar.
	buf := &bytes.Buffer{}
	tee := io.TeeReader(rc, buf)

	inflatedLayerData, err := compression.DecompressStream(tee)
	if err != nil {
		return err
	}
	defer inflatedLayerData.Close()

	if platform == nil {
		platform = &i.defaultPlatform
	}

	// Tee into layerstore and content store
	layerStore, err := i.GetLayerStore(*platform)
	if err != nil {
		return err
	}
	l, err := layerStore.Register(inflatedLayerData, "")
	if err != nil {
		return err
	}
	defer layer.ReleaseAndLog(layerStore, l)

	ctx, done, err := i.client.WithLease(ctx)
	if err != nil {
		return err
	}
	defer done(ctx)

	desc, err := archive.ImportIndex(ctx, i.client.ContentStore(), buf)
	if err != nil {
		return err
	}

	created := time.Now().UTC()
	img := images.Image{
		Name: src,
		// Labels: map[string]string
		Target:    desc,
		CreatedAt: created,
	}

	img, err = i.client.ImageService().Create(ctx, img)
	if err != nil {
		return err
	}

	// FIXME: connect with commit code and call refstore directly
	if newRef != nil {
		if err := i.TagImageWithReference(ctx, img.Target, newRef); err != nil {
			return err
		}
	}

	// TODO: is the ID the digest or the name?
	id := img.Target.Digest.String()
	i.LogImageEvent(ctx, id, img.Name, "import")
	outStream.Write(streamformatter.FormatStatus("", id))
	return nil
}
