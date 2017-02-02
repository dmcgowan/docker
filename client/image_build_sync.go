package client

import (
	"io"
	"net/url"

	"golang.org/x/net/context"

	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/stringid"
)

// ImageBuildSync attaches to a build server to start syncing the
// provided directory. Rewrites are provided to make changes to
// what is in the given directory and what is provided to the server.
func (cli *Client) ImageBuildSync(ctx context.Context, dir string, rewrites map[string]string) (string, error) {
	// TODO: Check for existing session, else create new session id
	// TODO: Have serverside create session?
	sessionID := stringid.GenerateRandomID()

	query := url.Values{
		"session": []string{sessionID},
	}

	resp, err := cli.postHijacked(ctx, "/build-attach", query, nil, nil)
	if err != nil {
		return "", err
	}

	defer resp.Conn.Close()

	// TODO: Use translater?
	var excludes []string
	for k, v := range rewrites {
		if v == "" {
			excludes = append(excludes, k)
		}
	}

	a, err := archive.TarWithOptions(dir, &archive.TarOptions{
		ExcludePatterns: excludes,
	})

	if _, err := io.Copy(resp.Conn, a); err != nil {
		return "", err
	}

	return sessionID, nil
}
