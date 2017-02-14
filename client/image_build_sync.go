package client

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"

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

	hc, err := cli.http2Client(ctx, "/build-attach", query, nil)
	if err != nil {
		return "", err
	}

	cli2 := &Client{}
	*cli2 = *cli
	cli2.client = &hc

	//// TODO: Use translater?
	var excludes []string
	for k, v := range rewrites {
		if v == "" {
			excludes = append(excludes, k)
		}
	}

	a, err := archive.TarWithOptions(dir, &archive.TarOptions{
		ExcludePatterns: excludes,
	})

	_, err = cli2.postRaw(ctx, "/send-content", query, a, nil)
	if err != nil {
		return "", err
	}

	return sessionID, nil
}

// http2Client returns an http client which uses HTTP2 by sending
// an upgrade request to given PATH to create HTTP2 connections.
func (cli *Client) http2Client(ctx context.Context, path string, query url.Values, headers map[string][]string) (http.Client, error) {
	apiPath := cli.getAPIPath(path, query)
	req, err := http.NewRequest("POST", apiPath, nil)
	if err != nil {
		return http.Client{}, err
	}
	req = cli.addHeaders(req, headers)

	req.Host = cli.addr
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "h2c")

	return http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(netw, addr string, cfg *tls.Config) (net.Conn, error) {
				// TODO: ensure valid address

				conn, err := dial(cli.proto, cli.addr, resolveTLSConfig(cli.client.Transport))
				if err != nil {
					if strings.Contains(err.Error(), "connection refused") {
						return nil, fmt.Errorf("cannot connect to the Docker daemon. Is 'docker daemon' running on this host?")
					}
					return nil, err
				}

				clientconn := httputil.NewClientConn(conn, nil)
				defer clientconn.Close()

				// Server hijacks the connection, error 'connection closed' expected
				resp, err := clientconn.Do(req)
				if resp.StatusCode != http.StatusSwitchingProtocols {
					return nil, fmt.Errorf("unable to upgrade to HTTP2")
				}
				if err != nil {
					return nil, err
				}

				c, br := clientconn.Hijack()
				if br.Buffered() > 0 {
					// If there is buffered content, wrap the connection
					c = &hijackedConn{c, br}
				} else {
					br.Reset(nil)
				}

				return c, nil
			},
		},
	}, nil
}

type hijackedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *hijackedConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}
