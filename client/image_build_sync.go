package client

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"

	"github.com/docker/docker/builder/dockerfile/api"
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

	cc, err := cli.grpcClient(ctx, "/build-attach", query, nil)
	if err != nil {
		return "", err
	}

	client := api.NewDockerfileServiceClient(cc)
	contextClient, err := client.SendContext(ctx)
	if err != nil {
		return "", err
	}

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

	buf := make([]byte, 1<<15)
	req := &api.ContextRequest{
		SessionID: sessionID,
	}

	for {
		n, err := a.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		req.TarContent = buf[:n]
		if err := contextClient.Send(req); err != nil {
			return "", err
		}
		req.SessionID = ""
	}

	_, err = contextClient.CloseAndRecv()
	if err != nil {
		return "", err
	}

	return sessionID, nil
}

// grpcClient returns a grpc client using the provided options for
// establishing an upgraded connection to the grpc server.
func (cli *Client) grpcClient(ctx context.Context, path string, query url.Values, headers map[string][]string) (*grpc.ClientConn, error) {
	dialer, err := cli.http2Dialer(ctx, path, query, headers)
	if err != nil {
		return nil, err
	}

	dialOpt := grpc.WithDialer(func(addr string, d time.Duration) (net.Conn, error) {
		// TODO: verify addr
		// TODO: handle duration
		return dialer()
	})

	return grpc.DialContext(ctx, "", dialOpt, grpc.WithInsecure())
}

// http2Client returns an http client which uses HTTP2 by sending
// an upgrade request to given PATH to create HTTP2 connections.
func (cli *Client) http2Client(ctx context.Context, path string, query url.Values, headers map[string][]string) (http.Client, error) {
	dialer, err := cli.http2Dialer(ctx, path, query, headers)
	if err != nil {
		return http.Client{}, err
	}

	return http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(netw, addr string, cfg *tls.Config) (net.Conn, error) {
				return dialer()
			},
		},
	}, nil
}

// http2Client returns a dialer which uses HTTP2 by sending
// an upgrade request to given PATH to create HTTP2 connections.
func (cli *Client) http2Dialer(ctx context.Context, path string, query url.Values, headers map[string][]string) (func() (net.Conn, error), error) {
	apiPath := cli.getAPIPath(path, query)
	req, err := http.NewRequest("POST", apiPath, nil)
	if err != nil {
		return nil, err
	}
	req = cli.addHeaders(req, headers)

	req.Host = cli.addr
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "h2c")

	return func() (net.Conn, error) {
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
	}, nil
}

type hijackedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *hijackedConn) Read(b []byte) (int, error) {
	return c.r.Read(b)
}
