package client

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/digest"
	"github.com/docker/distribution/registry/client/auth"
	"github.com/docker/distribution/registry/client/transport"
	"github.com/docker/docker/cliconfig"
	"github.com/docker/docker/pkg/homedir"
	"github.com/docker/docker/pkg/ansiescape"
	"github.com/docker/docker/pkg/ioutils"
	flag "github.com/docker/docker/pkg/mflag"
	"github.com/docker/docker/pkg/term"
	"github.com/docker/docker/pkg/tlsconfig"
	"github.com/docker/docker/registry"
	"github.com/docker/notary/client"
	"github.com/docker/notary/keystoremanager"
	"github.com/endophage/gotuf/data"
)

var untrusted bool

func addTrustedFlags(fs *flag.FlagSet, verify bool) {
	var trusted bool
	if e := os.Getenv("DOCKER_NOTARY"); e != "" {
		var err error
		trusted, err = strconv.ParseBool(e)
		if err != nil {
			// treat any other value as true
			trusted = true
		}
	}
	message := "Skip image signing"
	if verify {
		message = "Skip image verification"
	}
	fs.BoolVar(&untrusted, []string{"-untrusted"}, !trusted, message)
}

func isTrusted(name string) bool {
	// TODO(dmcgowan): false if name is hex image id
	// TODO(dmcgowan): whitelist specific names to be untrusted
	// TODO(dmcgowan): canonicalize name before checking whitelist
	return !untrusted
}

var targetRegexp = regexp.MustCompile(`([\S]+): digest: ([\S]+) size: ([\d]+)`)

type target struct {
	reference registry.Reference
	digest    digest.Digest
	size      int64
}

func trustDirectory() string {
	if e := os.Getenv("NOTARY_TRUST_DIR"); e != "" {
		return e
	}
	return filepath.Join(homedir.Get(), ".docker", "trust")
}

func trustServer(index *registry.IndexInfo) string {
	if s := os.Getenv("NOTARY_SERVER"); s != "" {
		if !strings.HasPrefix(s, "http") {
			return "https://" + s
		}
		return s
	}
	if index.Official {
		return registry.NOTARYSERVER
	}
	return "https://" + index.Name
}

type simpleCredentialStore struct {
	auth cliconfig.AuthConfig
}

func (scs simpleCredentialStore) Basic(u *url.URL) (string, string) {
	return scs.auth.Username, scs.auth.Password
}

func (cli *DockerCli) getNotaryRepository(repoInfo *registry.RepositoryInfo, authConfig cliconfig.AuthConfig) (*client.NotaryRepository, error) {
	server := trustServer(repoInfo.Index)
	var cfg = tlsconfig.ClientDefault
	cfg.InsecureSkipVerify = !repoInfo.Index.Secure
	// TODO(dmcgowan): load certificates
	base := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     &cfg,
		DisableKeepAlives:   true,
	}

	// Skip configuration headers since request is not going to Docker daemon
	modifiers := registry.DockerHeaders(http.Header{})
	authTransport := transport.NewTransport(base, modifiers...)
	pingClient := &http.Client{
		Transport: authTransport,
		Timeout:   5 * time.Second,
	}
	endpointStr := server + "/v2/"
	req, err := http.NewRequest("GET", endpointStr, nil)
	if err != nil {
		return nil, err
	}
	resp, err := pingClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	challengeManager := auth.NewSimpleChallengeManager()
	if err := challengeManager.AddResponse(resp); err != nil {
		return nil, err
	}

	creds := simpleCredentialStore{auth: authConfig}
	tokenHandler := auth.NewTokenHandler(authTransport, creds, repoInfo.RemoteName, "push", "pull")
	basicHandler := auth.NewBasicHandler(creds)
	modifiers = append(modifiers, transport.RequestModifier(auth.NewAuthorizer(challengeManager, tokenHandler, basicHandler)))
	tr := transport.NewTransport(base, modifiers...)

	return client.NewNotaryRepository(trustDirectory(), repoInfo.RemoteName, server, tr)
}

func convertTarget(t client.Target) (target, error) {
	h, ok := t.Hashes["sha256"]
	if !ok {
		return target{}, errors.New("no valid hash, expecting sha256")
	}
	return target{
		reference: registry.ParseReference(t.Name),
		digest:    digest.NewDigestFromHex("sha256", hex.EncodeToString(h)),
		size:      t.Length,
	}, nil
}

func (cli *DockerCli) getPassphrase() (string, error) {
	oldState, err := term.SaveState(cli.inFd)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(cli.out, "Passphrase: ")
	term.DisableEcho(cli.inFd, oldState)

	reader := bufio.NewReader(cli.in)
	line, _, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	passphrase := string(line)
	passphrase = strings.TrimSpace(passphrase)
	fmt.Fprint(cli.out, "\n")

	term.RestoreTerminal(cli.inFd, oldState)
	if len(passphrase) < 6 {
		fmt.Fprintf(cli.out, "Invalid passphrase, minimum length of 6")
	}

	return passphrase, nil
}

func (cli *DockerCli) trustedReference(repo string, ref registry.Reference) (registry.Reference, error) {
	repoInfo, err := registry.ParseRepositoryInfo(repo)
	if err != nil {
		return nil, err
	}

	// Resolve the Auth config relevant for this server
	authConfig := registry.ResolveAuthConfig(cli.configFile, repoInfo.Index)

	notaryRepo, err := cli.getNotaryRepository(repoInfo, authConfig)
	if err != nil {
		fmt.Fprintf(cli.out, "Error establishing connection to notary repository: %s\n", err)
		return nil, err
	}

	t, err := notaryRepo.GetTargetByName(ref.String())
	if err != nil {
		return nil, err
	}
	r, err := convertTarget(*t)
	if err != nil {
		return nil, err

	}

	return registry.DigestReference(r.digest), nil
}

func (cli *DockerCli) tagTrusted(repoInfo *registry.RepositoryInfo, trustedRef, ref registry.Reference) error {
	fullName := trustedRef.ImageName(repoInfo.LocalName)
	fmt.Fprintf(cli.out, "Tagging %s as %s\n", fullName, ref.ImageName(repoInfo.LocalName))
	tv := url.Values{}
	tv.Set("repo", repoInfo.LocalName)
	tv.Set("tag", ref.String())
	tv.Set("force", "1")

	if _, _, err := readBody(cli.call("POST", "/images/"+fullName+"/tag?"+tv.Encode(), nil, nil)); err != nil {
		return err
	}

	return nil
}

func (cli *DockerCli) trustedPull(repoInfo *registry.RepositoryInfo, ref registry.Reference, authConfig cliconfig.AuthConfig) error {
	var (
		v    = url.Values{}
		refs = []target{}
	)

	notaryRepo, err := cli.getNotaryRepository(repoInfo, authConfig)
	if err != nil {
		fmt.Fprintf(cli.out, "Error establishing connection to notary repository: %s\n", err)
		return err
	}

	if ref.String() == "" {
		// List all targets
		targets, err := notaryRepo.ListTargets()
		if err != nil {
			return err
		}
		for _, tgt := range targets {
			t, err := convertTarget(*tgt)
			if err != nil {
				fmt.Fprintf(cli.out, "Skipping target for %q\n", repoInfo.LocalName)
				continue
			}
			refs = append(refs, t)
		}
	} else {
		t, err := notaryRepo.GetTargetByName(ref.String())
		if err != nil {
			return err
		}
		r, err := convertTarget(*t)
		if err != nil {
			return err

		}
		refs = append(refs, r)
	}

	v.Set("fromImage", repoInfo.LocalName)
	for i, r := range refs {
		displayTag := r.reference.String()
		if displayTag != "" {
			displayTag = ":" + displayTag
		}
		fmt.Fprintf(cli.out, "Pull (%d of %d): %s%s@%s\n", i+1, len(refs), repoInfo.LocalName, displayTag, r.digest)
		v.Set("tag", r.digest.String())

		_, _, err = cli.clientRequestAttemptLogin("POST", "/images/create?"+v.Encode(), nil, cli.out, repoInfo.Index, "pull")
		if err != nil {
			return err
		}

		// If reference is not trusted, tag by trusted reference
		if !r.reference.HasDigest() {
			if err := cli.tagTrusted(repoInfo, registry.DigestReference(r.digest), r.reference); err != nil {
				return err

			}
		}
	}
	return nil
}

func targetStream(in io.Writer) (io.WriteCloser, <-chan []target) {
	// Create multiWriter
	r, w := io.Pipe()
	// Capture output
	out := io.MultiWriter(in, w)
	// output chan
	targetChan := make(chan []target)

	go func() {
		targets := []target{}
		scanner := bufio.NewScanner(r)
		scanner.Split(ansiescape.ScanANSILines)
		// New splitter
		for scanner.Scan() {
			line := scanner.Bytes()
			if matches := targetRegexp.FindSubmatch(line); len(matches) == 4 {
				dgst, err := digest.ParseDigest(string(matches[2]))
				if err != nil {
					fmt.Fprintf(in, "Bad digest value %q, ignoring\n", string(matches[2]))
					continue
				}
				s, err := strconv.ParseInt(string(matches[3]), 10, 64)
				if err != nil {
					fmt.Fprintf(in, "Bad size value %q, ignoring\n", string(matches[3]))
					continue
				}

				targets = append(targets, target{
					reference: registry.ParseReference(string(matches[1])),
					digest:    dgst,
					size:      s,
				})
			}
		}
		targetChan <- targets
	}()

	return ioutils.NewWriteCloserWrapper(out, w.Close), targetChan
}

func (cli *DockerCli) trustedPush(repoInfo *registry.RepositoryInfo, tag string, authConfig cliconfig.AuthConfig) error {
	streamOut, targetChan := targetStream(cli.out)

	v := url.Values{}
	v.Set("tag", tag)

	_, _, err := cli.clientRequestAttemptLogin("POST", "/images/"+repoInfo.LocalName+"/push?"+v.Encode(), nil, streamOut, repoInfo.Index, "push")
	// Close stream channel to finish target parsing
	if err := streamOut.Close(); err != nil {
		return err
	}
	// Check error from request
	if err != nil {
		return err
	}

	// Get target results
	targets := <-targetChan

	if tag == "" {
		fmt.Fprintf(cli.out, "No tag specified, skipping TUF metadata push\n")
		return nil
	}
	if len(targets) == 0 {
		fmt.Fprintf(cli.out, "No targets found, skipping TUF metadata push\n")
		return nil
	}

	fmt.Fprintf(cli.out, "Signing and pushing TUF metadata\n")

	repo, err := cli.getNotaryRepository(repoInfo, authConfig)
	if err != nil {
		fmt.Fprintf(cli.out, "Error establishing connection to notary repository: %s\n", err)
		return err
	}

	for _, target := range targets {
		hex, err := hex.DecodeString(target.digest.Hex())
		if err != nil {
			return err
		}
		t := &client.Target{
			Name: target.reference.String(),
			Hashes: data.Hashes{
				string(target.digest.Algorithm()): hex,
			},
			Length: int64(target.size),
		}
		if err := repo.AddTarget(t); err != nil {
			return err
		}
	}

	passFunc := func() (string, error) {
		passphrase, err := cli.getPassphrase()
		if err != nil {
			return "", err
		}
		return passphrase, nil
	}

	err = repo.Publish(passFunc)
	if _, ok := err.(*client.ErrRepoNotInitialized); !ok {
		return err
	}

	ks, err := keystoremanager.NewKeyStoreManager(cli.trustDirectory())
	if err != nil {
		return err
	}
	keys := ks.RootKeyStore().ListKeys()
	var rootKey string

	if len(keys) == 0 {
		fmt.Fprintf(cli.out, "Creating a new private key\n")
	} else {
		// TODO(dmcgowan): let user choose
		rootKey = keys[0]
		fmt.Fprintf(cli.out, "Using private key %q\n", rootKey)
	}
	passphrase, err := passFunc()
	if err != nil {
		return err
	}
	if rootKey == "" {
		rootKey, err = ks.GenRootKey("rsa", passphrase)
		if err != nil {
			return err
		}
	}
	cryptoService, err := ks.GetRootCryptoService(rootKey, passphrase)
	if err != nil {
		return err
	}

	passFunc = func() (string, error) {
		return passphrase, nil
	}

	if err := repo.Initialize(cryptoService); err != nil {
		return err
	}
	fmt.Fprintf(cli.out, "Finished initializing %q\n", repoInfo.CanonicalName)

	return repo.Publish(passFunc)

}
