package registry

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/registry/client/auth"
	"github.com/docker/docker/cliconfig"
	"github.com/docker/docker/pkg/tlsconfig"
)

type Service struct {
	Config *ServiceConfig
}

// NewService returns a new instance of Service ready to be
// installed no an engine.
func NewService(options *Options) *Service {
	return &Service{
		Config: NewServiceConfig(options),
	}
}

// Auth contacts the public registry with the provided credentials,
// and returns OK if authentication was sucessful.
// It can be used to verify the validity of a client's credentials.
func (s *Service) Auth(authConfig *cliconfig.AuthConfig) (string, error) {
	addr := authConfig.ServerAddress
	if addr == "" {
		// Use the official registry address if not specified.
		addr = INDEXSERVER
	}
	index, err := s.ResolveIndex(addr)
	if err != nil {
		return "", err
	}
	endpoint, err := NewEndpoint(index, nil)
	if err != nil {
		return "", err
	}
	authConfig.ServerAddress = endpoint.String()
	return Login(authConfig, endpoint)
}

// Search queries the public registry for images matching the specified
// search terms, and returns the results.
func (s *Service) Search(term string, authConfig *cliconfig.AuthConfig, headers map[string][]string) (*SearchResults, error) {
	repoInfo, err := s.ResolveRepository(term)
	if err != nil {
		return nil, err
	}

	// *TODO: Search multiple indexes.
	endpoint, err := repoInfo.GetEndpoint(http.Header(headers))
	if err != nil {
		return nil, err
	}
	r, err := NewSession(endpoint.client, authConfig, endpoint)
	if err != nil {
		return nil, err
	}
	return r.SearchRepositories(repoInfo.GetSearchTerm())
}

// ResolveRepository splits a repository name into its components
// and configuration of the associated registry.
func (s *Service) ResolveRepository(name string) (*RepositoryInfo, error) {
	return s.Config.NewRepositoryInfo(name)
}

// ResolveIndex takes indexName and returns index info
func (s *Service) ResolveIndex(name string) (*IndexInfo, error) {
	return s.Config.NewIndexInfo(name)
}

type APIEndpoint struct {
	Mirror        bool
	URL           string
	Version       APIVersion
	Official      bool
	TrimHostname  bool
	TLSConfig     *tls.Config
	VersionHeader string
	Versions      []auth.APIVersion
}

func (e APIEndpoint) ToV1Endpoint(metaHeaders http.Header) (*Endpoint, error) {
	return newEndpoint(e.URL, e.TLSConfig, metaHeaders)
}

func (s *Service) TlsConfig(hostname string) (*tls.Config, error) {
	// we construct a client tls config from server defaults
	// PreferredServerCipherSuites should have no effect
	tlsConfig := tlsconfig.ServerDefault

	isSecure := s.Config.isSecureIndex(hostname)

	tlsConfig.InsecureSkipVerify = !isSecure

	if isSecure {
		hostDir := filepath.Join(CERTS_DIR, hostname)
		logrus.Debugf("hostDir: %s", hostDir)
		if err := ReadCertsDirectory(&tlsConfig, hostDir); err != nil {
			return nil, err
		}
	}

	return &tlsConfig, nil
}

func (s *Service) tlsConfigForMirror(mirror string) (*tls.Config, error) {
	mirrorUrl, err := url.Parse(mirror)
	if err != nil {
		return nil, err
	}
	return s.TlsConfig(mirrorUrl.Host)
}

func (s *Service) LookupEndpoints(repoName string) (endpoints []APIEndpoint, err error) {
	var cfg = tlsconfig.ServerDefault
	tlsConfig := &cfg
	if strings.HasPrefix(repoName, DEFAULT_NAMESPACE+"/") {
		// v2 mirrors
		for _, mirror := range s.Config.Mirrors {
			mirrorTlsConfig, err := s.tlsConfigForMirror(mirror)
			if err != nil {
				return nil, err
			}
			endpoints = append(endpoints, APIEndpoint{
				URL: mirror,
				// guess mirrors are v2
				Version:      APIVersion2,
				Mirror:       true,
				TrimHostname: true,
				TLSConfig:    mirrorTlsConfig,
			})
		}
		// v2 registry
		endpoints = append(endpoints, APIEndpoint{
			URL:          DEFAULT_V2_REGISTRY,
			Version:      APIVersion2,
			Official:     true,
			TrimHostname: true,
			TLSConfig:    tlsConfig,
		})
		// v1 registry
		endpoints = append(endpoints, APIEndpoint{
			URL:          DEFAULT_V1_REGISTRY,
			Version:      APIVersion1,
			Official:     true,
			TrimHostname: true,
			TLSConfig:    tlsConfig,
		})
		return endpoints, nil
	}

	slashIndex := strings.IndexRune(repoName, '/')
	if slashIndex <= 0 {
		return nil, fmt.Errorf("invalid repo name: missing '/':  %s", repoName)
	}
	hostname := repoName[:slashIndex]

	tlsConfig, err = s.TlsConfig(hostname)
	if err != nil {
		return nil, err
	}
	isSecure := !tlsConfig.InsecureSkipVerify

	v2Versions := []auth.APIVersion{
		{
			Type:    "registry",
			Version: "2.0",
		},
	}
	endpoints = []APIEndpoint{
		{
			URL:           "https://" + hostname,
			Version:       APIVersion2,
			TrimHostname:  true,
			TLSConfig:     tlsConfig,
			VersionHeader: DEFAULT_REGISTRY_VERSION_HEADER,
			Versions:      v2Versions,
		},
		{
			URL:          "https://" + hostname,
			Version:      APIVersion1,
			TrimHostname: true,
			TLSConfig:    tlsConfig,
		},
	}

	if !isSecure {
		endpoints = append(endpoints, APIEndpoint{
			URL:          "http://" + hostname,
			Version:      APIVersion2,
			TrimHostname: true,
			// used to check if supposed to be secure via InsecureSkipVerify
			TLSConfig:     tlsConfig,
			VersionHeader: DEFAULT_REGISTRY_VERSION_HEADER,
			Versions:      v2Versions,
		}, APIEndpoint{
			URL:          "http://" + hostname,
			Version:      APIVersion1,
			TrimHostname: true,
			// used to check if supposed to be secure via InsecureSkipVerify
			TLSConfig: tlsConfig,
		})
	}

	return endpoints, nil
}
