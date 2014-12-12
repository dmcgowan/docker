package registry

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"

	log "github.com/Sirupsen/logrus"
)

// for mocking in unit tests
var lookupIP = net.LookupIP

// scans string for api version in the URL path. returns the trimmed address, if version found, string and API version.
func scanForAPIVersion(address string) (string, APIVersion) {
	var (
		chunks        []string
		apiVersionStr string
	)

	if strings.HasSuffix(address, "/") {
		address = address[:len(address)-1]
	}

	chunks = strings.Split(address, "/")
	apiVersionStr = chunks[len(chunks)-1]

	for k, v := range apiVersions {
		if apiVersionStr == v {
			address = strings.Join(chunks[:len(chunks)-1], "/")
			return address, k
		}
	}

	return address, APIVersionUnknown
}

// NewEndpoint parses the given address to return a registry endpoint.
func NewEndpoint(address string, insecureRegistries []string) (endpoint *Endpoint, err error) {
	if endpoint, err = newEndpoint(address, insecureRegistries); err != nil {
		log.Debugf("unable to get new registry endpoint for address %q: %s", address, err)
		return nil, err
	}

	log.Debugf("pinging registry endpoint %s", endpoint)

	// Try HTTPS ping to registry
	endpoint.URL.Scheme = "https"
	if _, err = endpoint.Ping(); err != nil {
		if endpoint.IsSecure {
			// If registry is secure and HTTPS failed, show user the error and tell them about `--insecure-registry`
			// in case that's what they need. DO NOT accept unknown CA certificates, and DO NOT fallback to HTTP.
			return nil, fmt.Errorf("invalid registry endpoint %s: %v. If this private registry supports only HTTP or HTTPS with an unknown CA certificate, please add `--insecure-registry %s` to the daemon's arguments. In the case of HTTPS, if you have access to the registry's CA certificate, no need for the flag; simply place the CA certificate at /etc/docker/certs.d/%s/ca.crt", endpoint, err, endpoint.URL.Host, endpoint.URL.Host)
		}

		// If registry is insecure and HTTPS failed, fallback to HTTP.
		log.Debugf("Error from registry %q marked as insecure: %v. Insecurely falling back to HTTP", endpoint, err)
		endpoint.URL.Scheme = "http"

		var err2 error
		if _, err2 = endpoint.Ping(); err2 == nil {
			return endpoint, nil
		}

		return nil, fmt.Errorf("invalid registry endpoint %q. HTTPS attempt: %v. HTTP attempt: %v", endpoint, err, err2)
	}

	return endpoint, nil
}

func newEndpoint(address string, insecureRegistries []string) (*Endpoint, error) {
	var (
		endpoint       = new(Endpoint)
		trimmedAddress string
		err            error
	)

	if !strings.HasPrefix(address, "http") {
		address = "https://" + address
	}

	trimmedAddress, endpoint.Version = scanForAPIVersion(address)

	if endpoint.URL, err = url.Parse(trimmedAddress); err != nil {
		return nil, err
	}

	if endpoint.IsSecure, err = isSecure(endpoint.URL.Host, insecureRegistries); err != nil {
		return nil, err
	}

	return endpoint, nil
}

// Endpoint stores basic information about a registry endpoint.
type Endpoint struct {
	URL            *url.URL
	Version        APIVersion
	IsSecure       bool
	AuthChallenges []*AuthorizationChallenge
}

// Get the formated URL for the root of this registry Endpoint
func (e *Endpoint) String() string {
	return fmt.Sprintf("%s/v%d/", e.URL, e.Version)
}

// VersionString returns a formatted string of this
// endpoint address using the given API Version.
func (e *Endpoint) VersionString(version APIVersion) string {
	return fmt.Sprintf("%s/v%d/", e.URL, version)
}

// Path returns a formatted string for the URL
// of this endpoint with the given path appended.
func (e *Endpoint) Path(path string) string {
	return fmt.Sprintf("%s/v%d/%s", e.URL, e.Version, path)
}

func (e *Endpoint) Ping() (RegistryInfo, error) {
	// The ping logic to use is determined by the registry endpoint version.
	switch e.Version {
	case APIVersion1:
		return e.pingV1()
	case APIVersion2:
		return e.pingV2()
	}

	// APIVersionUnknown
	// We should try v2 first...
	e.Version = APIVersion2
	regInfo, errV2 := e.pingV2()
	if errV2 == nil {
		return regInfo, nil
	}

	// ... then fallback to v1.
	e.Version = APIVersion1
	regInfo, errV1 := e.pingV1()
	if errV1 == nil {
		return regInfo, nil
	}

	e.Version = APIVersionUnknown
	return RegistryInfo{}, fmt.Errorf("unable to ping registry endpoint %s\nv2 ping attempt failed with error: %s\n v1 ping attempt failed with error: %s", e, errV2, errV1)
}

func (e *Endpoint) pingV1() (RegistryInfo, error) {
	log.Debugf("attempting v1 ping for registry endpoint %s", e)

	if e.String() == IndexServerAddress() {
		// Skip the check, we know this one is valid
		// (and we never want to fallback to http in case of error)
		return RegistryInfo{Standalone: false}, nil
	}

	req, err := http.NewRequest("GET", e.Path("_ping"), nil)
	if err != nil {
		return RegistryInfo{Standalone: false}, err
	}

	resp, _, err := doRequest(req, nil, ConnectTimeout, e.IsSecure)
	if err != nil {
		return RegistryInfo{Standalone: false}, err
	}

	defer resp.Body.Close()

	jsonString, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return RegistryInfo{Standalone: false}, fmt.Errorf("error while reading the http response: %s", err)
	}

	// If the header is absent, we assume true for compatibility with earlier
	// versions of the registry. default to true
	info := RegistryInfo{
		Standalone: true,
	}
	if err := json.Unmarshal(jsonString, &info); err != nil {
		log.Debugf("Error unmarshalling the _ping RegistryInfo: %s", err)
		// don't stop here. Just assume sane defaults
	}
	if hdr := resp.Header.Get("X-Docker-Registry-Version"); hdr != "" {
		log.Debugf("Registry version header: '%s'", hdr)
		info.Version = hdr
	}
	log.Debugf("RegistryInfo.Version: %q", info.Version)

	standalone := resp.Header.Get("X-Docker-Registry-Standalone")
	log.Debugf("Registry standalone header: '%s'", standalone)
	// Accepted values are "true" (case-insensitive) and "1".
	if strings.EqualFold(standalone, "true") || standalone == "1" {
		info.Standalone = true
	} else if len(standalone) > 0 {
		// there is a header set, and it is not "true" or "1", so assume fails
		info.Standalone = false
	}
	log.Debugf("RegistryInfo.Standalone: %t", info.Standalone)
	return info, nil
}

func (e *Endpoint) pingV2() (RegistryInfo, error) {
	log.Debugf("attempting v2 ping for registry endpoint %s", e)

	req, err := http.NewRequest("GET", e.Path(""), nil)
	if err != nil {
		return RegistryInfo{}, err
	}

	resp, _, err := doRequest(req, nil, ConnectTimeout, e.IsSecure)
	if err != nil {
		return RegistryInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// It would seem that no authentication/authorization is required.
		// So we don't need to parse/add any authorization schemes.
		return RegistryInfo{Standalone: true}, nil
	}

	if resp.StatusCode == http.StatusUnauthorized {
		// Parse the WWW-Authenticate Header and store the challenges
		// on this endpoint object.
		e.AuthChallenges = parseAuthHeader(resp.Header)
		return RegistryInfo{}, nil
	}

	return RegistryInfo{}, fmt.Errorf("v2 registry endpoint returned status %d: %q", resp.StatusCode, http.StatusText(resp.StatusCode))
}

// isSecure returns false if the provided hostname is part of the list of insecure registries.
// Insecure registries accept HTTP and/or accept HTTPS with certificates from unknown CAs.
//
// The list of insecure registries can contain an element with CIDR notation to specify a whole subnet.
// If the subnet contains one of the IPs of the registry specified by hostname, the latter is considered
// insecure.
//
// hostname should be a URL.Host (`host:port` or `host`) where the `host` part can be either a domain name
// or an IP address. If it is a domain name, then it will be resolved in order to check if the IP is contained
// in a subnet. If the resolving is not successful, isSecure will only try to match hostname to any element
// of insecureRegistries.
func isSecure(hostname string, insecureRegistries []string) (bool, error) {
	if hostname == IndexServerURL.Host {
		return true, nil
	}

	host, _, err := net.SplitHostPort(hostname)
	if err != nil {
		// assume hostname is of the form `host` without the port and go on.
		host = hostname
	}
	addrs, err := lookupIP(host)
	if err != nil {
		ip := net.ParseIP(host)
		if ip != nil {
			addrs = []net.IP{ip}
		}

		// if ip == nil, then `host` is neither an IP nor it could be looked up,
		// either because the index is unreachable, or because the index is behind an HTTP proxy.
		// So, len(addrs) == 0 and we're not aborting.
	}

	for _, r := range insecureRegistries {
		if hostname == r || host == r {
			// hostname matches insecure registry
			return false, nil
		}

		// Try CIDR notation only if addrs has any elements, i.e. if `host`'s IP could be determined.
		for _, addr := range addrs {

			// now assume a CIDR was passed to --insecure-registry
			_, ipnet, err := net.ParseCIDR(r)
			if err != nil {
				// if we could not parse it as a CIDR, even after removing
				// assume it's not a CIDR and go on with the next candidate
				break
			}

			// check if the addr falls in the subnet
			if ipnet.Contains(addr) {
				return false, nil
			}
		}
	}

	return true, nil
}
