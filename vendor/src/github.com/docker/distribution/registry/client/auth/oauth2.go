package auth

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const OAuth2ConfigHeader = "OAuth-Config"

var ErrInvalidOAuth2Scheme = errors.New("invalid oauth2 authorization scheme")

type OAuth2Config struct {
	ClientID    string   `json:"client_id"`
	AuthURL     string   `json:"auth_url"`
	RedirectURL string   `json:"redirect_url,omitempty"`
	LandingURL  string   `json:"landing_url,omitempty"`
	Scopes      []string `json:"scopes,omitempty"`
}

func (c OAuth2Config) HeaderValue() (str string) {
	str = fmt.Sprintf("oauth2 client_id=%q,auth_url=%q", c.ClientID, c.AuthURL)
	if c.RedirectURL != "" {
		str = fmt.Sprintf("%s,redirect_url=%q", str, c.RedirectURL)
	}
	if len(c.Scopes) > 0 {
		str = fmt.Sprintf("%s,scopes=%q", str, strings.Join(c.Scopes, " "))
	}

	return
}

func GetOAuth2Config(client *http.Client, ch Challenge) (OAuth2Config, error) {
	if ch.Scheme != "bearer" {
		return OAuth2Config{}, ErrInvalidOAuth2Scheme
	}

	realm, ok := ch.Parameters["realm"]
	if !ok {
		return OAuth2Config{}, errors.New("no realm specified for token auth challenge")
	}

	resp, err := client.Head(realm)
	if err != nil {
		return OAuth2Config{}, err
	}
	defer resp.Body.Close()

	value, params := parseValueAndParams(resp.Header.Get(OAuth2ConfigHeader))
	if value != "oauth2" {
		return OAuth2Config{}, errors.New("missing oauth2 config header")
	}

	config := OAuth2Config{
		ClientID:    params["client_id"],
		AuthURL:     params["auth_url"],
		RedirectURL: params["redirect_url"],
	}
	if scopes := params["scopes"]; scopes != "" {
		config.Scopes = strings.Split(scopes, " ")
	}

	return config, nil
}

type callbackHandler struct {
	authURL *url.URL

	listener net.Listener

	codeChan chan string
	state    string
	redirect string
}

func NewOAuth2CallbackHandler(callbackURL, state, redirect string) (<-chan string, error) {
	authURL, err := url.Parse(callbackURL)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", authURL.Host)
	if err != nil {
		return nil, err
	}

	if redirect == "" {
		redirect = "https://docs.docker.com/registry/"
	}

	codeChan := make(chan string, 1)

	handler := &callbackHandler{
		authURL:  authURL,
		listener: listener,
		codeChan: codeChan,
		state:    state,
		redirect: redirect,
	}

	go http.Serve(listener, handler)

	return codeChan, nil
}

func (h *callbackHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if r.URL.Path != h.authURL.Path {
		http.Error(rw, "Unexpected Path", http.StatusNotFound)
		return
	}

	state := r.URL.Query().Get("state")
	if state != h.state {
		http.Error(rw, "Bad state", http.StatusBadRequest)
		return
	}

	code := r.URL.Query().Get("code")
	if code != "" {
		h.codeChan <- code
		go func() {
			time.Sleep(5 * time.Second)
			h.listener.Close()
		}()
	}
	http.Redirect(rw, r, h.redirect, http.StatusMovedPermanently)
}
