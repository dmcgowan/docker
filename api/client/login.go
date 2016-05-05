package client

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"

	"github.com/docker/distribution/registry/client/auth"
	Cli "github.com/docker/docker/cli"
	"github.com/docker/docker/cliconfig"
	"github.com/docker/docker/cliconfig/credentials"
	flag "github.com/docker/docker/pkg/mflag"
	"github.com/docker/docker/pkg/term"
	"github.com/docker/engine-api/types"
)

// CmdLogin logs in a user to a Docker registry service.
//
// If no server is specified, the user will be logged into or registered to the registry's index server.
//
// Usage: docker login SERVER
func (cli *DockerCli) CmdLogin(args ...string) error {
	cmd := Cli.Subcmd("login", []string{"[SERVER]"}, Cli.DockerCommands["login"].Description+".\nIf no server is specified, the default is defined by the daemon.", true)
	cmd.Require(flag.Max, 1)

	flOAuth2 := cmd.Bool([]string{"-oauth"}, false, "Use Oauth2 login flow")
	flUser := cmd.String([]string{"u", "-username"}, "", "Username")
	flPassword := cmd.String([]string{"p", "-password"}, "", "Password")

	// Deprecated in 1.11: Should be removed in docker 1.13
	cmd.String([]string{"#e", "#-email"}, "", "Email")

	cmd.ParseFlags(args, true)

	// On Windows, force the use of the regular OS stdin stream. Fixes #14336/#14210
	if runtime.GOOS == "windows" {
		cli.in = os.Stdin
	}

	var (
		serverAddress     string
		isDefaultRegistry bool
		authConfig        types.AuthConfig
		err               error
	)
	if len(cmd.Args()) > 0 {
		serverAddress = cmd.Arg(0)
	} else {
		serverAddress = cli.electAuthServer()
		isDefaultRegistry = true
	}

	if *flOAuth2 {
		authConfig.ServerAddress = serverAddress
		authConfig.RegistryToken = "oauth2"

		response, err := cli.client.RegistryLogin(context.Background(), authConfig)
		if err != nil {
			return err
		}

		if response.IdentityToken == "" {
			return fmt.Errorf("Unable to complete oauth2 login: %v", response.Status)
		}

		b, err := base64.StdEncoding.DecodeString(response.IdentityToken)
		if err != nil {
			return fmt.Errorf("Bad oauth2 configuration from server: %v", err)
		}
		var oauth2Config auth.OAuth2Config
		if err := json.Unmarshal(b, &oauth2Config); err != nil {
			return fmt.Errorf("Bad oauth2 configuration from server: %v", err)
		}

		config := &oauth2.Config{
			ClientID: oauth2Config.ClientID,
			Endpoint: oauth2.Endpoint{
				AuthURL: oauth2Config.AuthURL,
			},
			RedirectURL: oauth2Config.RedirectURL,
			Scopes:      oauth2Config.Scopes,
		}

		state := "generatedrandomstate...."
		codeURL := config.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce)

		fmt.Fprintln(cli.out, "Login on browser and return")

		// TODO: Make handler configurable

		codeChan, err := auth.NewOAuth2CallbackHandler(config.RedirectURL, state, oauth2Config.LandingURL)
		if err != nil {
			return fmt.Errorf("Error setting up oauth2 callback: %v", err)
		}

		cmd := exec.Command("xdg-open", codeURL)
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(cli.out, "Error opening url: %v\n", err)
			fmt.Fprintf(cli.out, "Open URL in webrowser on localhost: %q", codeURL)
		}

		// TODO: Timeout waiting
		code := <-codeChan

		authConfig.RegistryToken = fmt.Sprintf("%s %s", code, config.RedirectURL)
	} else {
		authConfig, err = cli.configureAuth(*flUser, *flPassword, serverAddress, isDefaultRegistry)
		if err != nil {
			return err
		}
	}

	response, err := cli.client.RegistryLogin(context.Background(), authConfig)
	if err != nil {
		return err
	}

	if response.IdentityToken != "" {
		authConfig.Username = "Confidential"
		authConfig.Password = ""
		authConfig.IdentityToken = response.IdentityToken
		authConfig.RegistryToken = ""
	}
	if err := storeCredentials(cli.configFile, authConfig); err != nil {
		return fmt.Errorf("Error saving credentials: %v", err)
	}

	if response.Status != "" {
		fmt.Fprintln(cli.out, response.Status)
	}
	return nil
}

func (cli *DockerCli) promptWithDefault(prompt string, configDefault string) {
	if configDefault == "" {
		fmt.Fprintf(cli.out, "%s: ", prompt)
	} else {
		fmt.Fprintf(cli.out, "%s (%s): ", prompt, configDefault)
	}
}

func (cli *DockerCli) configureAuth(flUser, flPassword, serverAddress string, isDefaultRegistry bool) (types.AuthConfig, error) {
	authconfig, err := getCredentials(cli.configFile, serverAddress)
	if err != nil {
		return authconfig, err
	}

	authconfig.Username = strings.TrimSpace(authconfig.Username)

	if flUser = strings.TrimSpace(flUser); flUser == "" {
		if isDefaultRegistry {
			// if this is a defauly registry (docker hub), then display the following message.
			fmt.Fprintln(cli.out, "Login with your Docker ID to push and pull images from Docker Hub. If you don't have a Docker ID, head over to https://hub.docker.com to create one.")
		}
		cli.promptWithDefault("Username", authconfig.Username)
		flUser = readInput(cli.in, cli.out)
		flUser = strings.TrimSpace(flUser)
		if flUser == "" {
			flUser = authconfig.Username
		}
	}

	if flUser == "" {
		return authconfig, fmt.Errorf("Error: Non-null Username Required")
	}

	if flPassword == "" {
		oldState, err := term.SaveState(cli.inFd)
		if err != nil {
			return authconfig, err
		}
		fmt.Fprintf(cli.out, "Password: ")
		term.DisableEcho(cli.inFd, oldState)

		flPassword = readInput(cli.in, cli.out)
		fmt.Fprint(cli.out, "\n")

		term.RestoreTerminal(cli.inFd, oldState)
		if flPassword == "" {
			return authconfig, fmt.Errorf("Error: Password Required")
		}
	}

	authconfig.Username = flUser
	authconfig.Password = flPassword
	authconfig.ServerAddress = serverAddress
	authconfig.IdentityToken = ""

	return authconfig, nil
}

func readInput(in io.Reader, out io.Writer) string {
	reader := bufio.NewReader(in)
	line, _, err := reader.ReadLine()
	if err != nil {
		fmt.Fprintln(out, err.Error())
		os.Exit(1)
	}
	return string(line)
}

// getCredentials loads the user credentials from a credentials store.
// The store is determined by the config file settings.
func getCredentials(c *cliconfig.ConfigFile, serverAddress string) (types.AuthConfig, error) {
	s := loadCredentialsStore(c)
	return s.Get(serverAddress)
}

func getAllCredentials(c *cliconfig.ConfigFile) (map[string]types.AuthConfig, error) {
	s := loadCredentialsStore(c)
	return s.GetAll()
}

// storeCredentials saves the user credentials in a credentials store.
// The store is determined by the config file settings.
func storeCredentials(c *cliconfig.ConfigFile, auth types.AuthConfig) error {
	s := loadCredentialsStore(c)
	return s.Store(auth)
}

// eraseCredentials removes the user credentials from a credentials store.
// The store is determined by the config file settings.
func eraseCredentials(c *cliconfig.ConfigFile, serverAddress string) error {
	s := loadCredentialsStore(c)
	return s.Erase(serverAddress)
}

// loadCredentialsStore initializes a new credentials store based
// in the settings provided in the configuration file.
func loadCredentialsStore(c *cliconfig.ConfigFile) credentials.Store {
	if c.CredentialsStore != "" {
		return credentials.NewNativeStore(c)
	}
	return credentials.NewFileStore(c)
}
