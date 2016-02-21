package client

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	Cli "github.com/docker/docker/cli"
	flag "github.com/docker/docker/pkg/mflag"
	"github.com/docker/docker/pkg/term"
	"github.com/docker/engine-api/client"
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

	flUser := cmd.String([]string{"u", "-username"}, "", "Username")
	flPassword := cmd.String([]string{"p", "-password"}, "", "Password")

	// Deprecated in 1.11: Should be removed in docker 1.13
	cmd.String([]string{"#e", "#-email"}, "", "Email")

	cmd.ParseFlags(args, true)

	// On Windows, force the use of the regular OS stdin stream. Fixes #14336/#14210
	if runtime.GOOS == "windows" {
		cli.in = os.Stdin
	}

	var serverAddress string
	var isDefaultRegistry bool
	if len(cmd.Args()) > 0 {
		serverAddress = cmd.Arg(0)
	} else {
		serverAddress = cli.electAuthServer()
		isDefaultRegistry = true
	}

	authConfig, err := cli.configureAuth(*flUser, *flPassword, serverAddress, isDefaultRegistry)
	if err != nil {
		return err
	}

	response, err := cli.client.RegistryLogin(authConfig)
	if err != nil {
		if client.IsErrUnauthorized(err) {
			delete(cli.configFile.AuthConfigs, serverAddress)
			if err2 := cli.configFile.Save(); err2 != nil {
				fmt.Fprintf(cli.out, "WARNING: could not save config file: %v\n", err2)
			}
		}
		return err
	}

	if err := cli.configFile.Save(); err != nil {
		return fmt.Errorf("Error saving config file: %v", err)
	}
	fmt.Fprintf(cli.out, "WARNING: login credentials saved in %s\n", cli.configFile.Filename())

	if response.Status != "" {
		fmt.Fprintf(cli.out, "%s\n", response.Status)
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
	authconfig, ok := cli.configFile.AuthConfigs[serverAddress]
	if !ok {
		authconfig = types.AuthConfig{}
	}
	authconfig.Username = strings.TrimSpace(authconfig.Username)

	if flUser = strings.TrimSpace(flUser); flUser == "" {
		if isDefaultRegistry {
			// if this is a defauly registry (docker hub), then display the following message.
			msg := "Login with your Docker ID to push and pull images from Docker Hub. If you don't have a Docker ID, head over to https://hub.docker.com to create one."
			fmt.Fprintf(cli.out, "%s\n", msg)
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
	cli.configFile.AuthConfigs[serverAddress] = authconfig
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
