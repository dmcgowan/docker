package image

import (
	"errors"
	"runtime"
	"strings"

	derrdefs "github.com/docker/docker/errdefs"
)

// CheckOS checks if the given OS matches the host's platform, and
// returns an error otherwise.
func CheckOS(os string) error {
	if !strings.EqualFold(runtime.GOOS, os) {
		return derrdefs.InvalidParameter(errors.New("operating system is not supported"))
	}
	return nil
}
