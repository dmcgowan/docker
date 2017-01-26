package daemon

import (
	"fmt"
	"strings"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/api/errors"
)

func (d *Daemon) imageNotExistToErrcode(err error) error {
	if dne, isDNE := err.(ErrImageDoesNotExist); isDNE {
		if strings.Contains(dne.RefOrID, "@") {
			e := fmt.Errorf("No such image: %s", dne.RefOrID)
			return errors.NewRequestNotFoundError(e)
		}
		ref, err := reference.ParseNormalizedNamed(dne.RefOrID)
		if err != nil {
			e := fmt.Errorf("No such image: %s", dne.RefOrID)
			return errors.NewRequestNotFoundError(e)
		}
		e := fmt.Errorf("No such image: %s", reference.FamiliarString(reference.EnsureTagged(ref)))
		return errors.NewRequestNotFoundError(e)
	}
	return err
}

type errNotRunning struct {
	containerID string
}

func (e errNotRunning) Error() string {
	return fmt.Sprintf("Container %s is not running", e.containerID)
}

func (e errNotRunning) ContainerIsRunning() bool {
	return false
}

func errContainerIsRestarting(containerID string) error {
	err := fmt.Errorf("Container %s is restarting, wait until the container is running", containerID)
	return errors.NewRequestConflictError(err)
}

func errExecNotFound(id string) error {
	err := fmt.Errorf("No such exec instance '%s' found in daemon", id)
	return errors.NewRequestNotFoundError(err)
}

func errExecPaused(id string) error {
	err := fmt.Errorf("Container %s is paused, unpause the container before exec", id)
	return errors.NewRequestConflictError(err)
}
