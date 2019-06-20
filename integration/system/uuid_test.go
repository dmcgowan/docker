package system

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/versions"
	"gotest.tools/assert"
	"gotest.tools/skip"
)

func TestUUIDGeneration(t *testing.T) {
	skip.If(t, versions.LessThan(testEnv.DaemonAPIVersion(), "1.41"), "ID format changed")
	defer setupTest(t)()

	c := testEnv.APIClient()
	info, err := c.Info(context.Background())
	assert.NilError(t, err)

	assert.Assert(t, info.ID != "")
}
