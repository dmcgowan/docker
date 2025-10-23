package volumebackend

import (
	"github.com/moby/moby/api/types/volume"
	"github.com/moby/moby/v2/daemon/server/filters"
)

type ListOptions struct {
	Filters filters.Args
}

type UpdateOptions struct {
	Spec *volume.ClusterVolumeSpec `json:"Spec,omitempty"`
}
