package ipamutils

import (
	"net/netip"
	"testing"

	"gotest.tools/v3/assert"
)

// TestDeriveULABaseNetwork checks that for a given hostID, the derived prefix is stable over time.
func TestDeriveULABaseNetwork(t *testing.T) {
	testcases := []struct {
		name      string
		hostID    string
		expPrefix netip.Prefix
	}{
		{
			name:      "Empty hostID",
			expPrefix: netip.MustParsePrefix("fd42:98fc:1c14::/48"),
		},
		{
			name:      "499d4bc0-b0b3-416f-b1ee-cf6486315593",
			hostID:    "499d4bc0-b0b3-416f-b1ee-cf6486315593",
			expPrefix: netip.MustParsePrefix("fd62:fb69:18af::/48"),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			nw := DeriveULABaseNetwork(tc.hostID)
			assert.Equal(t, nw.Base, tc.expPrefix)
			assert.Equal(t, nw.Size, 64)
		})
	}
}
