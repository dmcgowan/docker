// Package ipamutils provides utility functions for ipam management
package ipamutils

import (
	"crypto/sha256"
	"encoding/binary"
	"net/netip"
	"slices"

	"github.com/moby/moby/v2/daemon/libnetwork/ipbits"
)

var (
	localScopeDefaultNetworks = []*NetworkToSplit{
		{netip.MustParsePrefix("172.17.0.0/16"), 16},
		{netip.MustParsePrefix("172.18.0.0/16"), 16},
		{netip.MustParsePrefix("172.19.0.0/16"), 16},
		{netip.MustParsePrefix("172.20.0.0/14"), 16},
		{netip.MustParsePrefix("172.24.0.0/14"), 16},
		{netip.MustParsePrefix("172.28.0.0/14"), 16},
		{netip.MustParsePrefix("192.168.0.0/16"), 20},
	}
	globalScopeDefaultNetworks = []*NetworkToSplit{
		{netip.MustParsePrefix("10.0.0.0/8"), 24},
	}
)

// NetworkToSplit represent a network that has to be split in chunks with mask length Size.
// Each subnet in the set is derived from the Base pool. Base is to be passed
// in CIDR format.
// Example: a Base "10.10.0.0/16 with Size 24 will define the set of 256
// 10.10.[0-255].0/24 address pools
type NetworkToSplit struct {
	Base netip.Prefix `json:"base"`
	Size int          `json:"size"`
}

// FirstPrefix returns the first prefix available in NetworkToSplit.
func (n NetworkToSplit) FirstPrefix() netip.Prefix {
	return netip.PrefixFrom(n.Base.Addr(), n.Size)
}

// Overlaps is a util function checking whether 'p' overlaps with 'n'.
func (n NetworkToSplit) Overlaps(p netip.Prefix) bool {
	return n.Base.Overlaps(p)
}

// GetGlobalScopeDefaultNetworks returns a copy of the global-scope network list.
func GetGlobalScopeDefaultNetworks() []*NetworkToSplit {
	return slices.Clone(globalScopeDefaultNetworks)
}

// GetLocalScopeDefaultNetworks returns a copy of the default local-scope network list.
func GetLocalScopeDefaultNetworks() []*NetworkToSplit {
	return slices.Clone(localScopeDefaultNetworks)
}

// DeriveULABaseNetwork derives a Global ID from the provided hostID and
// appends it to the ULA prefix (with L bit set) to generate a ULA prefix
// unique to this host. The returned NetworkToSplit is stable over time if
// hostID doesn't change.
//
// This is loosely based on the algorithm described in https://datatracker.ietf.org/doc/html/rfc4193#section-3.2.2.
func DeriveULABaseNetwork(hostID string) *NetworkToSplit {
	sha := sha256.Sum256([]byte(hostID))
	gid := binary.BigEndian.Uint64(sha[:]) & (1<<40 - 1) // Keep the 40 least significant bits.
	addr := ipbits.Add(netip.MustParseAddr("fd00::"), gid, 80)

	return &NetworkToSplit{
		Base: netip.PrefixFrom(addr, 48),
		Size: 64,
	}
}
