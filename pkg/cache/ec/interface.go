package ec

import (
	"io"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache"
)

// Interface is the single exported surface of the ec package. An EC
// tier is a cache.Tier (object-level Get/Fill implemented via
// Reed-Solomon encode/decode across shard peers), an io.Closer, and
// exposes per-peer stats for the Info gRPC service.
//
// Obtained via New.
type Interface interface {
	cache.Tier
	io.Closer

	// PeersStats returns per-peer counters in Maglev-sorted peer-ID
	// order. Used by the Info gRPC service to expose EC-tier peer
	// health alongside the cascade-view counters.
	PeersStats() []cache.PeerStats

	// ApplyMembership atomically swaps the cluster peer list; the new
	// membership's epoch must exceed the current one. Invoked from
	// the cache-ctl SIGHUP handler after re-reading the YAML config.
	ApplyMembership(m Membership) error

	// Epoch returns the current membership epoch (1 at startup, bumped
	// by every successful ApplyMembership).
	Epoch() int64
}

// Compile-time assertion that impl satisfies Interface.
var _ Interface = (*impl)(nil)
