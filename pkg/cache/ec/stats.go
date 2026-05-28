package ec

import (
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache/client"
)

// peerStater is the subset of client.ShardCloser that exposes per-peer
// stats. We type-assert rather than import the concrete type so the
// stats surface is decoupled from the wire-client internals.
type peerStater interface {
	Stats(id string) cache.PeerStats
}

// PeersStats returns per-peer counters in Maglev-sorted peer-ID order.
// The order is stable across calls (Router sorts IDs internally), so
// downstream consumers can index-compare snapshots over time.
//
// Peers that have never been dialled (no entry in the pool yet) are
// emitted with zero counters plus their Endpoint from the Router.
func (t *impl) PeersStats() []cache.PeerStats {
	peers := t.router.Peers()

	// Index currently-dialled peers by endpoint for O(1) lookup.
	dialled := make(map[string]client.ShardCloser, len(peers))
	for _, e := range t.pool.Peers() {
		dialled[e.Endpoint] = e.Client
	}

	out := make([]cache.PeerStats, len(peers))
	for i, p := range peers {
		if c, ok := dialled[p.Endpoint]; ok {
			if s, ok := c.(peerStater); ok {
				out[i] = s.Stats(p.ID)
				continue
			}
		}
		out[i] = cache.PeerStats{ID: p.ID, Endpoint: p.Endpoint}
	}
	return out
}
