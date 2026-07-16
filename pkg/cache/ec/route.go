package ec

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/kuasar-sandbox/accelerator/pkg/maglev"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// router maps shard keys to peers using Maglev consistent hashing.
//
// The router holds its Maglev table + peer set behind an atomic
// pointer so membership changes can swap them without locks. Callers
// take a consistent snapshot by calling Load once and reading all
// needed fields from it. Epoch is monotonically increasing;
// ApplyMembership rejects non-increasing updates to guard against
// config rollback.
type router struct {
	state atomic.Pointer[routerState]
}

// routerState is immutable after construction; the router publishes
// new states via atomic.Pointer swap, and readers obtain a consistent
// snapshot via Load. A stale snapshot stays valid as long as the
// reader holds the pointer — it just reflects the membership at the
// moment Load was called.
type routerState struct {
	epoch   int64
	peers   []Peer            // sorted by ID
	peerMap map[string]string // id → endpoint for O(1) Endpoint()
	table   *maglev.Table
}

// Peer identifies a cache-ctl shard node. Exported because it's the
// value type in Config.Cluster.Peers — external callers construct it
// when building the Config.
type Peer struct {
	ID       string `yaml:"id"`
	Endpoint string `yaml:"endpoint"`
}

// newRouter creates a Maglev lookup table from the given peers at
// epoch 1 (the initial membership).
func newRouter(peers []Peer) (*router, error) {
	s, err := buildRouterState(1, peers)
	if err != nil {
		return nil, err
	}
	r := &router{}
	r.state.Store(s)
	return r, nil
}

// buildRouterState constructs an immutable snapshot from (epoch, peers).
func buildRouterState(epoch int64, peers []Peer) (*routerState, error) {
	if len(peers) == 0 {
		return nil, fmt.Errorf("ec: no peers configured")
	}
	sorted := make([]Peer, len(peers))
	copy(sorted, peers)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	ids := make([]string, len(sorted))
	peerMap := make(map[string]string, len(sorted))
	for i, p := range sorted {
		ids[i] = p.ID
		peerMap[p.ID] = p.Endpoint
	}
	return &routerState{
		epoch:   epoch,
		peers:   sorted,
		peerMap: peerMap,
		table:   maglev.New(ids),
	}, nil
}

// LocateN returns the N distinct peers responsible for a given key.
// The key should include the chunk hash; shard index is NOT part of
// routing. Under the (idx, total) in-value design, shard_idx does not
// determine which peer gets a shard — any bijection from peer
// position to shard idx works, and the peer self-reports its held
// idx on Get.
func (r *router) LocateN(key store.ContentKey, n int) ([]string, error) {
	s := r.state.Load()
	return s.table.LocateN(key[:], n)
}

// LocatePeers returns the routed peer IDs and endpoints from one immutable
// router snapshot. Callers that retain the result for asynchronous work must
// use this method instead of resolving IDs through Endpoint later, after a
// membership swap may have published a different peer map.
func (r *router) LocatePeers(key store.ContentKey, n int) ([]Peer, error) {
	s := r.state.Load()
	ids, err := s.table.LocateN(key[:], n)
	if err != nil {
		return nil, err
	}
	peers := make([]Peer, len(ids))
	for i, id := range ids {
		endpoint, ok := s.peerMap[id]
		if !ok || endpoint == "" {
			return nil, fmt.Errorf("ec: routed peer %q has no endpoint", id)
		}
		peers[i] = Peer{ID: id, Endpoint: endpoint}
	}
	return peers, nil
}

// Endpoint returns the gRPC endpoint for the given peer ID, or empty
// string if the peer is not in the current membership.
func (r *router) Endpoint(peerID string) string {
	return r.state.Load().peerMap[peerID]
}

// Peers returns a stable snapshot of all peers in the current
// membership, sorted by peer ID. Used by stats aggregators that need
// a predictable list of peers regardless of which subset the pool
// has dialled.
func (r *router) Peers() []Peer {
	s := r.state.Load()
	out := make([]Peer, len(s.peers))
	copy(out, s.peers)
	return out
}

// Epoch returns the current membership epoch.
func (r *router) Epoch() int64 {
	return r.state.Load().epoch
}

// ApplyMembership atomically swaps in a new membership at the given
// epoch. Rejects non-monotonic updates. On success, Endpoint/LocateN
// start returning results based on the new peer set immediately for
// any subsequent call; in-flight callers that loaded the old state
// before the swap continue on the old snapshot until they return —
// no locking or barrier needed.
//
// Callers should probe new peers for liveness before invoking
// ApplyMembership; the router does not perform health checks.
func (r *router) ApplyMembership(epoch int64, peers []Peer) (*routerState, *routerState, error) {
	cur := r.state.Load()
	if epoch <= cur.epoch {
		return nil, nil, fmt.Errorf("ec: rejecting epoch %d (current %d); epochs must be monotonically increasing", epoch, cur.epoch)
	}
	next, err := buildRouterState(epoch, peers)
	if err != nil {
		return nil, nil, err
	}
	r.state.Store(next)
	return cur, next, nil
}
