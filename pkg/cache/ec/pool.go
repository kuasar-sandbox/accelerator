package ec

import (
	"sync"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/cache/client"
)

// peerPool manages a set of wire shard clients, one per peer endpoint.
// The stored type is client.ShardCloser so the pool owns the Close
// lifecycle as well as the cache.ShardTier contract.
type peerPool struct {
	mu       sync.Mutex
	clients  map[string]client.ShardCloser
	poolSize int
	timeout  time.Duration
	blobPool cache.BlobPool
}

// newPeerPool creates an empty pool. blobPool is injected into every
// peer client for payload allocation; pass nil to fall back to
// cache.DefaultPool.
func newPeerPool(poolSize int, timeout time.Duration, blobPool cache.BlobPool) *peerPool {
	return &peerPool{
		clients:  make(map[string]client.ShardCloser),
		poolSize: poolSize,
		timeout:  timeout,
		blobPool: blobPool,
	}
}

// Get returns a ShardCloser for the given peer, creating one if needed.
func (pp *peerPool) Get(endpoint string) (client.ShardCloser, error) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	if c, ok := pp.clients[endpoint]; ok {
		return c, nil
	}
	c, err := client.NewShard(endpoint, client.Options{
		Pool:     pp.poolSize,
		Timeout:  pp.timeout,
		BlobPool: pp.blobPool,
	})
	if err != nil {
		return nil, err
	}
	pp.clients[endpoint] = c
	return c, nil
}

// Close closes all clients in the pool.
func (pp *peerPool) Close() error {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	var firstErr error
	for k, c := range pp.clients {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(pp.clients, k)
	}
	return firstErr
}

// Reconcile closes clients whose endpoint is not in the new peer set.
// New peers aren't pre-dialled — they get lazy connections on the
// first Get/FillShard that hits them, same as the initial startup
// path. Returns the endpoints of closed clients for logging.
//
// Safe to call while Get/Fill are in flight: Reconcile takes the
// pool lock; an in-flight caller that already holds a ShardCloser
// reference from a prior Get() keeps using it until the wire RPC
// completes — Close on a dangling pool-owned client just invalidates
// the *next* Acquire attempt.
func (pp *peerPool) Reconcile(peers []Peer) []string {
	keep := make(map[string]struct{}, len(peers))
	for _, p := range peers {
		keep[p.Endpoint] = struct{}{}
	}
	pp.mu.Lock()
	defer pp.mu.Unlock()
	var closed []string
	for ep, c := range pp.clients {
		if _, ok := keep[ep]; ok {
			continue
		}
		_ = c.Close()
		delete(pp.clients, ep)
		closed = append(closed, ep)
	}
	return closed
}

// peerEntry pairs a peer's endpoint with the current ShardCloser (nil
// if the peer has never been dialled). Returned by Peers() for the EC
// tier's PeersStats aggregator.
type peerEntry struct {
	Endpoint string
	Client   client.ShardCloser
}

// Peers returns a snapshot of currently-dialled peers. Takes the pool
// lock briefly to copy (endpoint, ShardCloser) pairs, then releases.
// Pool state doesn't mutate after startup-time construction (peers are
// static), so callers can treat the returned slice as authoritative.
func (pp *peerPool) Peers() []peerEntry {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	out := make([]peerEntry, 0, len(pp.clients))
	for ep, c := range pp.clients {
		out = append(out, peerEntry{Endpoint: ep, Client: c})
	}
	return out
}
