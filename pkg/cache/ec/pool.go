package ec

import (
	"net"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
)

// peerPool manages a set of wire shard clients, one per peer endpoint.
// The stored type is client.ShardCloser so the pool owns the Close
// lifecycle as well as the cache.ShardTier contract.
type peerPool struct {
	mu        sync.Mutex
	clients   map[string]client.ShardCloser
	starting  map[string]*peerInit
	closed    bool
	closeErr  error
	closeOnce sync.Once
	work      sync.WaitGroup // initialization and clients retired by Reconcile
	poolSize  int
	timeout   time.Duration
	blobPool  cache.BlobPool
	newShard  func(string, client.Options) (client.ShardCloser, error)
}

// peerInit identifies one initialization, including across removal and re-addition
// of the same endpoint. Waiters look up the published client after done closes;
// a failed initializer does not supply their own connection attempt's result.
type peerInit struct {
	done chan struct{}
}

// newPeerPool creates an empty pool. blobPool is injected into every
// peer client for payload allocation; pass nil to fall back to
// cache.DefaultPool.
func newPeerPool(poolSize int, timeout time.Duration, blobPool cache.BlobPool) *peerPool {
	return &peerPool{
		clients:  make(map[string]client.ShardCloser),
		starting: make(map[string]*peerInit),
		poolSize: poolSize,
		timeout:  timeout,
		blobPool: blobPool,
		newShard: client.NewShard,
	}
}

// Get returns a ShardCloser for the given peer, creating one if needed. Only
// callers initializing the same endpoint wait for one another; neither dialing
// nor closing a discarded client holds the pool-wide lookup lock.
func (pp *peerPool) Get(endpoint string) (client.ShardCloser, error) {
	for {
		pp.mu.Lock()
		if pp.closed {
			pp.mu.Unlock()
			return nil, net.ErrClosed
		}
		if c, ok := pp.clients[endpoint]; ok {
			pp.mu.Unlock()
			return c, nil
		}
		if pending := pp.starting[endpoint]; pending != nil {
			pp.mu.Unlock()
			<-pending.done
			continue
		}
		pending := &peerInit{done: make(chan struct{})}
		pp.starting[endpoint] = pending
		pp.work.Add(1)
		pp.mu.Unlock()

		c, err := pp.newShard(endpoint, client.Options{
			Pool:     pp.poolSize,
			Timeout:  pp.timeout,
			BlobPool: pp.blobPool,
		})

		pp.mu.Lock()
		current := pp.starting[endpoint] == pending
		if current {
			delete(pp.starting, endpoint)
		}
		closing := pp.closed
		if closing || !current {
			// Close or Reconcile retired this initialization. In particular,
			// it must not overwrite a new client for a re-added endpoint.
			err = net.ErrClosed
		}
		if err == nil {
			pp.clients[endpoint] = c
		}
		pp.mu.Unlock()

		if err != nil && c != nil {
			closeErr := c.Close()
			if closing {
				pp.recordCloseError(closeErr)
			}
			c = nil
		}
		close(pending.done)
		pp.work.Done()
		return c, err
	}
}

// Close prevents new initialization/publication and joins construction and
// cleanup already in progress. NewShard retains its existing dial timeout.
func (pp *peerPool) Close() error {
	pp.closeOnce.Do(func() {
		pp.mu.Lock()
		pp.closed = true
		clients := pp.clients
		pp.clients = make(map[string]client.ShardCloser)
		clear(pp.starting)
		pp.mu.Unlock()

		for _, c := range clients {
			pp.recordCloseError(c.Close())
		}
		pp.work.Wait()
	})
	return pp.closeErr
}

func (pp *peerPool) recordCloseError(err error) {
	if err == nil {
		return
	}
	pp.mu.Lock()
	if pp.closeErr == nil {
		pp.closeErr = err
	}
	pp.mu.Unlock()
}

// Reconcile retires clients and initializations outside the new peer set.
// New peers aren't pre-dialled — they get lazy connections on the
// first Get/FillShard that hits them, same as the initial startup
// path. Returns the retired endpoints for logging.
//
// Removal is serialized with publication; actual Close calls run outside the
// lookup lock. An in-flight caller that already holds a ShardCloser
// reference from a prior Get() keeps using it until the wire RPC
// completes — Close on a dangling pool-owned client just invalidates
// the *next* Acquire attempt. As before, a later Get using an older routing
// snapshot may initialize that endpoint again; this is not a routing filter.
func (pp *peerPool) Reconcile(peers []Peer) []string {
	keep := make(map[string]struct{}, len(peers))
	for _, p := range peers {
		keep[p.Endpoint] = struct{}{}
	}
	pp.mu.Lock()
	if pp.closed {
		pp.mu.Unlock()
		return nil
	}
	var closed []string
	var clients []client.ShardCloser
	for ep, c := range pp.clients {
		if _, ok := keep[ep]; ok {
			continue
		}
		delete(pp.clients, ep)
		clients = append(clients, c)
		closed = append(closed, ep)
	}
	for ep := range pp.starting {
		if _, ok := keep[ep]; !ok {
			delete(pp.starting, ep)
			closed = append(closed, ep)
		}
	}
	if len(clients) > 0 {
		// Add while holding mu, before Close can stop admission and Wait.
		pp.work.Add(1)
	}
	pp.mu.Unlock()
	for _, c := range clients {
		_ = c.Close()
	}
	if len(clients) > 0 {
		pp.work.Done()
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
// Initializing and retired clients are not included in the snapshot.
func (pp *peerPool) Peers() []peerEntry {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	out := make([]peerEntry, 0, len(pp.clients))
	for ep, c := range pp.clients {
		out = append(out, peerEntry{Endpoint: ep, Client: c})
	}
	return out
}
