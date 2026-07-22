package fetch

import (
	"context"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// requestScheduler coordinates on-demand and prefetch cache requests made by
// one Fetcher. On-demand requests never wait for prefetch; prefetch requests
// are admitted one at a time and only while no on-demand request is active.
//
// changed is allocated only when a prefetch request has to wait. It is closed
// only when the admission predicate may have become true, then reset to nil so
// uncontended on-demand traffic does not allocate notification channels.
type requestScheduler struct {
	mu       sync.Mutex
	onDemand int
	prefetch bool
	changed  chan struct{}
}

func (s *requestScheduler) beginOnDemand() {
	s.mu.Lock()
	s.onDemand++
	s.mu.Unlock()
}

func (s *requestScheduler) endOnDemand() {
	s.mu.Lock()
	if s.onDemand <= 0 {
		s.mu.Unlock()
		panic("fetch: request scheduler on-demand underflow")
	}
	s.onDemand--
	if s.onDemand == 0 && !s.prefetch {
		s.signalLocked()
	}
	s.mu.Unlock()
}

// tryBeginPrefetch either acquires the sole prefetch token or returns the
// current change notification channel. The predicate check, token transition,
// and channel snapshot are all serialized by mu, preventing lost wakeups.
func (s *requestScheduler) tryBeginPrefetch() (bool, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.onDemand == 0 && !s.prefetch {
		s.prefetch = true
		return true, nil
	}
	return false, s.waitLocked()
}

func (s *requestScheduler) beginPrefetch(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		acquired, changed := s.tryBeginPrefetch()
		if acquired {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *requestScheduler) endPrefetch() {
	s.mu.Lock()
	if !s.prefetch {
		s.mu.Unlock()
		panic("fetch: request scheduler prefetch token underflow")
	}
	s.prefetch = false
	if s.onDemand == 0 {
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *requestScheduler) waitLocked() <-chan struct{} {
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

func (s *requestScheduler) signalLocked() {
	if s.changed == nil {
		return
	}
	close(s.changed)
	s.changed = nil
}

// scheduledCacheClient exposes two logical cache getters over one underlying
// Getter. All wrappers created by a client share the same scheduler pointer;
// the client neither owns nor closes inner.
type scheduledCacheClient struct {
	inner     cache.Getter
	scheduler *requestScheduler
}

func newScheduledCacheClient(inner cache.Getter) *scheduledCacheClient {
	return &scheduledCacheClient{
		inner:     inner,
		scheduler: &requestScheduler{},
	}
}

func (c *scheduledCacheClient) OnDemandGetter() cache.Getter {
	return &onDemandGetter{client: c}
}

func (c *scheduledCacheClient) PrefetchGetter() cache.Getter {
	return &prefetchGetter{client: c}
}

type onDemandGetter struct {
	client *scheduledCacheClient
}

func (g *onDemandGetter) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.client.scheduler.beginOnDemand()
	defer g.client.scheduler.endOnDemand()
	return g.client.inner.Get(ctx, p, key)
}

type prefetchGetter struct {
	client *scheduledCacheClient
}

func (g *prefetchGetter) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if err := g.client.scheduler.beginPrefetch(ctx); err != nil {
		return cache.CacheMiss, nil, err
	}
	defer g.client.scheduler.endPrefetch()

	// Cancellation can race with token acquisition. If it is already visible,
	// avoid entering inner; otherwise inner receives the original context and
	// remains responsible for observing any later cancellation.
	if err := ctx.Err(); err != nil {
		return cache.CacheMiss, nil, err
	}
	return g.client.inner.Get(ctx, p, key)
}

var _ cache.Getter = (*onDemandGetter)(nil)
var _ cache.Getter = (*prefetchGetter)(nil)
