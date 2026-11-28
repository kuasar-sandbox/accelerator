package fetch

import (
	"container/list"
	"context"
	"sync"
)

// baseCache is a plaintext cache of decoded delta bases, shared by the streams
// a Fetcher opens. Buffers are zeroed when they leave.
//
// It is keyed by baseID: the base object's address AND the key and size the
// delta object says to decrypt it with. The address alone is not enough. A base
// is an ordinary RAW or Snappy object under AES-CTR, so decrypting it with the
// wrong key yields garbage without an error; if the first delta to name a base
// carried a wrong key (a corrupt or hostile delta object), an address-only
// cache would hand that garbage to every later reader of the same base, each of
// whom supplied the right key. Keying on the key also keeps a tenant that
// shares the customer key but not the extra salt from reading a base plaintext
// it could not have decrypted itself: the plaintext it gets is the one its own
// key produces, or nothing.
type baseCache struct {
	mu       sync.Mutex
	max      int64
	used     int64
	m        map[baseID]*baseEntry
	lru      *list.List
	inflight map[baseID]*baseLoad
	closed   bool
}

// baseID identifies one decryption of one base object. Two deltas that name the
// same object with the same key and size share the entry; any difference is a
// different entry.
type baseID struct {
	objectKey [32]byte
	baseKey   [32]byte
	size      uint32
}

type baseEntry struct {
	key  baseID
	data []byte
	refs int
	dead bool
	elem *list.Element
}

type baseLoad struct {
	done chan struct{}
	err  error
}

func newBaseCache(maxBytes int64) *baseCache {
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	return &baseCache{
		max:      maxBytes,
		m:        make(map[baseID]*baseEntry),
		lru:      list.New(),
		inflight: make(map[baseID]*baseLoad),
	}
}

func (c *baseCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for _, e := range c.m {
		if e.refs == 0 {
			clearSlice(e.data)
			e.data = nil
		}
		e.dead = true
	}
	c.m = map[baseID]*baseEntry{}
	c.lru.Init()
	c.used = 0
}

// getOrLoad returns a leased plaintext. release must be called exactly once.
// Concurrent callers for the same key share one load.
func (c *baseCache) getOrLoad(ctx context.Context, key baseID, load func(context.Context) ([]byte, error)) (data []byte, release func(), err error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, nil, errManifestChunkCacheClosed
		}
		if e, ok := c.m[key]; ok {
			c.lru.MoveToFront(e.elem)
			e.refs++
			rel := c.releaseFn(e)
			got := e.data
			c.mu.Unlock()
			return got, rel, nil
		}
		if inflight := c.inflight[key]; inflight != nil {
			done := inflight.done
			c.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		inflight := &baseLoad{done: make(chan struct{})}
		c.inflight[key] = inflight
		c.mu.Unlock()

		plain, loadErr := load(ctx)

		c.mu.Lock()
		delete(c.inflight, key)
		inflight.err = loadErr
		close(inflight.done)
		if loadErr != nil || c.closed {
			clearSlice(plain)
			c.mu.Unlock()
			if loadErr != nil {
				return nil, nil, loadErr
			}
			return nil, nil, errManifestChunkCacheClosed
		}
		if int64(len(plain)) > c.max {
			// Too big to cache. Hand the caller the only copy and zero it on release.
			c.mu.Unlock()
			return plain, func() { clearSlice(plain) }, nil
		}
		for c.used+int64(len(plain)) > c.max {
			back := c.lru.Back()
			if back == nil {
				break
			}
			c.evictLocked(back.Value.(*baseEntry))
		}
		e := &baseEntry{key: key, data: plain, refs: 1}
		e.elem = c.lru.PushFront(e)
		c.m[key] = e
		c.used += int64(len(plain))
		rel := c.releaseFn(e)
		c.mu.Unlock()
		return plain, rel, nil
	}
}

func (c *baseCache) evictLocked(e *baseEntry) {
	c.lru.Remove(e.elem)
	delete(c.m, e.key)
	c.used -= int64(len(e.data))
	e.dead = true
	if e.refs == 0 {
		clearSlice(e.data)
		e.data = nil
	}
}

func (c *baseCache) releaseFn(e *baseEntry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			e.refs--
			if e.dead && e.refs == 0 {
				clearSlice(e.data)
				e.data = nil
			}
		})
	}
}
