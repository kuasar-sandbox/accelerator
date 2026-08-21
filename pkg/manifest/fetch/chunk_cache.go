package fetch

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

const (
	manifestChunkCacheMaxEntries = 32
	manifestChunkCacheMaxBytes   = 32 << 20
	manifestChunkCacheIdleTTL    = 5 * time.Second
)

var errManifestChunkCacheClosed = errors.New("fetch: manifest chunk cache is closed")

// decryptedChunkKey includes every input that determines plaintext. A single
// manifest stream can therefore reuse duplicate physical chunks without
// aliasing entries that happen to carry a different key or logical size.
type decryptedChunkKey struct {
	ciphertextHash [32]byte
	decryptKey     [32]byte
	plaintextSize  uint32
}

type decryptedChunkEntry struct {
	key      decryptedChunkKey
	plain    []byte
	lastUsed time.Time
	element  *list.Element
	readers  int
}

type decryptedChunkLoad struct {
	done chan struct{}
}

// decryptedChunkCache is a per-manifestStream plaintext cache. The resident
// entries are ordered most-recently-used first. Entries removed while a reader
// is copying from them are retired immediately and cleared after the last
// reader releases its lease.
type decryptedChunkCache struct {
	mu sync.Mutex

	entries  map[decryptedChunkKey]*decryptedChunkEntry
	inflight map[decryptedChunkKey]*decryptedChunkLoad
	lru      list.List
	bytes    int

	maxEntries int
	maxBytes   int
	idleTTL    time.Duration
	timer      *time.Timer
	closed     bool
}

func newDecryptedChunkCache(maxEntries, maxBytes int, idleTTL time.Duration) *decryptedChunkCache {
	return &decryptedChunkCache{
		entries:    make(map[decryptedChunkKey]*decryptedChunkEntry),
		inflight:   make(map[decryptedChunkKey]*decryptedChunkLoad),
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		idleTTL:    idleTTL,
	}
}

type decryptedChunkLease struct {
	cache *decryptedChunkCache
	entry *decryptedChunkEntry
	plain []byte // non-nil only when the result was deliberately not admitted
}

func (l *decryptedChunkLease) bytes() []byte {
	if l.entry != nil {
		return l.entry.plain
	}
	return l.plain
}

func (l *decryptedChunkLease) release() {
	if l.entry != nil {
		l.cache.releaseEntry(l.entry)
		l.entry = nil
	}
	if l.plain != nil {
		clearSlice(l.plain)
		l.plain = nil
	}
}

func (c *decryptedChunkCache) acquire(key decryptedChunkKey) *decryptedChunkLease {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.expireLocked(now)
	entry := c.entries[key]
	if entry == nil {
		return nil
	}
	c.touchLocked(entry, now)
	entry.readers++
	return &decryptedChunkLease{cache: c, entry: entry}
}

// acquireOrLoad returns a pinned cache entry. Only one caller loads a missing
// key at a time; waiters with a live context retry after a failed load instead
// of inheriting another caller's cancellation or transient failure.
func (c *decryptedChunkCache) acquireOrLoad(
	ctx context.Context,
	key decryptedChunkKey,
	loader func(context.Context) ([]byte, error),
) (*decryptedChunkLease, error) {
	for {
		now := time.Now()
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, errManifestChunkCacheClosed
		}
		c.expireLocked(now)
		if entry := c.entries[key]; entry != nil {
			c.touchLocked(entry, now)
			entry.readers++
			c.mu.Unlock()
			return &decryptedChunkLease{cache: c, entry: entry}, nil
		}
		if load := c.inflight[key]; load != nil {
			done := load.done
			c.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		load := &decryptedChunkLoad{done: make(chan struct{})}
		c.inflight[key] = load
		c.mu.Unlock()

		plain, err := loader(ctx)

		c.mu.Lock()
		delete(c.inflight, key)
		var lease *decryptedChunkLease
		if err != nil {
			clearSlice(plain)
		} else {
			if c.closed || c.maxEntries <= 0 || len(plain) > c.maxBytes {
				lease = &decryptedChunkLease{cache: c, plain: plain}
			} else {
				entry := &decryptedChunkEntry{
					key:      key,
					plain:    plain,
					lastUsed: time.Now(),
					readers:  1,
				}
				entry.element = c.lru.PushFront(entry)
				c.entries[key] = entry
				c.bytes += len(plain)
				c.evictCapacityLocked()
				if entry.element == nil {
					lease = &decryptedChunkLease{cache: c, plain: plain}
				} else {
					lease = &decryptedChunkLease{cache: c, entry: entry}
					c.scheduleExpiryLocked()
				}
			}
		}
		close(load.done)
		c.mu.Unlock()
		return lease, err
	}
}

func (c *decryptedChunkCache) releaseEntry(entry *decryptedChunkEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.readers--
	if entry.readers == 0 && entry.element == nil {
		clearSlice(entry.plain)
		entry.plain = nil
	}
}

func (c *decryptedChunkCache) touchLocked(entry *decryptedChunkEntry, now time.Time) {
	entry.lastUsed = now
	c.lru.MoveToFront(entry.element)
}

func (c *decryptedChunkCache) evictCapacityLocked() {
	for len(c.entries) > c.maxEntries || c.bytes > c.maxBytes {
		oldest := c.lru.Back()
		if oldest == nil {
			return
		}
		c.retireLocked(oldest.Value.(*decryptedChunkEntry))
	}
}

func (c *decryptedChunkCache) expireLocked(now time.Time) {
	if c.idleTTL <= 0 {
		for oldest := c.lru.Back(); oldest != nil; oldest = c.lru.Back() {
			c.retireLocked(oldest.Value.(*decryptedChunkEntry))
		}
		return
	}
	for {
		oldest := c.lru.Back()
		if oldest == nil {
			return
		}
		entry := oldest.Value.(*decryptedChunkEntry)
		if now.Sub(entry.lastUsed) < c.idleTTL {
			return
		}
		c.retireLocked(entry)
	}
}

func (c *decryptedChunkCache) retireLocked(entry *decryptedChunkEntry) {
	if entry.element == nil {
		return
	}
	c.lru.Remove(entry.element)
	entry.element = nil
	delete(c.entries, entry.key)
	c.bytes -= len(entry.plain)
	if entry.readers == 0 {
		clearSlice(entry.plain)
		entry.plain = nil
	}
}

// scheduleExpiryLocked keeps at most one active timer. Hits do not reset it:
// an early wake-up is cheap and recomputes the next oldest deadline, avoiding
// one timer allocation per cache hit on the page-fault hot path.
func (c *decryptedChunkCache) scheduleExpiryLocked() {
	if c.closed || c.timer != nil || c.lru.Len() == 0 {
		return
	}
	oldest := c.lru.Back().Value.(*decryptedChunkEntry)
	delay := time.Until(oldest.lastUsed.Add(c.idleTTL))
	if delay < 0 {
		delay = 0
	}
	c.timer = time.AfterFunc(delay, c.expireIdle)
}

func (c *decryptedChunkCache) expireIdle() {
	now := time.Now()
	c.mu.Lock()
	c.timer = nil
	if !c.closed {
		c.expireLocked(now)
		c.scheduleExpiryLocked()
	}
	c.mu.Unlock()
}

func (c *decryptedChunkCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	for oldest := c.lru.Back(); oldest != nil; oldest = c.lru.Back() {
		c.retireLocked(oldest.Value.(*decryptedChunkEntry))
	}
}
