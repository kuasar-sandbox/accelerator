// Package cache implements tiered caching for encrypted chunks.
package cache

import (
	"container/list"
	"sync"
)

// LocalCache is an LRU-k (k=2) cache for encrypted chunks.
type LocalCache struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int64

	// Lookup map.
	items map[[32]byte]*entry

	// Two lists: cold (accessed once) and warm (accessed 2+ times).
	cold *list.List
	warm *list.List

	bytesUsed int64
	hits      uint64
	misses    uint64
	evictions uint64
}

type entry struct {
	hash      [32]byte
	data      []byte
	accessCnt int
	elem      *list.Element
	warm      bool // which list this entry is in
}

// CacheStats holds a snapshot of cache statistics.
type CacheStats struct {
	Hits       uint64
	Misses     uint64
	Evictions  uint64
	EntryCount int
	BytesUsed  int64
}

// NewLocalCache creates a new LRU-k local cache.
func NewLocalCache(maxEntries int, maxBytes int64) *LocalCache {
	return &LocalCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		items:      make(map[[32]byte]*entry),
		cold:       list.New(),
		warm:       list.New(),
	}
}

// Get retrieves a chunk from the cache. Returns nil, false on miss.
func (c *LocalCache) Get(hash [32]byte) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[hash]
	if !ok {
		c.misses++
		return nil, false
	}

	c.hits++
	e.accessCnt++

	// Promote from cold to warm on second access.
	if !e.warm && e.accessCnt >= 2 {
		c.cold.Remove(e.elem)
		e.elem = c.warm.PushFront(e)
		e.warm = true
	} else if e.warm {
		c.warm.MoveToFront(e.elem)
	} else {
		c.cold.MoveToFront(e.elem)
	}

	return e.data, true
}

// Put inserts a chunk into the cache.
func (c *LocalCache) Put(hash [32]byte, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing entry.
	if e, ok := c.items[hash]; ok {
		e.accessCnt++
		if !e.warm && e.accessCnt >= 2 {
			c.cold.Remove(e.elem)
			e.elem = c.warm.PushFront(e)
			e.warm = true
		} else if e.warm {
			c.warm.MoveToFront(e.elem)
		} else {
			c.cold.MoveToFront(e.elem)
		}
		return
	}

	// Evict until we have room.
	for c.needsEviction(len(data)) {
		if !c.evictOne() {
			break
		}
	}

	e := &entry{
		hash:      hash,
		data:      data,
		accessCnt: 1,
	}
	e.elem = c.cold.PushFront(e)
	c.items[hash] = e
	c.bytesUsed += int64(len(data))
}

// Evict removes a specific chunk from the cache.
func (c *LocalCache) Evict(hash [32]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.items[hash]
	if !ok {
		return
	}
	c.removeEntry(e)
}

// Stats returns a snapshot of cache statistics.
func (c *LocalCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Hits:       c.hits,
		Misses:     c.misses,
		Evictions:  c.evictions,
		EntryCount: len(c.items),
		BytesUsed:  c.bytesUsed,
	}
}

func (c *LocalCache) needsEviction(addBytes int) bool {
	if len(c.items) >= c.maxEntries {
		return true
	}
	if c.bytesUsed+int64(addBytes) > c.maxBytes {
		return true
	}
	return false
}

// evictOne removes the least recently used entry. Cold entries are evicted first.
func (c *LocalCache) evictOne() bool {
	// Prefer evicting from cold list (scan-resistant).
	if c.cold.Len() > 0 {
		elem := c.cold.Back()
		c.removeEntry(elem.Value.(*entry))
		c.evictions++
		return true
	}
	if c.warm.Len() > 0 {
		elem := c.warm.Back()
		c.removeEntry(elem.Value.(*entry))
		c.evictions++
		return true
	}
	return false
}

func (c *LocalCache) removeEntry(e *entry) {
	if e.warm {
		c.warm.Remove(e.elem)
	} else {
		c.cold.Remove(e.elem)
	}
	delete(c.items, e.hash)
	c.bytesUsed -= int64(len(e.data))
}
