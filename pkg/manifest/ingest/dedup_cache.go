package ingest

import (
	"context"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const defaultChunkDedupCacheLimit = 65536

// OrderedChunkSkipper is an optional interface for ordered chunk store writers
// that can advance ordinal sequencing when a chunk object is already present.
type OrderedChunkSkipper interface {
	SkipChunkOrdered(ctx context.Context, admission store.WriteAdmission, key store.ContentKey, size uint64, ordinal uint64) (bool, error)
}

type chunkMeta struct {
	key    [32]byte
	ck     store.ContentKey
	ctLen  uint64
	encLen uint64
	format byte
}

type chunkCacheEntry struct {
	fp   [32]byte
	meta chunkMeta
	prev *chunkCacheEntry
	next *chunkCacheEntry
}

type chunkFlight struct {
	done    chan struct{}
	ordinal uint64
}

type chunkDedupCache struct {
	limit   int
	entries map[[32]byte]*chunkCacheEntry
	head    *chunkCacheEntry
	tail    *chunkCacheEntry
}

func newChunkDedupCache(limit int) *chunkDedupCache {
	if limit <= 0 {
		limit = defaultChunkDedupCacheLimit
	}
	return &chunkDedupCache{
		limit:   limit,
		entries: make(map[[32]byte]*chunkCacheEntry),
	}
}

func (c *chunkDedupCache) len() int {
	if c == nil {
		return 0
	}
	return len(c.entries)
}

func (c *chunkDedupCache) get(fp [32]byte) (chunkMeta, bool) {
	if c == nil {
		return chunkMeta{}, false
	}
	entry, ok := c.entries[fp]
	if !ok {
		return chunkMeta{}, false
	}
	c.moveToFront(entry)
	return entry.meta, true
}

func (c *chunkDedupCache) put(fp [32]byte, meta chunkMeta) {
	if c == nil {
		return
	}
	if entry, ok := c.entries[fp]; ok {
		entry.meta = meta
		c.moveToFront(entry)
		return
	}
	for len(c.entries) >= c.limit && c.tail != nil {
		c.removeOldest()
	}
	entry := &chunkCacheEntry{
		fp:   fp,
		meta: meta,
	}
	c.entries[fp] = entry
	c.pushFront(entry)
}

func (c *chunkDedupCache) pushFront(e *chunkCacheEntry) {
	e.next = c.head
	e.prev = nil
	if c.head != nil {
		c.head.prev = e
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

func (c *chunkDedupCache) moveToFront(e *chunkCacheEntry) {
	if c.head == e {
		return
	}
	c.removeNode(e)
	c.pushFront(e)
}

func (c *chunkDedupCache) removeNode(e *chunkCacheEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		c.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		c.tail = e.prev
	}
	e.prev = nil
	e.next = nil
}

func (c *chunkDedupCache) removeOldest() {
	if c.tail == nil {
		return
	}
	oldest := c.tail
	c.removeNode(oldest)
	delete(c.entries, oldest.fp)
}
