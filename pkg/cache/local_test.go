package cache

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"
)

func makeHash(i int) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("chunk-%d", i)))
}

func makeData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func TestLocalCachePutGet(t *testing.T) {
	c := NewLocalCache(100, 100*1024*1024)

	hash := makeHash(0)
	data := makeData(1024)

	c.Put(hash, data)
	got, ok := c.Get(hash)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) != len(data) {
		t.Fatalf("data length mismatch: %d vs %d", len(got), len(data))
	}
}

func TestLocalCacheMiss(t *testing.T) {
	c := NewLocalCache(100, 100*1024*1024)

	hash := makeHash(0)
	_, ok := c.Get(hash)
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestLocalCacheEviction(t *testing.T) {
	c := NewLocalCache(3, 100*1024*1024)

	// Insert 3 items (fills cache).
	for i := range 3 {
		c.Put(makeHash(i), makeData(100))
	}

	// Insert 4th item → should evict oldest cold entry.
	c.Put(makeHash(3), makeData(100))

	// First entry should be evicted.
	_, ok := c.Get(makeHash(0))
	if ok {
		t.Error("entry 0 should have been evicted")
	}

	// Entry 1, 2, 3 should still be present.
	for i := 1; i <= 3; i++ {
		_, ok := c.Get(makeHash(i))
		if !ok {
			t.Errorf("entry %d should still be in cache", i)
		}
	}
}

func TestLocalCacheLRUKScanResistance(t *testing.T) {
	c := NewLocalCache(5, 100*1024*1024)

	// Insert 3 "warm" entries (accessed twice each).
	for i := range 3 {
		h := makeHash(i)
		c.Put(h, makeData(100))
		c.Get(h) // second access → promoted to warm
	}

	// Insert 3 "cold" entries (accessed once each) — fills + overflows.
	for i := 3; i < 6; i++ {
		c.Put(makeHash(i), makeData(100))
	}

	// Warm entries should survive because cold entries are evicted first.
	for i := range 3 {
		_, ok := c.Get(makeHash(i))
		if !ok {
			t.Errorf("warm entry %d should not have been evicted", i)
		}
	}
}

func TestLocalCacheByteLimit(t *testing.T) {
	c := NewLocalCache(1000, 500)

	// Each entry is 200 bytes. Should fit 2 entries (400 bytes).
	for i := range 3 {
		c.Put(makeHash(i), makeData(200))
	}

	stats := c.Stats()
	if stats.BytesUsed > 500 {
		t.Errorf("bytes used %d exceeds max 500", stats.BytesUsed)
	}
}

func TestLocalCacheEvictSpecific(t *testing.T) {
	c := NewLocalCache(100, 100*1024*1024)

	hash := makeHash(0)
	c.Put(hash, makeData(100))
	c.Evict(hash)

	_, ok := c.Get(hash)
	if ok {
		t.Error("entry should have been evicted")
	}
}

func TestLocalCacheStats(t *testing.T) {
	c := NewLocalCache(100, 100*1024*1024)

	hash := makeHash(0)
	c.Put(hash, makeData(100))

	c.Get(hash)            // hit
	c.Get(makeHash(999))   // miss

	stats := c.Stats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.EntryCount != 1 {
		t.Errorf("expected 1 entry, got %d", stats.EntryCount)
	}
}

func BenchmarkLocalCacheGet(b *testing.B) {
	c := NewLocalCache(10000, 10*1024*1024*1024)
	hash := makeHash(0)
	c.Put(hash, makeData(512*1024))

	b.ResetTimer()
	for b.Loop() {
		c.Get(hash)
	}
}
