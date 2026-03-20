package cache

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/fullof-work/container-accelerator-research/pkg/store"
)

func TestTieredCacheL1Hit(t *testing.T) {
	dir := t.TempDir()
	origin, err := store.NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	l1 := NewLocalCache(100, 100*1024*1024)
	tc := NewTieredCache(l1, origin)
	ctx := context.Background()

	data := make([]byte, 1024)
	rand.Read(data)
	hash := sha256.Sum256(data)

	// Pre-populate L1.
	l1.Put(hash, data)

	got, err := tc.Get(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Fatalf("data length mismatch: %d vs %d", len(got), len(data))
	}

	stats := tc.L1Stats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 L1 hit, got %d", stats.Hits)
	}
}

func TestTieredCacheL1MissOriginHit(t *testing.T) {
	dir := t.TempDir()
	origin, err := store.NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	l1 := NewLocalCache(100, 100*1024*1024)
	tc := NewTieredCache(l1, origin)
	ctx := context.Background()

	data := make([]byte, 1024)
	rand.Read(data)
	hash := sha256.Sum256(data)

	// Put in origin only.
	if err := origin.Put(ctx, hash, data); err != nil {
		t.Fatal(err)
	}

	got, err := tc.Get(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Fatalf("data length mismatch: %d vs %d", len(got), len(data))
	}

	// Should now be promoted to L1.
	_, ok := l1.Get(hash)
	if !ok {
		t.Error("expected data to be promoted to L1 after origin hit")
	}
}

func TestTieredCacheNotFound(t *testing.T) {
	dir := t.TempDir()
	origin, err := store.NewFilesystemStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	l1 := NewLocalCache(100, 100*1024*1024)
	tc := NewTieredCache(l1, origin)
	ctx := context.Background()

	var hash [32]byte
	rand.Read(hash[:])

	_, err = tc.Get(ctx, hash)
	if err == nil {
		t.Error("expected error for missing chunk")
	}
}
