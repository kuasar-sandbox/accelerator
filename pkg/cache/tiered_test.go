package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// countingBlob is a cache.Blob mock that tracks Clone and Release
// calls through a shared refcount, so tests can assert that fill-
// aside handles the lifecycle correctly.
type countingBlob struct {
	data    []byte
	state   *blobState
	handles *atomic.Int32
}

type blobState struct {
	clones   atomic.Int32
	releases atomic.Int32
}

func newCountingBlob(data []byte) *countingBlob {
	b := &countingBlob{
		data:    data,
		state:   &blobState{},
		handles: new(atomic.Int32),
	}
	b.handles.Store(1)
	return b
}

func (b *countingBlob) Bytes() []byte { return b.data }

func (b *countingBlob) Clone() Blob {
	b.state.clones.Add(1)
	b.handles.Add(1)
	return &countingBlob{data: b.data, state: b.state, handles: b.handles}
}

func (b *countingBlob) Release() {
	b.state.releases.Add(1)
	b.handles.Add(-1)
}

// mockTier is a Tier that never hits on Get and records the
// bytes it was asked to Fill. It also serialises Fill behind a
// channel so the test can observe the moment the goroutine sees the
// data.
type mockTier struct {
	fillCalled atomic.Int32
	fillBytes  chan []byte
}

func newMockTier() *mockTier {
	return &mockTier{fillBytes: make(chan []byte, 4)}
}

func (m *mockTier) Get(ctx context.Context, p store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
	return CacheMiss, nil, nil
}

func (m *mockTier) Fill(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) error {
	m.fillCalled.Add(1)
	// Copy data — we're validating startFill clone semantics, not
	// Fill's own contract (which explicitly allows data to go away
	// after Fill returns).
	m.fillBytes <- append([]byte(nil), data...)
	return nil
}

// TestTieredCache_StartFillClonesBlob asserts the contract baked into
// startFill:
//
//   - Every call to startFill produces exactly one Clone of the
//     supplied blob (for the fill goroutine's own handle) and exactly
//     one Release (after Fill returns).
//   - The caller's original handle is neither cloned nor released by
//     startFill itself.
//   - The bytes received by Fill match the blob's contents verbatim.
//
// This is the load-bearing invariant of the refcounted Blob design —
// without Clone-before-goroutine, a pool-backed blob could be returned
// to its pool while the fill goroutine still references it.
func TestTieredCache_StartFillClonesBlob(t *testing.T) {
	tier := newMockTier()
	tc := NewTieredCache(NewOriginAdapter(nil, 0), tier)

	blob := newCountingBlob([]byte("payload"))
	key := store.ContentKey{1, 2, 3}

	tc.startFill(0, store.PartitionChunk, key, blob)

	// Wait for the goroutine to do its work.
	select {
	case got := <-tier.fillBytes:
		if string(got) != "payload" {
			t.Errorf("Fill received wrong data: got %q, want %q", got, "payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Fill was not called within 2s")
	}

	// Join the fill WaitGroup to ensure Release has happened.
	if err := tc.WaitFills(context.Background()); err != nil {
		t.Fatalf("WaitFills: %v", err)
	}

	// Validate the clone accounting. The original blob still has
	// refcount = 1 (caller owns it); the fill-goroutine's clone has
	// been Released.
	if got := blob.state.clones.Load(); got != 1 {
		t.Errorf("Clone count: got %d, want 1", got)
	}
	if got := blob.state.releases.Load(); got != 1 {
		t.Errorf("Release count (clone side): got %d, want 1", got)
	}
	if got := blob.handles.Load(); got != 1 {
		t.Errorf("live handle count: got %d, want 1 (the original)", got)
	}
	if got := tier.fillCalled.Load(); got != 1 {
		t.Errorf("Fill call count: got %d, want 1", got)
	}

	// The caller's original blob is still usable.
	if string(blob.Bytes()) != "payload" {
		t.Errorf("original blob corrupted after startFill")
	}
	blob.Release()
}

// hitTier is a Tier that always reports CacheHit and returns a fresh
// blob; used to walk TieredCache counters through the cascade.
type hitTier struct{ data []byte }

func (h *hitTier) Get(ctx context.Context, p store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
	return CacheHit, NewMemBlob(h.data), nil
}
func (h *hitTier) Fill(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) error {
	return nil
}

// missGetter is a trivial Getter that always misses (for the origin
// slot) so the cascade runs through without a hit.
type missGetter struct{}

func (missGetter) Get(ctx context.Context, p store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
	return CacheMiss, nil, nil
}

// TestTieredCache_CountersCascade exercises the Get cascade with a
// miss-then-hit pattern and verifies the parallel hit/miss/fill
// counters track the layers correctly. It also verifies the origin
// counters bump when the cascade falls through to origin.
func TestTieredCache_CountersCascade(t *testing.T) {
	miss := newMockTier() // tier 0 — miss
	hit := &hitTier{data: []byte("payload")}

	tc := NewTieredCache(missGetter{}, miss, hit)
	key := store.ContentKey{1, 2, 3}

	found, blob, err := tc.Get(context.Background(), store.PartitionChunk, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected hit (tier 1)")
	}
	blob.Release()

	// Wait for backfill to complete.
	if err := tc.WaitFills(context.Background()); err != nil {
		t.Fatalf("WaitFills: %v", err)
	}

	c := tc.Counters()
	if c.TierHits[0] != 0 || c.TierMisses[0] != 1 {
		t.Errorf("tier 0: hits=%d misses=%d (want 0/1)", c.TierHits[0], c.TierMisses[0])
	}
	if c.TierHits[1] != 1 || c.TierMisses[1] != 0 {
		t.Errorf("tier 1: hits=%d misses=%d (want 1/0)", c.TierHits[1], c.TierMisses[1])
	}
	// tier 0 is upstream of the hit; one backfill attempt flows up.
	if c.TierFills[0] != 1 {
		t.Errorf("tier 0 fills=%d (want 1 — backfilled from hit)", c.TierFills[0])
	}
	if c.TierFills[1] != 0 {
		t.Errorf("tier 1 fills=%d (want 0 — it was the hit)", c.TierFills[1])
	}
	if c.OriginHits != 0 || c.OriginMisses != 0 {
		t.Errorf("origin: hits=%d misses=%d (want both 0 — hit at tier 1)", c.OriginHits, c.OriginMisses)
	}

	// Now drive a full miss so origin gets bumped.
	tc2 := NewTieredCache(missGetter{}, newMockTier())
	_, _, _ = tc2.Get(context.Background(), store.PartitionChunk, key)
	c2 := tc2.Counters()
	if c2.TierMisses[0] != 1 {
		t.Errorf("tier 0 missed: got %d", c2.TierMisses[0])
	}
	if c2.OriginMisses != 1 {
		t.Errorf("origin missed: got %d", c2.OriginMisses)
	}
}
