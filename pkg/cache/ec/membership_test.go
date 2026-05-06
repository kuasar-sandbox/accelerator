package ec

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// TestEncodePrefixed_RoundTrip exercises the zero-extra-copy Fill-side
// encode: allocate N prefix-padded buffers, encode in place, parse the
// first two bytes, and verify shard-data body Decodes back to the
// original value.
func TestEncodePrefixed_RoundTrip(t *testing.T) {
	enc, err := newEncoder(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	total := 5

	for _, size := range []int{1, 256, 1000, 1 << 20} {
		t.Run("", func(t *testing.T) {
			value := bytes.Repeat([]byte{0x7A}, size)
			bufSize := ShardBufferSize(len(value), 4)
			bufs := make([][]byte, total)
			for i := 0; i < total; i++ {
				bufs[i] = make([]byte, bufSize)
			}
			if err := enc.EncodePrefixed(value, bufs); err != nil {
				t.Fatalf("EncodePrefixed: %v", err)
			}

			// Each buffer begins with [idx][total]; body is the raw
			// RS shard data that Decode consumes.
			shards := make([][]byte, total)
			for i := 0; i < total; i++ {
				idx, tot, data, ok := cache.ParseShardPrefix(bufs[i])
				if !ok {
					t.Fatalf("bufs[%d] missing prefix", i)
				}
				if int(idx) != i {
					t.Fatalf("bufs[%d]: parsed idx %d, want %d", i, idx, i)
				}
				if int(tot) != total {
					t.Fatalf("bufs[%d]: parsed total %d, want %d", i, tot, total)
				}
				shards[i] = data
			}
			got, err := enc.Decode(shards)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !bytes.Equal(got, value) {
				t.Fatalf("decoded mismatch: got %d bytes, want %d", len(got), len(value))
			}
		})
	}
}

// TestDecodeWithReorderedShards simulates a membership change where
// LocateN returns peers in a different order: the peer at position 0
// now holds shard idx=3, etc. The decoder should still reconstruct
// correctly because shards[] is keyed by peer-reported idx, not by
// arrival order.
func TestDecodeWithReorderedShards(t *testing.T) {
	enc, err := newEncoder(4, 1)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte{0x5A}, 4096)
	bufs := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		bufs[i] = make([]byte, ShardBufferSize(len(value), 4))
	}
	if err := enc.EncodePrefixed(value, bufs); err != nil {
		t.Fatal(err)
	}

	// Simulate receiving any 4 shards in arbitrary order; drop shard 2.
	shards := make([][]byte, 5)
	for _, recvOrder := range []int{4, 0, 3, 1} {
		_, _, data, _ := cache.ParseShardPrefix(bufs[recvOrder])
		shards[recvOrder] = data
	}
	// shard idx 2 is nil (missing)

	got, err := enc.Decode(shards)
	if err != nil {
		t.Fatalf("Decode (one missing): %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("decoded mismatch after one miss: got %d bytes, want %d", len(got), len(value))
	}
}

func TestRouter_ApplyMembership_MonotoneEpoch(t *testing.T) {
	peers := []Peer{{ID: "a", Endpoint: "a:1"}, {ID: "b", Endpoint: "b:1"}}
	r, err := newRouter(peers)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Epoch(); got != 1 {
		t.Fatalf("initial epoch: got %d, want 1", got)
	}

	// Same-epoch update: rejected.
	if _, _, err := r.ApplyMembership(1, peers); err == nil {
		t.Fatal("expected error on same epoch")
	}
	// Regressing epoch: rejected.
	if _, _, err := r.ApplyMembership(0, peers); err == nil {
		t.Fatal("expected error on lower epoch")
	}
	// Forward epoch: accepted.
	newPeers := []Peer{{ID: "a", Endpoint: "a:1"}, {ID: "c", Endpoint: "c:1"}}
	if _, _, err := r.ApplyMembership(2, newPeers); err != nil {
		t.Fatalf("epoch 2: %v", err)
	}
	if got := r.Epoch(); got != 2 {
		t.Fatalf("after apply: epoch got %d, want 2", got)
	}
	if ep := r.Endpoint("c"); ep != "c:1" {
		t.Fatalf("new peer endpoint: got %q, want c:1", ep)
	}
	// Old peer b should be gone.
	if ep := r.Endpoint("b"); ep != "" {
		t.Fatalf("removed peer b still resolves to %q", ep)
	}
}

// TestRouter_ConcurrentLookupApply verifies that concurrent LocateN
// calls never observe a partial/corrupt routerState during an
// ApplyMembership swap. Under -race, a data race would trip.
func TestRouter_ConcurrentLookupApply(t *testing.T) {
	peers := []Peer{
		{ID: "a", Endpoint: "a:1"},
		{ID: "b", Endpoint: "b:1"},
		{ID: "c", Endpoint: "c:1"},
	}
	r, err := newRouter(peers)
	if err != nil {
		t.Fatal(err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := store.ContentKey{}
			for !stop.Load() {
				_, _ = r.LocateN(key, 2)
			}
		}()
	}

	for epoch := int64(2); epoch <= 200; epoch++ {
		next := []Peer{
			{ID: "a", Endpoint: "a:1"},
			{ID: "b", Endpoint: "b:1"},
		}
		if epoch%2 == 0 {
			next = append(next, Peer{ID: "c", Endpoint: "c:1"})
		} else {
			next = append(next, Peer{ID: "d", Endpoint: "d:1"})
		}
		if _, _, err := r.ApplyMembership(epoch, next); err != nil {
			t.Fatalf("apply epoch %d: %v", epoch, err)
		}
	}
	stop.Store(true)
	wg.Wait()
}

func TestShardValuePrefix_RoundTrip(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	buf := make([]byte, cache.ShardPrefixSize+len(data))
	cache.EncodeShardPrefix(buf, 3, 5)
	copy(buf[cache.ShardPrefixSize:], data)

	idx, total, got, ok := cache.ParseShardPrefix(buf)
	if !ok {
		t.Fatal("ParseShardPrefix returned !ok")
	}
	if idx != 3 || total != 5 {
		t.Fatalf("parsed (%d, %d), want (3, 5)", idx, total)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %x, want %x", got, data)
	}
}

func TestShardValuePrefix_TooShort(t *testing.T) {
	_, _, _, ok := cache.ParseShardPrefix([]byte{0x01})
	if ok {
		t.Fatal("expected !ok for 1-byte buffer")
	}
}
