package cache

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type gatedMissOrigin struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	once    sync.Once
	data    []byte

	mu   sync.Mutex
	blob *countingBlob
}

func (o *gatedMissOrigin) Get(ctx context.Context, _ store.Partition, _ store.ContentKey) (CacheResult, Blob, error) {
	o.calls.Add(1)
	o.once.Do(func() { close(o.started) })
	select {
	case <-o.release:
	case <-ctx.Done():
		return CacheMiss, nil, ctx.Err()
	}
	o.mu.Lock()
	o.blob = newCountingBlob(o.data)
	blob := o.blob
	o.mu.Unlock()
	return CacheHit, blob, nil
}

type countingHitOrigin struct {
	calls atomic.Int32
	data  []byte
}

func (o *countingHitOrigin) Get(_ context.Context, _ store.Partition, _ store.ContentKey) (CacheResult, Blob, error) {
	o.calls.Add(1)
	return CacheHit, NewMemBlob(o.data), nil
}

type blockingFillTier struct {
	mu          sync.Mutex
	data        map[store.ContentKey][]byte
	fillStarted chan struct{}
	fillRelease chan struct{}
	fillOnce    sync.Once
	gets        atomic.Int32
}

func newBlockingFillTier() *blockingFillTier {
	return &blockingFillTier{
		data:        make(map[store.ContentKey][]byte),
		fillStarted: make(chan struct{}),
		fillRelease: make(chan struct{}),
	}
}

func (t *blockingFillTier) Get(_ context.Context, _ store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
	t.gets.Add(1)
	t.mu.Lock()
	data, ok := t.data[key]
	t.mu.Unlock()
	if !ok {
		return CacheMiss, nil, nil
	}
	return CacheHit, NewMemBlob(append([]byte(nil), data...)), nil
}

func (t *blockingFillTier) Fill(ctx context.Context, _ store.Partition, key store.ContentKey, data []byte) error {
	t.fillOnce.Do(func() { close(t.fillStarted) })
	select {
	case <-t.fillRelease:
	case <-ctx.Done():
		return ctx.Err()
	}
	t.mu.Lock()
	t.data[key] = append([]byte(nil), data...)
	t.mu.Unlock()
	return nil
}

type cancelOnceOrigin struct {
	calls       atomic.Int32
	firstStart  chan struct{}
	firstStartO sync.Once
	data        []byte
}

func (o *cancelOnceOrigin) Get(ctx context.Context, _ store.Partition, _ store.ContentKey) (CacheResult, Blob, error) {
	if o.calls.Add(1) == 1 {
		o.firstStartO.Do(func() { close(o.firstStart) })
		<-ctx.Done()
		return CacheMiss, nil, ctx.Err()
	}
	return CacheHit, NewMemBlob(o.data), nil
}

func waitForCacheWaiters(t *testing.T, tc *TieredCache, fk getFlightKey, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		tc.flightMu.Lock()
		flight := tc.flights[fk]
		got := 0
		if flight != nil {
			got = flight.waiterN
		}
		tc.flightMu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("flight waiters = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForNoCacheFlight(t *testing.T, tc *TieredCache, fk getFlightKey) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		tc.flightMu.Lock()
		flight := tc.flights[fk]
		tc.flightMu.Unlock()
		if flight == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("flight remained after backfill completed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTieredCacheCoalescesConcurrentOriginMisses(t *testing.T) {
	const readers = 32
	const payload = "shared-payload"
	key := store.ContentKey{1, 2, 3}
	fk := getFlightKey{partition: store.PartitionChunk, key: key}

	origin := &gatedMissOrigin{
		started: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte(payload),
	}
	tier := newMockTier()
	tc := NewTieredCache(origin, tier)

	start := make(chan struct{})
	type result struct {
		found bool
		blob  Blob
		err   error
	}
	results := make(chan result, readers)
	for i := 0; i < readers; i++ {
		go func() {
			<-start
			found, blob, err := tc.Get(context.Background(), store.PartitionChunk, key)
			results <- result{found: found, blob: blob, err: err}
		}()
	}
	close(start)
	select {
	case <-origin.started:
	case <-time.After(2 * time.Second):
		t.Fatal("origin Get did not start")
	}
	waitForCacheWaiters(t, tc, fk, readers-1)
	close(origin.release)

	for i := 0; i < readers; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("Get error: %v", res.err)
		}
		if !res.found || res.blob == nil {
			t.Fatal("Get missed, want hit")
		}
		if got := string(res.blob.Bytes()); got != payload {
			t.Fatalf("Get payload = %q, want %q", got, payload)
		}
		res.blob.Release()
	}
	if got := origin.calls.Load(); got != 1 {
		t.Fatalf("origin calls = %d, want 1", got)
	}

	tc.Close()
	if origin.blob != nil {
		if got := origin.blob.handles.Load(); got != 0 {
			t.Fatalf("live origin blob handles = %d, want 0", got)
		}
	}
}

func TestTieredCacheRetainsFlightThroughTierZeroBackfill(t *testing.T) {
	key := store.ContentKey{4, 5, 6}
	fk := getFlightKey{partition: store.PartitionChunk, key: key}
	origin := &countingHitOrigin{data: []byte("backfill-window")}
	tier := newBlockingFillTier()
	tc := NewTieredCache(origin, tier)

	found, first, err := tc.Get(context.Background(), store.PartitionChunk, key)
	if err != nil || !found {
		t.Fatalf("first Get = %v, %v", found, err)
	}
	defer first.Release()
	select {
	case <-tier.fillStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("tier zero fill did not start")
	}

	type result struct {
		found bool
		blob  Blob
		err   error
	}
	secondDone := make(chan result, 1)
	go func() {
		found, blob, err := tc.Get(context.Background(), store.PartitionChunk, key)
		secondDone <- result{found: found, blob: blob, err: err}
	}()
	waitForCacheWaiters(t, tc, fk, 1)

	var second result
	select {
	case second = <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second Get blocked on backfill")
	}
	if second.err != nil || !second.found || second.blob == nil {
		t.Fatalf("second Get = %v, %v, %v", second.found, second.blob, second.err)
	}
	if !bytes.Equal(second.blob.Bytes(), []byte("backfill-window")) {
		t.Fatalf("second Get payload = %q", second.blob.Bytes())
	}
	second.blob.Release()
	if got := origin.calls.Load(); got != 1 {
		t.Fatalf("origin calls during fill = %d, want 1", got)
	}

	close(tier.fillRelease)
	waitForNoCacheFlight(t, tc, fk)

	found, third, err := tc.Get(context.Background(), store.PartitionChunk, key)
	if err != nil || !found {
		t.Fatalf("third Get = %v, %v", found, err)
	}
	third.Release()
	if got := origin.calls.Load(); got != 1 {
		t.Fatalf("origin calls after fill = %d, want 1", got)
	}
	tc.Close()
}

func TestTieredCacheLeaderCancellationLetsWaitersRetry(t *testing.T) {
	key := store.ContentKey{7, 8, 9}
	fk := getFlightKey{partition: store.PartitionChunk, key: key}
	origin := &cancelOnceOrigin{
		firstStart: make(chan struct{}),
		data:       []byte("retry-success"),
	}
	tc := NewTieredCache(origin, newMockTier())

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, blob, err := tc.Get(leaderCtx, store.PartitionChunk, key)
		if blob != nil {
			blob.Release()
		}
		leaderDone <- err
	}()
	select {
	case <-origin.firstStart:
	case <-time.After(2 * time.Second):
		t.Fatal("leader origin Get did not start")
	}

	type result struct {
		found bool
		blob  Blob
		err   error
	}
	waiterDone := make(chan result, 1)
	go func() {
		found, blob, err := tc.Get(context.Background(), store.PartitionChunk, key)
		waiterDone <- result{found: found, blob: blob, err: err}
	}()
	waitForCacheWaiters(t, tc, fk, 1)

	cancelLeader()
	if err := <-leaderDone; err == nil {
		t.Fatal("canceled leader returned nil error")
	}

	select {
	case res := <-waiterDone:
		if res.err != nil {
			t.Fatalf("waiter Get error: %v", res.err)
		}
		if !res.found || res.blob == nil {
			t.Fatal("waiter missed after leader cancellation")
		}
		if got := string(res.blob.Bytes()); got != "retry-success" {
			t.Fatalf("waiter payload = %q", got)
		}
		res.blob.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not retry after leader cancellation")
	}
	if got := origin.calls.Load(); got != 2 {
		t.Fatalf("origin calls = %d, want 2", got)
	}
	tc.Close()
}
