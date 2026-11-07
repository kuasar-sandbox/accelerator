package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type fillTestGetter func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error)

func (f fillTestGetter) Get(ctx context.Context, p store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
	return f(ctx, p, key)
}

type fillTestTier struct {
	get  fillTestGetter
	fill func(context.Context, store.Partition, store.ContentKey, []byte) error
}

func (t *fillTestTier) Get(ctx context.Context, p store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
	if t.get != nil {
		return t.get(ctx, p, key)
	}
	return CacheMiss, nil, nil
}

func (t *fillTestTier) Fill(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) error {
	return t.fill(ctx, p, key, data)
}

type stableFillTestTier struct{ *fillTestTier }

func (*stableFillTestTier) StableFillTarget() {}

type fillTestRead struct {
	found bool
	blob  Blob
	err   error
}

func startFillTestRead(tc *TieredCache, ctx context.Context, p store.Partition, key store.ContentKey) <-chan fillTestRead {
	done := make(chan fillTestRead, 1)
	go func() {
		found, blob, err := tc.Get(ctx, p, key)
		done <- fillTestRead{found: found, blob: blob, err: err}
	}()
	return done
}

func awaitFillTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not reach its completion barrier")
		var zero T
		return zero
	}
}

func requireFillTestPayload(t *testing.T, done <-chan fillTestRead, want string) Blob {
	t.Helper()
	r := awaitFillTest(t, done)
	if r.err != nil || !r.found || r.blob == nil {
		if r.blob != nil {
			r.blob.Release()
		}
		t.Fatalf("Get = found %v, blob %T, err %v", r.found, r.blob, r.err)
	}
	if got := string(r.blob.Bytes()); got != want {
		r.blob.Release()
		t.Fatalf("Get data = %q, want %q", got, want)
	}
	return r.blob
}

func TestTieredCacheReusesSuccessfulFillWithoutWaiting(t *testing.T) {
	for _, fillErr := range []error{nil, errors.New("fill failed")} {
		t.Run(fmt.Sprint(fillErr), func(t *testing.T) {
			gate := make(chan struct{})
			release := sync.OnceFunc(func() { close(gate) })
			t.Cleanup(release)
			started := make(chan struct{}, 2)
			tier := &stableFillTestTier{&fillTestTier{fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
				started <- struct{}{}
				select {
				case <-gate:
					return fillErr
				case <-ctx.Done():
					return ctx.Err()
				}
			}}}
			pool := NewPool(64)
			var calls atomic.Int32
			refs := make(chan *atomic.Int32, 2)
			origin := fillTestGetter(func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error) {
				calls.Add(1)
				buf, blob := pool.Alloc(7)
				copy(buf, "payload")
				refs <- blob.(*pooledBlob).refs
				return CacheHit, blob, nil
			})
			tc := NewTieredCache(origin, NewTierAdapter(tier, 1))
			t.Cleanup(tc.Close)
			key := store.ContentKey{1}
			first := requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload")
			first.Release()
			awaitFillTest(t, started)
			firstRefs := awaitFillTest(t, refs)

			// The first caller has released its handle and Fill is still blocked.
			// Later readers must get their own live reference without another Get.
			second := requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload")
			if got := calls.Load(); got != 1 {
				t.Fatalf("origin calls during Fill = %d, want 1", got)
			}
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			r := awaitFillTest(t, startFillTestRead(tc, cancelled, store.PartitionChunk, key))
			if !errors.Is(r.err, context.Canceled) || r.blob != nil {
				t.Fatalf("cancelled reuse = %+v", r)
			}

			release()
			tc.Wait()
			if got := firstRefs.Load(); got != 1 {
				t.Fatalf("references after Fill = %d, want only the second reader", got)
			}
			if string(second.Bytes()) != "payload" {
				t.Fatal("Fill retirement invalidated a reader's Blob")
			}
			second.Release()
			if got := firstRefs.Load(); got != 0 {
				t.Fatalf("references after reader release = %d", got)
			}

			// This fixture remains a miss (or models eviction after a successful
			// Fill). Neither successful nor failed fills leave a retained cache.
			third := requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload")
			third.Release()
			tc.Wait()
			if got := calls.Load(); got != 2 {
				t.Fatalf("origin calls after Fill = %d, want 2", got)
			}
			if got := tc.Counters().TierFills[0]; got != 2 {
				t.Fatalf("fills after retry = %d, want 2", got)
			}
			if got := awaitFillTest(t, refs).Load(); got != 0 {
				t.Fatalf("references after second Fill = %d", got)
			}
		})
	}
}

func TestTieredCacheCloseKeepsFillOwnershipUntilReturn(t *testing.T) {
	cancelled := make(chan struct{})
	finish := make(chan struct{})
	release := sync.OnceFunc(func() { close(finish) })
	tier := &stableFillTestTier{&fillTestTier{fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
		<-ctx.Done()
		close(cancelled)
		// Cancellation need not interrupt an underlying storage call at once.
		<-finish
		return ctx.Err()
	}}}
	buf, blob := NewPool(64).Alloc(7)
	copy(buf, "payload")
	refs := blob.(*pooledBlob).refs
	origin := fillTestGetter(func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error) {
		return CacheHit, blob, nil
	})
	tc := NewTieredCache(origin, tier)
	t.Cleanup(func() { release(); tc.Close() })
	key := store.ContentKey{10}
	requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload").Release()
	reader := requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload")
	closed := make(chan struct{})
	go func() { tc.Close(); close(closed) }()
	awaitFillTest(t, cancelled)
	if got := refs.Load(); got != 2 {
		t.Fatalf("references while cancelled Fill still runs = %d, want Fill and reader", got)
	}
	select {
	case <-closed:
		t.Fatal("Close returned before the actual Fill")
	default:
	}
	release()
	awaitFillTest(t, closed)
	if got := refs.Load(); got != 1 {
		t.Fatalf("references after Close = %d, want reader only", got)
	}
	if string(reader.Bytes()) != "payload" {
		t.Fatal("Close invalidated a returned Blob")
	}
	reader.Release()
	if got := refs.Load(); got != 0 {
		t.Fatalf("references after reader release = %d", got)
	}
}

func TestTieredCacheReadersRaceWithFillRetirement(t *testing.T) {
	const readerCount, readsPerReader = 16, 32
	race := make(chan struct{})
	release := sync.OnceFunc(func() { close(race) })
	fillStarted := make(chan struct{})
	readersReady := make(chan struct{}, readerCount)
	var gateReads atomic.Bool
	var fills atomic.Int32
	tier := &stableFillTestTier{&fillTestTier{
		get: func(ctx context.Context, _ store.Partition, _ store.ContentKey) (CacheResult, Blob, error) {
			if gateReads.Load() {
				readersReady <- struct{}{}
				select {
				case <-race:
				case <-ctx.Done():
					return CacheMiss, nil, ctx.Err()
				}
			}
			return CacheMiss, nil, nil
		},
		fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
			if fills.Add(1) == 1 {
				close(fillStarted)
				select {
				case <-race:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	}}
	payload := bytes.Repeat([]byte("successful immutable chunk"), 128)
	pool := NewPool(len(payload))
	var refsMu sync.Mutex
	var refs []*atomic.Int32
	origin := fillTestGetter(func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error) {
		buf, blob := pool.Alloc(len(payload))
		copy(buf, payload)
		refsMu.Lock()
		refs = append(refs, blob.(*pooledBlob).refs)
		refsMu.Unlock()
		return CacheHit, blob, nil
	})
	tc := NewTieredCache(origin, tier)
	var readers sync.WaitGroup
	t.Cleanup(func() { release(); readers.Wait(); tc.Close() })
	key := store.ContentKey{11}
	requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), string(payload)).Release()
	awaitFillTest(t, fillStarted)

	errs := make(chan error, readerCount)
	gateReads.Store(true)
	for range readerCount {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range readsPerReader {
				found, blob, err := tc.Get(t.Context(), store.PartitionChunk, key)
				if err != nil || !found || blob == nil {
					if blob != nil {
						blob.Release()
					}
					errs <- fmt.Errorf("Get: found=%v, blob=%T, err=%v", found, blob, err)
					return
				}
				valid := bytes.Equal(blob.Bytes(), payload)
				blob.Release()
				if !valid {
					errs <- errors.New("Fill retirement corrupted a reader's payload")
					return
				}
			}
		}()
	}
	for range readerCount {
		awaitFillTest(t, readersReady)
	}
	// Release the first Fill and all pending lookups together. Reads may clone
	// that payload before retirement or independently fetch after it, and keep
	// racing subsequent short fills; either outcome must preserve ownership.
	gateReads.Store(false)
	release()
	readersDone := make(chan struct{})
	go func() { readers.Wait(); close(readersDone) }()
	awaitFillTest(t, readersDone)
	tc.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	refsMu.Lock()
	defer refsMu.Unlock()
	for i, ref := range refs {
		if got := ref.Load(); got != 0 {
			t.Errorf("origin Blob %d retained %d references after readers and fills exited", i, got)
		}
	}
}

func TestTieredCacheColdMissesRemainIndependent(t *testing.T) {
	const readers = 8
	entered := make(chan struct{}, readers)
	getGate := make(chan struct{})
	release := sync.OnceFunc(func() { close(getGate) })
	t.Cleanup(release)
	origin := fillTestGetter(func(ctx context.Context, _ store.Partition, _ store.ContentKey) (CacheResult, Blob, error) {
		entered <- struct{}{}
		select {
		case <-getGate:
			return CacheHit, NewMemBlob([]byte("payload")), nil
		case <-ctx.Done():
			return CacheMiss, nil, ctx.Err()
		}
	})
	var fills atomic.Int32
	tier := &stableFillTestTier{&fillTestTier{fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
		fills.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}}}
	tc := NewTieredCache(origin, tier)
	t.Cleanup(tc.Close)
	reads := make([]<-chan fillTestRead, readers)
	for i := range reads {
		reads[i] = startFillTestRead(tc, t.Context(), store.PartitionChunk, store.ContentKey{2})
	}
	// Every cold request reaches origin before any successful payload exists.
	for range readers {
		awaitFillTest(t, entered)
	}
	release()
	for _, read := range reads {
		requireFillTestPayload(t, read, "payload").Release()
	}
	tc.Close()
	if got := fills.Load(); got != 1 {
		t.Fatalf("equivalent fills = %d, want 1", got)
	}
}

func TestTieredCacheRetryAndRedundantReadEscapeSlowAttempt(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_first=%v", cancelFirst), func(t *testing.T) {
			entered := make(chan struct{})
			getGate := make(chan struct{})
			release := sync.OnceFunc(func() { close(getGate) })
			t.Cleanup(release)
			var calls atomic.Int32
			origin := fillTestGetter(func(ctx context.Context, _ store.Partition, _ store.ContentKey) (CacheResult, Blob, error) {
				if calls.Add(1) == 1 {
					close(entered)
					// Deliberately model I/O that does not stop immediately on cancel.
					<-getGate
					if err := ctx.Err(); err != nil {
						return CacheMiss, nil, err
					}
					return CacheMiss, nil, errors.New("isolated slow attempt failed")
				}
				return CacheHit, NewMemBlob([]byte("payload")), nil
			})
			tier := &stableFillTestTier{&fillTestTier{fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
				<-ctx.Done()
				return ctx.Err()
			}}}
			tc := NewTieredCache(origin, tier)
			t.Cleanup(tc.Close)
			firstCtx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			key := store.ContentKey{3}
			first := startFillTestRead(tc, firstCtx, store.PartitionChunk, key)
			awaitFillTest(t, entered)
			if cancelFirst {
				cancel()
			}
			second := requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload")
			second.Release()
			if got := calls.Load(); got != 2 {
				t.Fatalf("independent attempts = %d, want 2", got)
			}
			release()
			if r := awaitFillTest(t, first); r.err == nil || r.blob != nil {
				t.Fatalf("slow attempt = %+v", r)
			}
			// The older failure cannot replace or retire the newer success.
			requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload").Release()
			if got := calls.Load(); got != 2 {
				t.Fatalf("origin calls after unrelated failure = %d, want 2", got)
			}
		})
	}
}

func TestTieredCachePendingFillPreservesHitMissAndErrorPaths(t *testing.T) {
	var mode atomic.Int32
	injected := errors.New("tier error")
	tier := &stableFillTestTier{&fillTestTier{
		get: func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error) {
			switch mode.Load() {
			case 1:
				return CacheHit, NewMemBlob([]byte("tier hit")), nil
			case 2:
				return CacheHitMiss, nil, nil
			case 3:
				return CacheMiss, nil, injected
			default:
				return CacheMiss, nil, nil
			}
		},
		fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}}
	tc := NewTieredCache(missGetter{}, tier)
	t.Cleanup(tc.Close)
	key := store.ContentKey{4}
	blob := NewMemBlob([]byte("pending"))
	tc.startFill(0, store.PartitionChunk, key, blob)
	blob.Release()

	// Holding the bookkeeping lock proves all three paths bypass it, including
	// the ordinary local-hit fast path. The safety timeout is not a timing test.
	pending := tc.pendingFills[0]
	pending.mu.Lock()
	defer pending.mu.Unlock()
	mode.Store(1)
	requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "tier hit").Release()
	mode.Store(2)
	if r := awaitFillTest(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key)); r.found || r.err != nil || r.blob != nil {
		t.Fatalf("negative hit = %+v", r)
	}
	mode.Store(3)
	if r := awaitFillTest(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key)); !errors.Is(r.err, injected) || r.blob != nil {
		t.Fatalf("tier error = %+v", r)
	}
}

func TestTieredCacheReusedLowerFillStillFillsUpperTier(t *testing.T) {
	var upperFills, lowerFills, origins atomic.Int32
	upper := &stableFillTestTier{&fillTestTier{fill: func(context.Context, store.Partition, store.ContentKey, []byte) error {
		upperFills.Add(1)
		return nil
	}}}
	lower := &stableFillTestTier{&fillTestTier{fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
		lowerFills.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}}}
	origin := fillTestGetter(func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error) {
		origins.Add(1)
		return CacheHit, NewMemBlob([]byte("payload")), nil
	})
	tc := NewTieredCache(origin, upper, lower)
	t.Cleanup(tc.Close)
	key := store.ContentKey{5}
	requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload").Release()
	tc.fillInflight[0].Wait()
	// Upper fill is finished and upper still misses (e.g. after eviction).
	// Lower's pending success must serve the read and refill upper again.
	requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload").Release()
	tc.Close()
	if got := upperFills.Load(); got != 2 {
		t.Fatalf("upper fills = %d, want 2", got)
	}
	if got := lowerFills.Load(); got != 1 {
		t.Fatalf("lower fills = %d, want 1", got)
	}
	if got := origins.Load(); got != 1 {
		t.Fatalf("origin calls = %d, want 1", got)
	}
}

func TestTieredCacheFillReuseSeparatesPartitionsAndKeys(t *testing.T) {
	var calls, fills atomic.Int32
	tier := &stableFillTestTier{&fillTestTier{fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
		fills.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}}}
	payload := func(p store.Partition, key store.ContentKey) string { return fmt.Sprintf("%s/%d", p, key[0]) }
	origin := fillTestGetter(func(_ context.Context, p store.Partition, key store.ContentKey) (CacheResult, Blob, error) {
		calls.Add(1)
		return CacheHit, NewMemBlob([]byte(payload(p, key))), nil
	})
	tc := NewTieredCache(origin, tier)
	t.Cleanup(tc.Close)
	keys := []fillKey{
		{partition: store.PartitionChunk, key: store.ContentKey{6}},
		{partition: store.PartitionManifest, key: store.ContentKey{6}},
		{partition: store.PartitionChunk, key: store.ContentKey{7}},
	}
	for range 2 {
		for _, k := range keys {
			requireFillTestPayload(t, startFillTestRead(tc, t.Context(), k.partition, k.key), payload(k.partition, k.key)).Release()
		}
	}
	tc.Close()
	if got := calls.Load(); got != int32(len(keys)) {
		t.Fatalf("origin calls = %d, want %d", got, len(keys))
	}
	if got := fills.Load(); got != int32(len(keys)) {
		t.Fatalf("fills = %d, want %d", got, len(keys))
	}
}

func TestTieredCacheDoesNotReuseUnstableFillTarget(t *testing.T) {
	var route, calls atomic.Int32
	route.Store(1)
	started := make(chan int32, 2)
	// No StableFillTarget marker: the same tier can route later fills elsewhere.
	tier := &fillTestTier{fill: func(ctx context.Context, _ store.Partition, _ store.ContentKey, _ []byte) error {
		started <- route.Load()
		<-ctx.Done()
		return ctx.Err()
	}}
	origin := fillTestGetter(func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error) {
		calls.Add(1)
		return CacheHit, NewMemBlob([]byte("payload")), nil
	})
	tc := NewTieredCache(origin, NewTierAdapter(tier, 1))
	t.Cleanup(tc.Close)
	key := store.ContentKey{8}
	requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload").Release()
	if got := awaitFillTest(t, started); got != 1 {
		t.Fatalf("first route = %d", got)
	}
	route.Store(2)
	requireFillTestPayload(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key), "payload").Release()
	if got := awaitFillTest(t, started); got != 2 {
		t.Fatalf("second route = %d", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("origin calls for changing destination = %d, want 2", got)
	}
}

func TestTieredCacheDoesNotRetainMissesOrReadErrors(t *testing.T) {
	var calls atomic.Int32
	injected := errors.New("origin error")
	origin := fillTestGetter(func(context.Context, store.Partition, store.ContentKey) (CacheResult, Blob, error) {
		if calls.Add(1) == 1 {
			return CacheMiss, nil, nil
		}
		return CacheMiss, nil, injected
	})
	tier := &stableFillTestTier{&fillTestTier{fill: func(context.Context, store.Partition, store.ContentKey, []byte) error {
		t.Error("a failed read must not fill")
		return nil
	}}}
	tc := NewTieredCache(origin, tier)
	t.Cleanup(tc.Close)
	key := store.ContentKey{9}
	if r := awaitFillTest(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key)); r.found || r.err != nil || r.blob != nil {
		t.Fatalf("initial miss = %+v", r)
	}
	for range 2 {
		if r := awaitFillTest(t, startFillTestRead(tc, t.Context(), store.PartitionChunk, key)); !errors.Is(r.err, injected) || r.blob != nil {
			t.Fatalf("origin error = %+v", r)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("origin calls = %d, want 3", got)
	}
}
