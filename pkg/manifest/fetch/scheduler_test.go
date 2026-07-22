package fetch

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func TestRequestSchedulerStateRecovery(t *testing.T) {
	s := &requestScheduler{}

	s.beginOnDemand()
	acquired, changed := s.tryBeginPrefetch()
	if acquired {
		t.Fatal("prefetch acquired while on-demand was active")
	}
	if changed == nil {
		t.Fatal("blocked prefetch did not receive a notification channel")
	}
	s.endOnDemand()
	assertClosed(t, changed)

	acquired, _ = s.tryBeginPrefetch()
	if !acquired {
		t.Fatal("prefetch did not acquire after on-demand drained")
	}
	acquired, changed = s.tryBeginPrefetch()
	if acquired {
		t.Fatal("second prefetch acquired while the token was held")
	}

	// An on-demand request may overlap the already admitted prefetch. Ending
	// the prefetch cannot wake another prefetch until on-demand also drains.
	s.beginOnDemand()
	s.endPrefetch()
	assertOpen(t, changed)
	s.endOnDemand()
	assertClosed(t, changed)

	acquired, _ = s.tryBeginPrefetch()
	if !acquired {
		t.Fatal("prefetch token was not restored")
	}
	s.endPrefetch()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onDemand != 0 || s.prefetch || s.changed != nil {
		t.Fatalf("scheduler did not return to idle state: onDemand=%d prefetch=%v changed=%v", s.onDemand, s.prefetch, s.changed != nil)
	}
}

func TestScheduledGettersShareScheduler(t *testing.T) {
	inner := newGateGetter()
	client := newScheduledCacheClient(inner)
	onDemand := client.OnDemandGetter()
	prefetch := client.PrefetchGetter()

	demandDone := callGetter(onDemand, context.Background(), keyForTest(1))
	demandCall := receiveCall(t, inner.entered)
	if demandCall.key != keyForTest(1) {
		t.Fatalf("first inner call key = %x, want demand key", demandCall.key)
	}

	prefetchDone := callGetter(prefetch, context.Background(), keyForTest(2))
	waitForChangeChannel(t, client.scheduler)
	assertNoCall(t, inner.entered)

	close(demandCall.release)
	receiveResult(t, demandDone)
	prefetchCall := receiveCall(t, inner.entered)
	if prefetchCall.key != keyForTest(2) {
		t.Fatalf("second inner call key = %x, want prefetch key", prefetchCall.key)
	}
	close(prefetchCall.release)
	receiveResult(t, prefetchDone)

	assertSchedulerIdle(t, client.scheduler)
}

func TestOnDemandOverlapsAdmittedPrefetch(t *testing.T) {
	inner := newGateGetter()
	client := newScheduledCacheClient(inner)
	prefetch := client.PrefetchGetter()
	onDemand := client.OnDemandGetter()

	prefetchDone := callGetter(prefetch, context.Background(), keyForTest(3))
	prefetchCall := receiveCall(t, inner.entered)

	demandDone := callGetter(onDemand, context.Background(), keyForTest(4))
	demandCall := receiveCall(t, inner.entered)
	if demandCall.key != keyForTest(4) {
		t.Fatalf("on-demand did not bypass admitted prefetch: got key %x", demandCall.key)
	}

	close(demandCall.release)
	receiveResult(t, demandDone)
	close(prefetchCall.release)
	receiveResult(t, prefetchDone)
	assertSchedulerIdle(t, client.scheduler)
}

func TestOnlyOnePrefetchCallsInner(t *testing.T) {
	inner := newGateGetter()
	client := newScheduledCacheClient(inner)
	prefetchA := client.PrefetchGetter()
	prefetchB := client.PrefetchGetter()

	firstDone := callGetter(prefetchA, context.Background(), keyForTest(12))
	firstCall := receiveCall(t, inner.entered)

	secondDone := callGetter(prefetchB, context.Background(), keyForTest(13))
	waitForChangeChannel(t, client.scheduler)
	assertNoCall(t, inner.entered)

	close(firstCall.release)
	receiveResult(t, firstDone)
	secondCall := receiveCall(t, inner.entered)
	if secondCall.key != keyForTest(13) {
		t.Fatalf("second admitted prefetch key = %x, want %x", secondCall.key, keyForTest(13))
	}
	close(secondCall.release)
	receiveResult(t, secondDone)
	assertSchedulerIdle(t, client.scheduler)
}

func TestPrefetchWaitCancellationRestoresState(t *testing.T) {
	inner := newGateGetter()
	client := newScheduledCacheClient(inner)
	onDemand := client.OnDemandGetter()
	prefetch := client.PrefetchGetter()

	demandDone := callGetter(onDemand, context.Background(), keyForTest(5))
	demandCall := receiveCall(t, inner.entered)

	ctx, cancel := context.WithCancel(context.Background())
	prefetchDone := callGetter(prefetch, ctx, keyForTest(6))
	waitForChangeChannel(t, client.scheduler)
	cancel()
	result := receiveResult(t, prefetchDone)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("prefetch cancellation error = %v, want context.Canceled", result.err)
	}
	if result.blob != nil || result.cacheResult != cache.CacheMiss {
		t.Fatalf("cancelled prefetch returned result=%v blob=%v", result.cacheResult, result.blob)
	}
	assertNoCall(t, inner.entered)

	close(demandCall.release)
	receiveResult(t, demandDone)

	// A cancellation must not retain the token or poison the next request.
	nextDone := callGetter(prefetch, context.Background(), keyForTest(7))
	nextCall := receiveCall(t, inner.entered)
	close(nextCall.release)
	receiveResult(t, nextDone)
	assertSchedulerIdle(t, client.scheduler)
}

func TestScheduledCacheClientsAreIsolated(t *testing.T) {
	inner := newGateGetter()
	clientA := newScheduledCacheClient(inner)
	clientB := newScheduledCacheClient(inner)

	demandADone := callGetter(clientA.OnDemandGetter(), context.Background(), keyForTest(8))
	demandA := receiveCall(t, inner.entered)

	prefetchADone := callGetter(clientA.PrefetchGetter(), context.Background(), keyForTest(9))
	waitForChangeChannel(t, clientA.scheduler)

	prefetchBDone := callGetter(clientB.PrefetchGetter(), context.Background(), keyForTest(10))
	prefetchB := receiveCall(t, inner.entered)
	if prefetchB.key != keyForTest(10) {
		t.Fatalf("client B prefetch key = %x, want %x", prefetchB.key, keyForTest(10))
	}
	close(prefetchB.release)
	receiveResult(t, prefetchBDone)

	close(demandA.release)
	receiveResult(t, demandADone)
	prefetchA := receiveCall(t, inner.entered)
	close(prefetchA.release)
	receiveResult(t, prefetchADone)

	assertSchedulerIdle(t, clientA.scheduler)
	assertSchedulerIdle(t, clientB.scheduler)
}

func TestScheduledGetterPassesThroughAndRestoresAfterError(t *testing.T) {
	sentinelErr := errors.New("sentinel")
	sentinelBlob := &observedBlob{}
	inner := &returnGetter{
		result: cache.CacheHitMiss,
		blob:   sentinelBlob,
		err:    sentinelErr,
	}
	client := newScheduledCacheClient(inner)

	for _, getter := range []cache.Getter{client.OnDemandGetter(), client.PrefetchGetter()} {
		result, blob, err := getter.Get(context.Background(), store.PartitionChunk, keyForTest(11))
		if result != cache.CacheHitMiss || blob != sentinelBlob || !errors.Is(err, sentinelErr) {
			t.Fatalf("wrapper changed inner return: result=%v blob=%p err=%v", result, blob, err)
		}
		assertSchedulerIdle(t, client.scheduler)
	}

	if got := inner.calls.Load(); got != 2 {
		t.Fatalf("inner calls = %d, want 2", got)
	}
	if sentinelBlob.bytes.Load() != 0 || sentinelBlob.clones.Load() != 0 || sentinelBlob.releases.Load() != 0 {
		t.Fatalf("wrapper touched blob: bytes=%d clones=%d releases=%d", sentinelBlob.bytes.Load(), sentinelBlob.clones.Load(), sentinelBlob.releases.Load())
	}
}

type gateGetter struct {
	entered chan *gateCall
}

type gateCall struct {
	key     store.ContentKey
	release chan struct{}
}

func newGateGetter() *gateGetter {
	return &gateGetter{entered: make(chan *gateCall, 16)}
}

func (g *gateGetter) Get(ctx context.Context, _ store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	call := &gateCall{key: key, release: make(chan struct{})}
	select {
	case g.entered <- call:
	case <-ctx.Done():
		return cache.CacheMiss, nil, ctx.Err()
	}
	select {
	case <-call.release:
		return cache.CacheMiss, nil, nil
	case <-ctx.Done():
		return cache.CacheMiss, nil, ctx.Err()
	}
}

type returnGetter struct {
	result cache.CacheResult
	blob   cache.Blob
	err    error
	calls  atomic.Int64
}

func (g *returnGetter) Get(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.calls.Add(1)
	return g.result, g.blob, g.err
}

type observedBlob struct {
	bytes    atomic.Int64
	clones   atomic.Int64
	releases atomic.Int64
}

func (b *observedBlob) Bytes() []byte {
	b.bytes.Add(1)
	return nil
}

func (b *observedBlob) Clone() cache.Blob {
	b.clones.Add(1)
	return b
}

func (b *observedBlob) Release() {
	b.releases.Add(1)
}

type getterResult struct {
	cacheResult cache.CacheResult
	blob        cache.Blob
	err         error
}

func callGetter(getter cache.Getter, ctx context.Context, key store.ContentKey) <-chan getterResult {
	done := make(chan getterResult, 1)
	go func() {
		result, blob, err := getter.Get(ctx, store.PartitionChunk, key)
		done <- getterResult{cacheResult: result, blob: blob, err: err}
	}()
	return done
}

func receiveCall(t *testing.T, calls <-chan *gateCall) *gateCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for inner Get")
		return nil
	}
}

func receiveResult(t *testing.T, results <-chan getterResult) getterResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Getter result")
		return getterResult{}
	}
}

func waitForChangeChannel(t *testing.T, s *requestScheduler) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		waiting := s.changed != nil
		s.mu.Unlock()
		if waiting {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for prefetch admission wait")
		default:
			runtime.Gosched()
		}
	}
}

func assertSchedulerIdle(t *testing.T, s *requestScheduler) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onDemand != 0 || s.prefetch {
		t.Fatalf("scheduler is not idle: onDemand=%d prefetch=%v", s.onDemand, s.prefetch)
	}
}

func assertNoCall(t *testing.T, calls <-chan *gateCall) {
	t.Helper()
	select {
	case call := <-calls:
		t.Fatalf("unexpected inner Get for key %x", call.key)
	default:
	}
}

func assertClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		t.Fatal("notification channel is still open")
	}
}

func assertOpen(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("notification channel closed before admission could become available")
	default:
	}
}

func keyForTest(v byte) store.ContentKey {
	var key store.ContentKey
	key[0] = v
	return key
}
