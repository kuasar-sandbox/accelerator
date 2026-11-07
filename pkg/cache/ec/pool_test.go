package ec

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type poolTestShard struct {
	value    []byte
	closes   atomic.Int32
	onClose  func()
	closeErr error
}

func (s *poolTestShard) GetShard(ctx context.Context, _ store.Partition, _ store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	if err := ctx.Err(); err != nil {
		return cache.CacheMiss, nil, err
	}
	if s.value == nil {
		return cache.CacheMiss, nil, nil
	}
	return cache.CacheHit, cache.NewMemBlob(append([]byte(nil), s.value...)), nil
}

func (*poolTestShard) FillShard(context.Context, store.Partition, store.ContentKey, []byte) error {
	return nil
}

func (s *poolTestShard) Close() error {
	s.closes.Add(1)
	if s.onClose != nil {
		s.onClose()
	}
	return s.closeErr
}

type poolGetResult struct {
	client client.ShardCloser
	err    error
}

func startPoolGet(pp *peerPool, endpoint string) <-chan poolGetResult {
	done := make(chan poolGetResult, 1)
	go func() {
		c, err := pp.Get(endpoint)
		done <- poolGetResult{client: c, err: err}
	}()
	return done
}

func awaitPoolEvent[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("operation was blocked by an unrelated peer operation")
		var zero T
		return zero
	}
}

func requirePoolClient(t *testing.T, result poolGetResult, want client.ShardCloser) {
	t.Helper()
	if result.err != nil || result.client != want {
		t.Fatalf("Get client=%v err=%v, want client=%v", result.client, result.err, want)
	}
}

func TestPeerPoolSlowInitializationDoesNotBlockOtherPeers(t *testing.T) {
	pp := newPeerPool(2, time.Second, cache.DefaultPool)
	blocked, ready, cold := &poolTestShard{}, &poolTestShard{}, &poolTestShard{}
	entered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	pp.newShard = func(endpoint string, _ client.Options) (client.ShardCloser, error) {
		switch endpoint {
		case "blocked":
			close(entered)
			<-gate
			return blocked, nil
		case "ready":
			return ready, nil
		default:
			return cold, nil
		}
	}
	t.Cleanup(func() { release(); _ = pp.Close() })
	c, err := pp.Get("ready")
	requirePoolClient(t, poolGetResult{c, err}, ready)
	blockedGet := startPoolGet(pp, "blocked")
	awaitPoolEvent(t, entered)

	// Neither an existing client nor a different cold peer may wait for
	// the blocked peer's initialization to complete.
	requirePoolClient(t, awaitPoolEvent(t, startPoolGet(pp, "ready")), ready)
	requirePoolClient(t, awaitPoolEvent(t, startPoolGet(pp, "cold")), cold)
	if got := len(pp.Peers()); got != 2 {
		t.Fatalf("ready peer count=%d, want 2 while the third is initializing", got)
	}
	release()
	requirePoolClient(t, awaitPoolEvent(t, blockedGet), blocked)
}

func TestPeerPoolSharesSameEndpointInitialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pp := newPeerPool(2, time.Second, cache.DefaultPool)
		shard := &poolTestShard{}
		entered := make(chan struct{})
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		var calls atomic.Int32
		pp.newShard = func(string, client.Options) (client.ShardCloser, error) {
			if calls.Add(1) == 1 {
				close(entered)
			}
			<-gate
			return shard, nil
		}
		t.Cleanup(func() { release(); _ = pp.Close() })
		const n = 16
		results := make([]<-chan poolGetResult, n)
		results[0] = startPoolGet(pp, "peer")
		awaitPoolEvent(t, entered)
		for i := 1; i < n; i++ {
			results[i] = startPoolGet(pp, "peer")
		}
		synctest.Wait() // All callers have joined the unfinished initialization.
		if got := calls.Load(); got != 1 {
			t.Fatalf("concurrent client initializations=%d, want 1", got)
		}
		release()
		for _, result := range results {
			requirePoolClient(t, awaitPoolEvent(t, result), shard)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("client initializations=%d, want 1", got)
		}
		if err := pp.Close(); err != nil {
			t.Fatal(err)
		}
		if got := shard.closes.Load(); got != 1 {
			t.Fatalf("client closes=%d, want 1", got)
		}
	})
}

func TestPeerPoolFailedInitializationCanRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pp := newPeerPool(2, time.Second, cache.DefaultPool)
		wantErr := errors.New("dial failed")
		shard := &poolTestShard{}
		entered := make(chan struct{})
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		var calls atomic.Int32
		pp.newShard = func(string, client.Options) (client.ShardCloser, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-gate
				return nil, wantErr
			}
			return shard, nil
		}
		t.Cleanup(func() { release(); _ = pp.Close() })
		first := startPoolGet(pp, "peer")
		awaitPoolEvent(t, entered)
		const n = 8
		waiters := make([]<-chan poolGetResult, n)
		for i := range waiters {
			waiters[i] = startPoolGet(pp, "peer")
		}
		synctest.Wait() // The waiting callers must not inherit the first error.
		release()
		if result := awaitPoolEvent(t, first); result.client != nil || !errors.Is(result.err, wantErr) {
			t.Fatalf("failed Get client=%v err=%v", result.client, result.err)
		}
		for _, waiter := range waiters {
			requirePoolClient(t, awaitPoolEvent(t, waiter), shard)
		}
		c, err := pp.Get("peer")
		requirePoolClient(t, poolGetResult{c, err}, shard)
		if got := calls.Load(); got != 2 {
			t.Fatalf("initializations=%d, want 2", got)
		}
	})
}

func TestPeerPoolCloseJoinsInitialization(t *testing.T) {
	pp := newPeerPool(2, time.Second, cache.DefaultPool)
	closeStarted := make(chan struct{})
	ready := &poolTestShard{onClose: func() { close(closeStarted) }}
	wantCloseErr := errors.New("late client close failed")
	late := &poolTestShard{closeErr: wantCloseErr}
	entered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	pp.newShard = func(endpoint string, _ client.Options) (client.ShardCloser, error) {
		if endpoint == "ready" {
			return ready, nil
		}
		close(entered)
		<-gate
		return late, nil
	}
	t.Cleanup(func() { release(); _ = pp.Close() })
	if _, err := pp.Get("ready"); err != nil {
		t.Fatal(err)
	}
	lateGet := startPoolGet(pp, "late")
	awaitPoolEvent(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- pp.Close() }()
	awaitPoolEvent(t, closeStarted)
	if c, err := pp.Get("after-close"); c != nil || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Get after Close client=%v err=%v, want net.ErrClosed", c, err)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the initializing client was owned: %v", err)
	default:
	}
	release()
	if result := awaitPoolEvent(t, lateGet); result.client != nil || !errors.Is(result.err, net.ErrClosed) {
		t.Fatalf("late initialization result=%+v, want net.ErrClosed", result)
	}
	if err := awaitPoolEvent(t, closed); !errors.Is(err, wantCloseErr) {
		t.Fatalf("Close error=%v, want %v", err, wantCloseErr)
	}
	if len(pp.Peers()) != 0 || late.closes.Load() != 1 || ready.closes.Load() != 1 {
		t.Fatalf("Close left clients: peers=%d late closes=%d ready closes=%d", len(pp.Peers()), late.closes.Load(), ready.closes.Load())
	}
}

func TestPeerPoolReconcileInvalidatesOldInitialization(t *testing.T) {
	pp := newPeerPool(2, time.Second, cache.DefaultPool)
	old, replacement := &poolTestShard{}, &poolTestShard{}
	entered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	var calls atomic.Int32
	pp.newShard = func(string, client.Options) (client.ShardCloser, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-gate
			return old, nil
		}
		return replacement, nil
	}
	t.Cleanup(func() { release(); _ = pp.Close() })
	oldGet := startPoolGet(pp, "peer")
	awaitPoolEvent(t, entered)
	reconciled := make(chan struct{})
	go func() { pp.Reconcile(nil); close(reconciled) }()
	awaitPoolEvent(t, reconciled)

	// Re-adding the same endpoint must not let the original initialization
	// overwrite or delete the replacement published by this new lookup.
	pp.Reconcile([]Peer{{ID: "peer", Endpoint: "peer"}})
	requirePoolClient(t, awaitPoolEvent(t, startPoolGet(pp, "peer")), replacement)
	release()
	if result := awaitPoolEvent(t, oldGet); result.client != nil || !errors.Is(result.err, net.ErrClosed) {
		t.Fatalf("removed initialization result=%+v, want net.ErrClosed", result)
	}
	c, err := pp.Get("peer")
	requirePoolClient(t, poolGetResult{c, err}, replacement)
	if old.closes.Load() != 1 || replacement.closes.Load() != 0 {
		t.Fatalf("old closes=%d replacement closes=%d", old.closes.Load(), replacement.closes.Load())
	}
}

func TestPeerPoolReconcilePreservesKeptInitialization(t *testing.T) {
	pp := newPeerPool(2, time.Second, cache.DefaultPool)
	shard := &poolTestShard{}
	entered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	pp.newShard = func(string, client.Options) (client.ShardCloser, error) {
		close(entered)
		<-gate
		return shard, nil
	}
	t.Cleanup(func() { release(); _ = pp.Close() })
	get := startPoolGet(pp, "kept")
	awaitPoolEvent(t, entered)
	reconciled := make(chan struct{})
	go func() {
		pp.Reconcile([]Peer{{ID: "kept", Endpoint: "kept"}, {ID: "new", Endpoint: "new"}})
		close(reconciled)
	}()
	awaitPoolEvent(t, reconciled)
	release()
	requirePoolClient(t, awaitPoolEvent(t, get), shard)
	if shard.closes.Load() != 0 {
		t.Fatal("unchanged member's initializing client was closed")
	}
}

func TestPeerPoolReconcileCloseDoesNotBlockLookups(t *testing.T) {
	pp := newPeerPool(2, time.Second, cache.DefaultPool)
	closeEntered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	retired := &poolTestShard{onClose: func() { close(closeEntered); <-gate }}
	closeStarted := make(chan struct{})
	ready := &poolTestShard{onClose: func() { close(closeStarted) }}
	pp.newShard = func(endpoint string, _ client.Options) (client.ShardCloser, error) {
		if endpoint == "retired" {
			return retired, nil
		}
		return ready, nil
	}
	t.Cleanup(func() { release(); _ = pp.Close() })
	if _, err := pp.Get("retired"); err != nil {
		t.Fatal(err)
	}
	if _, err := pp.Get("ready"); err != nil {
		t.Fatal(err)
	}
	reconciled := make(chan struct{})
	go func() {
		pp.Reconcile([]Peer{{ID: "ready", Endpoint: "ready"}})
		close(reconciled)
	}()
	awaitPoolEvent(t, closeEntered)
	requirePoolClient(t, awaitPoolEvent(t, startPoolGet(pp, "ready")), ready)
	closed := make(chan error, 1)
	go func() { closed <- pp.Close() }()
	awaitPoolEvent(t, closeStarted)
	select {
	case err := <-closed:
		t.Fatalf("Close did not join a client retired by Reconcile: %v", err)
	default:
	}
	release()
	awaitPoolEvent(t, reconciled)
	if err := awaitPoolEvent(t, closed); err != nil {
		t.Fatal(err)
	}
	if retired.closes.Load() != 1 || ready.closes.Load() != 1 {
		t.Fatalf("retired closes=%d ready closes=%d", retired.closes.Load(), ready.closes.Load())
	}
}

func TestECGetQuorumDoesNotWaitForPeerInitialization(t *testing.T) {
	peers := []Peer{
		{ID: "a", Endpoint: "a"}, {ID: "b", Endpoint: "b"},
		{ID: "c", Endpoint: "c"}, {ID: "d", Endpoint: "d"}, {ID: "e", Endpoint: "e"},
	}
	handle, err := New(Config{Cluster: ClusterConfig{DataShards: 4, ParityShards: 1, Peers: peers}})
	if err != nil {
		t.Fatal(err)
	}
	tier := handle.(*impl)
	value := bytes.Repeat([]byte("independent-peers"), 1024)
	shards := make([][]byte, len(peers))
	for i := range shards {
		shards[i] = make([]byte, ShardBufferSize(len(value), 4))
	}
	if err := tier.enc.EncodePrefixed(value, shards); err != nil {
		t.Fatal(err)
	}
	key := store.ContentKey{1, 3, 5}
	routed, err := tier.router.LocatePeers(key, len(peers))
	if err != nil {
		t.Fatal(err)
	}
	clients := make(map[string]*poolTestShard, len(peers))
	for i, peer := range routed {
		clients[peer.Endpoint] = &poolTestShard{value: shards[i]}
	}
	slowEndpoint := routed[len(routed)-1].Endpoint
	entered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	tier.pool.newShard = func(endpoint string, _ client.Options) (client.ShardCloser, error) {
		if endpoint == slowEndpoint {
			close(entered)
			<-gate
		}
		return clients[endpoint], nil
	}
	var readers sync.WaitGroup
	t.Cleanup(func() { release(); readers.Wait(); _ = tier.Close() })
	slowGet := startPoolGet(tier.pool, slowEndpoint)
	awaitPoolEvent(t, entered)
	type readResult struct {
		result cache.CacheResult
		value  []byte
		err    error
	}
	done := make(chan readResult, 1)
	readers.Add(1)
	go func() {
		defer readers.Done()
		result, blob, err := tier.Get(context.Background(), store.PartitionChunk, key)
		var data []byte
		if blob != nil {
			data = append([]byte(nil), blob.Bytes()...)
			blob.Release()
		}
		done <- readResult{result: result, value: data, err: err}
	}()
	result := awaitPoolEvent(t, done)
	if result.err != nil || result.result != cache.CacheHit || !bytes.Equal(result.value, value) {
		t.Fatalf("quorum Get result=%v bytes=%d err=%v", result.result, len(result.value), result.err)
	}
	// The four healthy peers have already supplied the object while the
	// fifth peer still has not returned a client.
	release()
	requirePoolClient(t, awaitPoolEvent(t, slowGet), clients[slowEndpoint])
}
