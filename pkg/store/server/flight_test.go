package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

type gatedObjectBackend struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	once    sync.Once
	data    []byte
}

func (b *gatedObjectBackend) Get(ctx context.Context, _ store.Generation, _ store.Partition, _ store.ContentKey) (bool, []byte, error) {
	b.calls.Add(1)
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return false, nil, ctx.Err()
	}
	return true, b.data, nil
}

func (*gatedObjectBackend) Exists(context.Context, store.Generation, store.Partition, store.ContentKey, *int64) (bool, error) {
	return false, nil
}

func (*gatedObjectBackend) OpenPut(store.Generation, store.Partition, store.ContentKey, *int64) (store.PutHandle, error) {
	return nil, errors.New("not implemented")
}

type cancelOnceObjectBackend struct {
	calls      atomic.Int32
	firstStart chan struct{}
	once       sync.Once
	data       []byte
}

func (b *cancelOnceObjectBackend) Get(ctx context.Context, _ store.Generation, _ store.Partition, _ store.ContentKey) (bool, []byte, error) {
	if b.calls.Add(1) == 1 {
		b.once.Do(func() { close(b.firstStart) })
		<-ctx.Done()
		return false, nil, ctx.Err()
	}
	return true, b.data, nil
}

func (*cancelOnceObjectBackend) Exists(context.Context, store.Generation, store.Partition, store.ContentKey, *int64) (bool, error) {
	return false, nil
}

func (*cancelOnceObjectBackend) OpenPut(store.Generation, store.Partition, store.ContentKey, *int64) (store.PutHandle, error) {
	return nil, errors.New("not implemented")
}

type generationObjectBackend struct {
	mu      sync.Mutex
	calls   []store.Generation
	g1Start chan struct{}
	release chan struct{}
	g1Once  sync.Once
}

func (b *generationObjectBackend) Get(ctx context.Context, generation store.Generation, _ store.Partition, _ store.ContentKey) (bool, []byte, error) {
	b.mu.Lock()
	b.calls = append(b.calls, generation)
	b.mu.Unlock()
	switch generation {
	case "G1":
		b.g1Once.Do(func() { close(b.g1Start) })
		select {
		case <-b.release:
		case <-ctx.Done():
			return false, nil, ctx.Err()
		}
		return true, []byte("from-G1"), nil
	default:
		return true, []byte("from-" + string(generation)), nil
	}
}

func (*generationObjectBackend) Exists(context.Context, store.Generation, store.Partition, store.ContentKey, *int64) (bool, error) {
	return false, nil
}

func (*generationObjectBackend) OpenPut(store.Generation, store.Partition, store.ContentKey, *int64) (store.PutHandle, error) {
	return nil, errors.New("not implemented")
}

func waitForObjectWaiters(t *testing.T, srv *Server, fk objectFlightKey, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		srv.flights.mu.Lock()
		call := srv.flights.calls[fk]
		got := 0
		if call != nil {
			got = call.waiterN
		}
		srv.flights.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("flight waiters = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGetObjectCoalescesConcurrentReads(t *testing.T) {
	const readers = 32
	const payload = "shared-object"
	key := store.ContentKey{1, 2, 3}
	gens := []store.Generation{"G1"}
	fk := objectFlightKey{partition: store.PartitionChunk, key: key, generations: generationsKey(gens)}
	backend := &gatedObjectBackend{
		started: make(chan struct{}),
		release: make(chan struct{}),
		data:    []byte(payload),
	}
	srv, err := New(Options{
		Backend:     backend,
		Generations: func() []store.Generation { return gens },
	})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	type result struct {
		found bool
		data  []byte
		err   error
	}
	results := make(chan result, readers)
	for i := 0; i < readers; i++ {
		go func() {
			<-start
			found, data, err := srv.GetObject(context.Background(), store.PartitionChunk, key)
			results <- result{found: found, data: data, err: err}
		}()
	}
	close(start)
	select {
	case <-backend.started:
	case <-time.After(2 * time.Second):
		t.Fatal("backend Get did not start")
	}
	waitForObjectWaiters(t, srv, fk, readers-1)
	close(backend.release)

	for i := 0; i < readers; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("GetObject error: %v", res.err)
		}
		if !res.found || !bytes.Equal(res.data, []byte(payload)) {
			t.Fatalf("GetObject = %v, %q", res.found, res.data)
		}
	}
	if got := backend.calls.Load(); got != 1 {
		t.Fatalf("backend calls = %d, want 1", got)
	}
}

func TestGetObjectLeaderCancellationLetsWaitersRetry(t *testing.T) {
	key := store.ContentKey{4, 5, 6}
	gens := []store.Generation{"G1"}
	fk := objectFlightKey{partition: store.PartitionChunk, key: key, generations: generationsKey(gens)}
	backend := &cancelOnceObjectBackend{
		firstStart: make(chan struct{}),
		data:       []byte("retry-success"),
	}
	srv, err := New(Options{
		Backend:     backend,
		Generations: func() []store.Generation { return gens },
	})
	if err != nil {
		t.Fatal(err)
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := srv.GetObject(leaderCtx, store.PartitionChunk, key)
		leaderDone <- err
	}()
	select {
	case <-backend.firstStart:
	case <-time.After(2 * time.Second):
		t.Fatal("leader backend Get did not start")
	}

	type result struct {
		found bool
		data  []byte
		err   error
	}
	waiterDone := make(chan result, 1)
	go func() {
		found, data, err := srv.GetObject(context.Background(), store.PartitionChunk, key)
		waiterDone <- result{found: found, data: data, err: err}
	}()
	waitForObjectWaiters(t, srv, fk, 1)

	cancelLeader()
	if err := <-leaderDone; err == nil {
		t.Fatal("canceled leader returned nil error")
	}

	select {
	case res := <-waiterDone:
		if res.err != nil {
			t.Fatalf("waiter GetObject error: %v", res.err)
		}
		if !res.found || !bytes.Equal(res.data, []byte("retry-success")) {
			t.Fatalf("waiter GetObject = %v, %q", res.found, res.data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not retry after leader cancellation")
	}
	if got := backend.calls.Load(); got != 2 {
		t.Fatalf("backend calls = %d, want 2", got)
	}
}

func TestGetObjectDoesNotShareAcrossGenerationSnapshots(t *testing.T) {
	key := store.ContentKey{7, 8, 9}
	var current atomic.Value
	current.Store([]store.Generation{"G1"})
	backend := &generationObjectBackend{
		g1Start: make(chan struct{}),
		release: make(chan struct{}),
	}
	srv, err := New(Options{
		Backend: backend,
		Generations: func() []store.Generation {
			return current.Load().([]store.Generation)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		found bool
		data  []byte
		err   error
	}
	leaderDone := make(chan result, 1)
	go func() {
		found, data, err := srv.GetObject(context.Background(), store.PartitionChunk, key)
		leaderDone <- result{found: found, data: data, err: err}
	}()
	select {
	case <-backend.g1Start:
	case <-time.After(2 * time.Second):
		t.Fatal("G1 backend Get did not start")
	}

	current.Store([]store.Generation{"G1", "G2"})
	waiterDone := make(chan result, 1)
	go func() {
		found, data, err := srv.GetObject(context.Background(), store.PartitionChunk, key)
		waiterDone <- result{found: found, data: data, err: err}
	}()

	select {
	case res := <-waiterDone:
		if res.err != nil || !res.found || !bytes.Equal(res.data, []byte("from-G2")) {
			t.Fatalf("new-generation GetObject = %v, %q, %v", res.found, res.data, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("new-generation GetObject joined the old flight")
	}

	close(backend.release)
	select {
	case res := <-leaderDone:
		if res.err != nil || !res.found || !bytes.Equal(res.data, []byte("from-G1")) {
			t.Fatalf("old-generation GetObject = %v, %q, %v", res.found, res.data, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("old-generation GetObject did not finish")
	}

	backend.mu.Lock()
	gotCalls := append([]store.Generation(nil), backend.calls...)
	backend.mu.Unlock()
	if len(gotCalls) != 2 || gotCalls[0] != "G1" || gotCalls[1] != "G2" {
		t.Fatalf("backend calls = %v, want [G1 G2]", gotCalls)
	}
}

type gatedMissBeforePutBackend struct {
	inner       Backend
	missStarted chan struct{}
	missRelease chan struct{}
	missOnce    sync.Once
	getCalls    atomic.Int32
}

func (b *gatedMissBeforePutBackend) Get(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	callIdx := b.getCalls.Add(1)
	found, data, err := b.inner.Get(ctx, generation, partition, key)
	if callIdx == 1 && !found && err == nil {
		b.missOnce.Do(func() { close(b.missStarted) })
		select {
		case <-b.missRelease:
		case <-ctx.Done():
			return false, nil, ctx.Err()
		}
	}
	return found, data, err
}

func (b *gatedMissBeforePutBackend) Exists(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (bool, error) {
	return b.inner.Exists(ctx, generation, partition, key, expectedSize)
}

func (b *gatedMissBeforePutBackend) OpenPut(generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (store.PutHandle, error) {
	return b.inner.OpenPut(generation, partition, key, expectedSize)
}

func TestGetObjectAfterPutDoesNotJoinPreWriteMissFlight(t *testing.T) {
	payload := []byte("written-while-prewrite-get-inflight")
	key := store.ContentKey(sha256.Sum256(payload))
	backend := &gatedMissBeforePutBackend{
		inner:       newFSStore(t, "G1"),
		missStarted: make(chan struct{}),
		missRelease: make(chan struct{}),
	}
	cli, srv := startBufconnServer(t, Options{
		Backend:     backend,
		Generations: func() []store.Generation { return []store.Generation{"G1"} },
		VerifyKey:   true,
	})

	type getResult struct {
		found bool
		data  []byte
		err   error
	}
	preWriteDone := make(chan getResult, 1)
	go func() {
		found, data, err := srv.GetObject(context.Background(), store.PartitionChunk, key)
		preWriteDone <- getResult{found: found, data: data, err: err}
	}()

	select {
	case <-backend.missStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("pre-write GetObject did not observe initial miss")
	}

	putResp, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, payload)
	if err != nil || !putResp.GetIsNew() {
		t.Fatalf("streamPut = %v, %v", putResp, err)
	}

	postWriteDone := make(chan getResult, 1)
	go func() {
		found, data, err := srv.GetObject(context.Background(), store.PartitionChunk, key)
		postWriteDone <- getResult{found: found, data: data, err: err}
	}()

	select {
	case res := <-postWriteDone:
		if res.err != nil || !res.found || !bytes.Equal(res.data, payload) {
			t.Fatalf("post-Put GetObject = %v, %q, %v; want hit with payload", res.found, res.data, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-Put GetObject joined the blocked pre-write miss flight")
	}

	close(backend.missRelease)
	select {
	case res := <-preWriteDone:
		if res.err != nil || res.found {
			t.Fatalf("pre-Put GetObject = %v, %q, %v; want miss", res.found, res.data, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pre-Put GetObject did not finish")
	}

	dedupResp, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, payload)
	if err != nil || dedupResp.GetIsNew() {
		t.Fatalf("dedup streamPut = %v, %v; want isNew=false", dedupResp, err)
	}
	if got := srv.flights.writeEpoch(store.PartitionChunk, key); got != 2 {
		t.Fatalf("writeEpoch after commit + dedup = %d, want 2", got)
	}
}

