package redisstore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type fakeAction struct {
	wait     <-chan struct{}
	response []byte
	signal   chan<- struct{}
	close    bool
}

type fakeRedis struct {
	t        *testing.T
	listener net.Listener
	path     string
	done     chan struct{}
	wg       sync.WaitGroup

	mu     sync.RWMutex
	values map[string][]byte
	hook   func(command string, key, value []byte) fakeAction

	accepted atomic.Int64
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	path := filepath.Join(t.TempDir(), "redis.sock")
	lis, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRedis{
		t:        t,
		listener: lis,
		path:     path,
		done:     make(chan struct{}),
		values:   make(map[string][]byte),
	}
	f.wg.Add(1)
	go f.accept()
	t.Cleanup(f.close)
	return f
}

func (f *fakeRedis) accept() {
	defer f.wg.Done()
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			select {
			case <-f.done:
				return
			default:
				f.t.Errorf("accept: %v", err)
				return
			}
		}
		f.accepted.Add(1)
		f.wg.Add(1)
		go f.serve(conn)
	}
}

func (f *fakeRedis) serve(conn net.Conn) {
	defer f.wg.Done()
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		parts, err := readRequest(r)
		if err != nil {
			if err != io.EOF && !isClosedConn(err) {
				f.t.Errorf("read request: %v", err)
			}
			return
		}
		command := string(bytes.ToUpper(parts[0]))
		key := parts[1]
		var value []byte
		if len(parts) == 3 {
			value = parts[2]
			f.mu.Lock()
			f.values[string(key)] = append([]byte(nil), value...)
			f.mu.Unlock()
		}

		f.mu.RLock()
		hook := f.hook
		f.mu.RUnlock()
		var action fakeAction
		if hook != nil {
			action = hook(command, key, value)
		}
		if action.signal != nil {
			select {
			case action.signal <- struct{}{}:
			default:
			}
		}
		if action.wait != nil {
			select {
			case <-action.wait:
			case <-f.done:
				return
			}
		}

		response := action.response
		if response == nil {
			switch command {
			case "GET":
				f.mu.RLock()
				stored, ok := f.values[string(key)]
				value = append([]byte(nil), stored...)
				f.mu.RUnlock()
				if !ok {
					response = []byte("$-1\r\n")
				} else {
					response = []byte(fmt.Sprintf("$%d\r\n", len(value)))
					response = append(response, value...)
					response = append(response, '\r', '\n')
				}
			case "SET":
				response = []byte("+OK\r\n")
			default:
				response = []byte("-ERR unsupported\r\n")
			}
		}
		if _, err := conn.Write(response); err != nil {
			return
		}
		if action.close {
			return
		}
	}
}

func (f *fakeRedis) setHook(hook func(command string, key, value []byte) fakeAction) {
	f.mu.Lock()
	f.hook = hook
	f.mu.Unlock()
}

func (f *fakeRedis) close() {
	select {
	case <-f.done:
		return
	default:
		close(f.done)
		_ = f.listener.Close()
		f.wg.Wait()
		_ = os.Remove(f.path)
	}
}

func readRequest(r *bufio.Reader) ([][]byte, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' || line[len(line)-2:] != "\r\n" {
		return nil, fmt.Errorf("invalid array header %q", line)
	}
	n, err := strconv.Atoi(line[1 : len(line)-2])
	if err != nil || (n != 2 && n != 3) {
		return nil, fmt.Errorf("invalid array length %q", line)
	}
	parts := make([][]byte, n)
	for i := range parts {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 4 || line[0] != '$' || line[len(line)-2:] != "\r\n" {
			return nil, fmt.Errorf("invalid bulk header %q", line)
		}
		size, err := strconv.Atoi(line[1 : len(line)-2])
		if err != nil || size < 0 {
			return nil, fmt.Errorf("invalid bulk size %q", line)
		}
		parts[i] = make([]byte, size)
		if _, err := io.ReadFull(r, parts[i]); err != nil {
			return nil, err
		}
		var suffix [2]byte
		if _, err := io.ReadFull(r, suffix[:]); err != nil {
			return nil, err
		}
		if suffix != [2]byte{'\r', '\n'} {
			return nil, fmt.Errorf("bulk missing CRLF")
		}
	}
	return parts, nil
}

func isClosedConn(err error) bool {
	return err != nil && (bytes.Contains([]byte(err.Error()), []byte("closed")) || bytes.Contains([]byte(err.Error()), []byte("reset")))
}

func openTestStore(t *testing.T, server *fakeRedis, pool cache.BlobPool) *Store {
	t.Helper()
	s, err := Open(runtime.RedisConfig{
		Socket:  server.path,
		GetPool: 1,
		SetPool: 1,
		Timeout: "2s",
	}, pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func testKey(seed byte) store.ContentKey {
	var key store.ContentKey
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

func requireValue(t *testing.T, result cache.CacheResult, blob cache.Blob, err error, want []byte) {
	t.Helper()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if result != cache.CacheHit || blob == nil {
		t.Fatalf("result=%v blob=%v, want hit", result, blob)
	}
	defer blob.Release()
	if !bytes.Equal(blob.Bytes(), want) {
		t.Fatalf("value=%q, want %q", blob.Bytes(), want)
	}
}

func TestStoreObjectShardAndPartitionIsolation(t *testing.T) {
	server := newFakeRedis(t)
	s := openTestStore(t, server, cache.NewPool(1024))
	ctx := context.Background()
	key := testKey(1)

	if err := s.Fill(ctx, store.PartitionChunk, key, []byte("object")); err != nil {
		t.Fatal(err)
	}
	if err := s.FillShard(ctx, store.PartitionChunk, key, []byte{3, 5, 's', 'h', 'a', 'r', 'd'}); err != nil {
		t.Fatal(err)
	}
	if err := s.Fill(ctx, store.PartitionManifest, key, []byte("manifest")); err != nil {
		t.Fatal(err)
	}

	result, blob, err := s.Get(ctx, store.PartitionChunk, key)
	requireValue(t, result, blob, err, []byte("object"))
	result, blob, err = s.GetShard(ctx, store.PartitionChunk, key)
	requireValue(t, result, blob, err, []byte{3, 5, 's', 'h', 'a', 'r', 'd'})
	result, blob, err = s.Get(ctx, store.PartitionManifest, key)
	requireValue(t, result, blob, err, []byte("manifest"))

	result, blob, err = s.Get(ctx, store.PartitionBlob, key)
	if err != nil || result != cache.CacheMiss || blob != nil {
		t.Fatalf("blob partition: result=%v blob=%v err=%v, want confirmed miss", result, blob, err)
	}
	stats := s.Stats()
	if stats.GetHits != 3 || stats.GetMisses != 1 || stats.Sets != 3 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestStoreServerErrorIsNotMissAndDoesNotReconnect(t *testing.T) {
	server := newFakeRedis(t)
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "GET" {
			return fakeAction{response: []byte("-ERR backend unavailable\r\n")}
		}
		return fakeAction{}
	})
	s := openTestStore(t, server, nil)

	result, blob, err := s.Get(context.Background(), store.PartitionChunk, testKey(2))
	if err == nil || result != cache.CacheMiss || blob != nil {
		t.Fatalf("result=%v blob=%v err=%v, want backend error", result, blob, err)
	}
	if got := s.Stats(); got.BackendErrors != 1 || got.GetMisses != 0 || got.Reconnects != 0 {
		t.Fatalf("unexpected stats: %+v", got)
	}
	if server.accepted.Load() != 2 {
		t.Fatalf("accepted=%d, want initial two connections", server.accepted.Load())
	}
}

func TestStoreCancelDrainsAndReusesConnection(t *testing.T) {
	server := newFakeRedis(t)
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "GET" {
			return fakeAction{signal: received, wait: release, response: []byte("$4\r\nlate\r\n")}
		}
		return fakeAction{}
	})
	pool := &trackingPool{}
	s := openTestStore(t, server, pool)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := s.Get(ctx, store.PartitionChunk, testKey(3))
		done <- err
	}()
	<-received
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("cancel error=%v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Get did not return promptly on cancellation")
	}
	if got := s.Stats().Draining; got != 1 {
		t.Fatalf("draining=%d, want 1", got)
	}
	close(release)
	waitFor(t, time.Second, func() bool { return s.Stats().Draining == 0 })
	if pool.released.Load() != 1 {
		t.Fatalf("late blob releases=%d, want 1", pool.released.Load())
	}
	stats := s.Stats()
	if stats.LateBytesDrained != 4 || stats.Reconnects != 0 {
		t.Fatalf("unexpected drain stats: %+v", stats)
	}
	if server.accepted.Load() != 2 {
		t.Fatalf("cancel reconnected UDS: accepted=%d", server.accepted.Load())
	}

	server.setHook(nil)
	result, blob, err := s.Get(context.Background(), store.PartitionChunk, testKey(4))
	if err != nil || result != cache.CacheMiss || blob != nil {
		t.Fatalf("connection not reusable: result=%v blob=%v err=%v", result, blob, err)
	}
}

func TestStoreSetCancellationReleasesPayloadAfterWrite(t *testing.T) {
	server := newFakeRedis(t)
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "SET" {
			return fakeAction{signal: received, wait: release}
		}
		return fakeAction{}
	})
	s := openTestStore(t, server, nil)
	key := testKey(5)
	value := bytes.Repeat([]byte{0x5a}, 128<<10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Fill(ctx, store.PartitionChunk, key, value) }()
	<-received // fake server has consumed and copied the complete SET payload
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("cancel error=%v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Fill did not return after its socket write completed")
	}
	for i := range value {
		value[i] = 0
	}
	close(release)
	waitFor(t, time.Second, func() bool { return s.Stats().Draining == 0 })

	server.setHook(nil)
	result, blob, err := s.Get(context.Background(), store.PartitionChunk, key)
	if err != nil || result != cache.CacheHit {
		t.Fatalf("get written value: result=%v err=%v", result, err)
	}
	defer blob.Release()
	if !bytes.Equal(blob.Bytes(), bytes.Repeat([]byte{0x5a}, 128<<10)) {
		t.Fatal("SET retained caller payload after Fill returned")
	}
}

func TestStorePreCancelledSetIsNotSubmitted(t *testing.T) {
	server := newFakeRedis(t)
	received := make(chan struct{}, 1)
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "SET" {
			return fakeAction{signal: received}
		}
		return fakeAction{}
	})
	s := openTestStore(t, server, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Fill(ctx, store.PartitionChunk, testKey(12), []byte("must-not-be-written"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Fill error=%v, want context.Canceled", err)
	}
	select {
	case <-received:
		t.Fatal("pre-cancelled SET reached Redis")
	case <-time.After(20 * time.Millisecond):
	}
	stats := s.Stats()
	if stats.Sets != 0 || stats.SetInflight != 0 || stats.Cancelled != 1 {
		t.Fatalf("unexpected stats after pre-cancelled SET: %+v", stats)
	}
}

func TestStoreSetServerErrorDoesNotReconnect(t *testing.T) {
	server := newFakeRedis(t)
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "SET" {
			return fakeAction{response: []byte("-ERR read only\r\n")}
		}
		return fakeAction{}
	})
	s := openTestStore(t, server, nil)

	err := s.Fill(context.Background(), store.PartitionChunk, testKey(13), []byte("rejected"))
	if err == nil {
		t.Fatal("SET -ERR unexpectedly succeeded")
	}
	stats := s.Stats()
	if stats.Sets != 0 || stats.BackendErrors != 1 || stats.Reconnects != 0 {
		t.Fatalf("unexpected stats after SET -ERR: %+v", stats)
	}
	if server.accepted.Load() != 2 {
		t.Fatalf("SET -ERR reconnected UDS: accepted=%d", server.accepted.Load())
	}

	server.setHook(nil)
	if err := s.Fill(context.Background(), store.PartitionChunk, testKey(14), []byte("accepted")); err != nil {
		t.Fatalf("SET after -ERR did not reuse connection: %v", err)
	}
}

func TestStoreProtocolErrorReconnects(t *testing.T) {
	server := newFakeRedis(t)
	var malformed atomic.Bool
	malformed.Store(true)
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "GET" && malformed.CompareAndSwap(true, false) {
			return fakeAction{response: []byte("!bad\r\n")}
		}
		return fakeAction{}
	})
	s := openTestStore(t, server, nil)

	_, _, err := s.Get(context.Background(), store.PartitionChunk, testKey(6))
	if err == nil {
		t.Fatal("malformed response unexpectedly succeeded")
	}
	waitFor(t, time.Second, func() bool { return s.Stats().Reconnects == 1 })
	result, blob, err := s.Get(context.Background(), store.PartitionChunk, testKey(7))
	if err != nil || result != cache.CacheMiss || blob != nil {
		t.Fatalf("reconnected get: result=%v blob=%v err=%v", result, blob, err)
	}
	stats := s.Stats()
	if stats.ProtocolErrors != 1 || server.accepted.Load() != 3 {
		t.Fatalf("unexpected reconnect state: stats=%+v accepted=%d", stats, server.accepted.Load())
	}
}

func TestStoreTruncatedBulkReleasesBlobAndReconnects(t *testing.T) {
	server := newFakeRedis(t)
	var truncate atomic.Bool
	truncate.Store(true)
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "GET" && truncate.CompareAndSwap(true, false) {
			return fakeAction{response: []byte("$4\r\nxx"), close: true}
		}
		return fakeAction{}
	})
	pool := &trackingPool{}
	s := openTestStore(t, server, pool)

	result, blob, err := s.Get(context.Background(), store.PartitionChunk, testKey(10))
	if err == nil || result != cache.CacheMiss || blob != nil {
		t.Fatalf("truncated GET: result=%v blob=%v err=%v", result, blob, err)
	}
	waitFor(t, time.Second, func() bool { return s.Stats().Reconnects == 1 })
	if pool.allocated.Load() != 1 || pool.released.Load() != 1 {
		t.Fatalf("blob lifecycle: allocated=%d released=%d", pool.allocated.Load(), pool.released.Load())
	}

	result, blob, err = s.Get(context.Background(), store.PartitionChunk, testKey(11))
	if err != nil || result != cache.CacheMiss || blob != nil {
		t.Fatalf("GET after reconnect: result=%v blob=%v err=%v", result, blob, err)
	}
}

func TestStoreCancelCompletionRaceDoesNotLeakBlob(t *testing.T) {
	server := newFakeRedis(t)
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "GET" {
			return fakeAction{response: []byte("$1\r\nx\r\n")}
		}
		return fakeAction{}
	})
	pool := &trackingPool{}
	s := openTestStore(t, server, pool)

	for i := 0; i < 500; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancelled := make(chan struct{})
		go func() {
			cancel()
			close(cancelled)
		}()
		result, blob, err := s.Get(ctx, store.PartitionChunk, testKey(byte(i)))
		<-cancelled
		switch {
		case err == nil && result == cache.CacheHit && blob != nil:
			blob.Release()
		case errors.Is(err, context.Canceled) && result == cache.CacheMiss && blob == nil:
		default:
			t.Fatalf("iteration %d: result=%v blob=%v err=%v", i, result, blob, err)
		}
	}
	waitFor(t, time.Second, func() bool {
		stats := s.Stats()
		return stats.GetInflight == 0 && stats.Draining == 0
	})
	if got, want := pool.released.Load(), pool.allocated.Load(); got != want {
		t.Fatalf("blob leak: allocated=%d released=%d", want, got)
	}
	if got := s.Stats().Reconnects; got != 0 {
		t.Fatalf("normal cancellation caused %d reconnects", got)
	}
}

func TestStorePoolWaitHonorsContext(t *testing.T) {
	server := newFakeRedis(t)
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	server.setHook(func(command string, key, value []byte) fakeAction {
		if command == "GET" {
			return fakeAction{signal: received, wait: release}
		}
		return fakeAction{}
	})
	s := openTestStore(t, server, nil)
	first := make(chan error, 1)
	go func() {
		_, _, err := s.Get(context.Background(), store.PartitionChunk, testKey(8))
		first <- err
	}()
	<-received

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, _, err := s.Get(ctx, store.PartitionChunk, testKey(9))
	if err != context.DeadlineExceeded {
		t.Fatalf("pool wait error=%v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestStoreProbeUsesDedicatedConnectionAndCountersStayClean(t *testing.T) {
	server := newFakeRedis(t)
	s := openTestStore(t, server, nil)
	before := s.Stats()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Probe(ctx); err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	if after.GetHits != before.GetHits || after.GetMisses != before.GetMisses || after.GetInflight != 0 {
		t.Fatalf("probe changed data-plane counters: before=%+v after=%+v", before, after)
	}
	if server.accepted.Load() != 3 {
		t.Fatalf("accepted=%d, want GET + SET + dedicated probe", server.accepted.Load())
	}
}

func TestWorkerExitDropsInstalledConnection(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	s := &Store{stop: stop}
	pool := &workerPool{kind: getPool, store: s, available: make(chan *worker, 1)}
	w := &worker{store: s, pool: pool, ops: make(chan *operation)}
	workerConn, peerConn := net.Pipe()
	defer peerConn.Close()
	w.installConn(workerConn)
	s.wg.Add(1)
	go w.run()
	s.wg.Wait()
	if got := s.getConnected.Load(); got != 0 {
		t.Fatalf("connected workers=%d, want 0", got)
	}
	requirePeerClosed(t, peerConn)
}

func TestReconnectDoesNotInstallConnectionAfterStop(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	s := &Store{stop: stop}
	pool := &workerPool{kind: getPool, store: s}
	w := &worker{store: s, pool: pool}
	workerConn, peerConn := net.Pipe()
	defer peerConn.Close()
	if w.installReconnected(workerConn) {
		t.Fatal("connection installed after stop")
	}
	if got := s.getConnected.Load(); got != 0 {
		t.Fatalf("connected workers=%d, want 0", got)
	}
	requirePeerClosed(t, peerConn)
}

func requirePeerClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		var one [1]byte
		_, err := conn.Read(one[:])
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("connection remained open: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection remained open")
	}
}

type trackingPool struct {
	allocated atomic.Int64
	released  atomic.Int64
}

func (p *trackingPool) Alloc(size int) ([]byte, cache.Blob) {
	p.allocated.Add(1)
	data := make([]byte, size)
	return data, &trackingBlob{data: data, released: &p.released}
}

type trackingBlob struct {
	data     []byte
	released *atomic.Int64
	done     atomic.Bool
}

func (b *trackingBlob) Bytes() []byte { return b.data }

func (b *trackingBlob) Clone() cache.Blob {
	panic("Clone is not used by this test pool")
}

func (b *trackingBlob) Release() {
	if b.done.CompareAndSwap(false, true) {
		b.released.Add(1)
		b.data = nil
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
