package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Each peer owns its listener, connections and workers, including shutdown.
func startReadPeer(t *testing.T, path string, reply func(net.Conn, *wire.Request) bool) func() {
	t.Helper()
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	conns := map[net.Conn]bool{}
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[c] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				defer func() { mu.Lock(); delete(conns, c); mu.Unlock() }()
				r := bufio.NewReader(c)
				for {
					req, err := wire.ReadRequest(r)
					if err != nil {
						return
					}
					keep := reply(c, req)
					req.Release()
					if !keep {
						return
					}
				}
			}()
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			l.Close()
			<-accepted
			mu.Lock()
			for c := range conns {
				c.Close()
			}
			mu.Unlock()
			wg.Wait()
		})
	}
	t.Cleanup(stop)
	return stop
}
func TestGetAndShardSingleAttemptHalfFrameAndCancellation(t *testing.T) {
	for _, shard := range []bool{false, true} {
		t.Run(map[bool]string{false: "object", true: "shard"}[shard], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cache.sock")
			payload := bytes.Repeat([]byte{0x42}, 4096)
			var calls atomic.Int32
			blocked := make(chan struct{})
			startReadPeer(t, path, func(c net.Conn, req *wire.Request) bool {
				n := calls.Add(1)
				if n == 1 {
					var frame bytes.Buffer
					wire.WriteResponse(&frame, &wire.Response{Status: wire.StatusHit, Value: cache.NewMemBlob(bytes.Repeat([]byte{0x19}, 4096))})
					c.Write(frame.Bytes()[:100])
					return false
				}
				if n == 2 {
					wire.WriteResponse(c, &wire.Response{Status: wire.StatusCancelled})
					return true
				}
				if n == 3 {
					close(blocked)
					var b [1]byte
					c.Read(b[:])
					return false
				}
				return wire.WriteResponse(c, &wire.Response{Status: wire.StatusHit, Value: cache.NewMemBlob(payload)}) == nil
			})
			var get func(context.Context) (cache.CacheResult, cache.Blob, error)
			if shard {
				c, err := NewShard(path, Options{Pool: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				get = func(ctx context.Context) (cache.CacheResult, cache.Blob, error) {
					return c.GetShard(ctx, store.PartitionChunk, store.ContentKey{})
				}
			} else {
				c, err := NewGetter(path, Options{Pool: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				get = func(ctx context.Context) (cache.CacheResult, cache.Blob, error) {
					return c.Get(ctx, store.PartitionChunk, store.ContentKey{})
				}
			}
			_, blob, err := get(context.Background())
			if err == nil || readerr.IsPermanent(err) || blob != nil || calls.Load() != 1 {
				t.Fatalf("half-frame attempt: %v blob=%v calls=%d", err, blob, calls.Load())
			}
			_, blob, err = get(context.Background())
			if !errors.Is(err, ErrCancelled) || readerr.IsPermanent(err) || blob != nil || calls.Load() != 2 {
				t.Fatalf("server cancellation: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, b, err := get(ctx)
				if b != nil {
					b.Release()
				}
				done <- err
			}()
			<-blocked
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("canceled read succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("socket operation did not join")
			}
			_, blob, err = get(context.Background())
			if err != nil || blob == nil {
				t.Fatal(err)
			}
			defer blob.Release()
			if !bytes.Equal(blob.Bytes(), payload) || calls.Load() != 4 {
				t.Fatal("new attempt spliced old response or replayed a read")
			}
		})
	}
}
func TestWholePoolOldConnectionsAndSameEndpointRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.sock")
	var calls atomic.Int32
	reply := func(c net.Conn, _ *wire.Request) bool {
		calls.Add(1)
		return wire.WriteResponse(c, &wire.Response{Status: wire.StatusHit, Value: cache.NewMemBlob([]byte("recovered"))}) == nil
	}
	stop := startReadPeer(t, path, reply)
	c, err := NewGetter(path, Options{Pool: 4, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stop() // every idle pooled connection belongs to the stopped peer
	for range 4 {
		_, blob, err := c.Get(context.Background(), store.PartitionChunk, store.ContentKey{})
		if err == nil || blob != nil {
			t.Fatalf("stale connection succeeded: %v", err)
		}
	}
	// Stay down across many independent attempts; no health tick is necessary
	// for recovery, and unsuccessful acquisition does not consume quota.
	for range 32 {
		_, _, err := c.Get(context.Background(), store.PartitionChunk, store.ContentKey{})
		if err == nil {
			t.Fatal("offline read succeeded")
		}
	}
	pool := c.(*impl).getPool
	if n := pool.current.Load(); n < 0 || n > 4 {
		t.Fatalf("offline capacity=%d", n)
	}
	startReadPeer(t, path, reply)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, blob, err := c.Get(ctx, store.PartitionChunk, store.ContentKey{})
	if err != nil {
		t.Fatal(err)
	}
	defer blob.Release()
	if string(blob.Bytes()) != "recovered" || calls.Load() != 1 {
		t.Fatalf("endpoint recovery calls=%d", calls.Load())
	}
}
