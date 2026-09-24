package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type blockingGetter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *blockingGetter) Get(context.Context, store.Partition, store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	g.once.Do(func() { close(g.started) })
	<-g.release
	return cache.CacheMiss, nil, nil
}

type singleConnListener struct {
	conn   net.Conn
	closed chan struct{}
	once   sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		c := l.conn
		l.conn = nil
		return c, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *singleConnListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
func (*singleConnListener) Addr() net.Addr { return testAddr("wire-test") }

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

func TestGracefulStopTimeoutAndWaitForCompletion(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	listener := &singleConnListener{conn: serverConn, closed: make(chan struct{})}
	getter := &blockingGetter{started: make(chan struct{}), release: make(chan struct{})}
	s := NewWireServer(NewCacheHandler(roTier{getter}, nil), 0, 0)
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.Serve(listener) }()

	client := wire.NewConn(clientConn)
	if err := client.WriteRequest(&wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSChunk}); err != nil {
		t.Fatal(err)
	}
	<-getter.started

	started := time.Now()
	s.GracefulStop()
	elapsed := time.Since(started)
	if elapsed < 4500*time.Millisecond || elapsed > 7*time.Second {
		t.Fatalf("GracefulStop returned after %v, want legacy five-second bound", elapsed)
	}

	waitDone := make(chan struct{})
	go func() {
		s.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		t.Fatal("Wait returned while the handler was still active")
	default:
	}

	close(getter.release)
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after the handler completed")
	}
	if err := <-serveDone; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Serve: %v", err)
	}
	_ = clientConn.Close()
}

func TestStartConnRejectsAdmissionAfterStop(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	s := NewWireServer(NewCacheHandler(nil, nil), 0, 0)
	s.GracefulStop()
	if s.startConn(serverConn) {
		t.Fatal("connection admitted after stop")
	}
	_ = serverConn.Close()
	_ = clientConn.Close()
	done := make(chan struct{})
	go func() {
		s.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rejected admission left Wait blocked")
	}
}
