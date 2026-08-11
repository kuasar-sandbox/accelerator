package client

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
)

// TestPoolConnPingClearsDeadline is a regression test for the stale-deadline
// bug (issue #58): a successful Ping used to leave its deadline installed on
// the connection, so a later operation reusing the connection after the
// deadline had passed failed immediately with an I/O timeout even though the
// connection was healthy.
func TestPoolConnPingClearsDeadline(t *testing.T) {
	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		br := bufio.NewReader(serverRaw)

		// Answer the Ping.
		req, err := wire.ReadRequest(br)
		if err != nil {
			return
		}
		req.Release()
		if err := wire.WriteResponse(serverRaw, &wire.Response{Status: wire.StatusHit}); err != nil {
			return
		}

		// Answer the follow-up op issued after the ping deadline expired.
		req2, err := wire.ReadRequest(br)
		if err != nil {
			return
		}
		req2.Release()
		_ = wire.WriteResponse(serverRaw, &wire.Response{Status: wire.StatusHit})
	}()

	pc := &PoolConn{conn: wire.NewConn(clientRaw)}

	const pingTimeout = 20 * time.Millisecond
	if err := pc.Ping(pingTimeout); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// Let the ping deadline expire while the connection sits idle, exactly
	// as it would between healthLoop's ping and the next caller acquiring
	// the connection from the pool.
	time.Sleep(5 * pingTimeout)

	writeErrCh := make(chan error, 1)
	go func() {
		writeErrCh <- pc.WriteRequest(&wire.Request{Opcode: wire.OpcodePing})
	}()

	select {
	case err := <-writeErrCh:
		if err != nil {
			t.Fatalf("post-ping write failed, deadline was not cleared: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("post-ping write did not complete in time")
	}

	if _, err := pc.ReadResponse(nil); err != nil {
		t.Fatalf("post-ping read failed, deadline was not cleared: %v", err)
	}

	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("server did not observe the follow-up request in time")
	}
}

// TestSetOpDeadlineClearsStaleDeadlineWithoutBudget is a regression test for
// the second stale-deadline path in issue #58: when SetOpDeadline resolves
// to no deadline (no per-op timeout budget and no context deadline), it must
// clear any deadline left over from a previous operation on the same pooled
// connection instead of silently leaving it in place.
func TestSetOpDeadlineClearsStaleDeadlineWithoutBudget(t *testing.T) {
	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()

	pc := &PoolConn{conn: wire.NewConn(clientRaw)}

	// Simulate a deadline left over from a prior operation (e.g. a health
	// check) that has already expired.
	if err := pc.conn.SetDeadline(time.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	// No per-op timeout budget and no context deadline: must clear the
	// stale deadline rather than leaving the connection permanently timed
	// out.
	pc.SetOpDeadline(context.Background(), 0)

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		br := bufio.NewReader(serverRaw)
		req, err := wire.ReadRequest(br)
		if err != nil {
			return
		}
		req.Release()
		_ = wire.WriteResponse(serverRaw, &wire.Response{Status: wire.StatusHit})
	}()

	writeErrCh := make(chan error, 1)
	go func() {
		writeErrCh <- pc.WriteRequest(&wire.Request{Opcode: wire.OpcodePing})
	}()

	select {
	case err := <-writeErrCh:
		if err != nil {
			t.Fatalf("write failed, stale deadline was not cleared: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write did not complete in time, stale deadline was not cleared")
	}

	if _, err := pc.ReadResponse(nil); err != nil {
		t.Fatalf("read failed, stale deadline was not cleared: %v", err)
	}

	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("server did not observe the request in time")
	}
}
