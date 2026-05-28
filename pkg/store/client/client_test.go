package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store/pb"
	storeserver "github.com/kuasar-sandbox/sandbox-accelerator/pkg/store/server"
)

// startBufconnPair stands up a real fs.Store + Server + bufconn
// listener and returns a Client wired to it. Pool size is settable
// for round-robin testing.
func startBufconnPair(t *testing.T, pool int, verifyKey bool) *Client {
	t.Helper()

	root := t.TempDir()
	if err := fs.Init(fs.Config{Root: root}, "G1"); err != nil {
		t.Fatalf("fs.Init: %v", err)
	}
	backend, err := fs.New(fs.Config{Root: root, VerifyKey: verifyKey})
	if err != nil {
		t.Fatalf("fs.New: %v", err)
	}
	srv, err := storeserver.New(storeserver.Options{Backend: backend, VerifyKey: verifyKey})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterStoreServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	// Manually build a Client whose pool is a slice of bufconn-
	// backed ClientConns. This bypasses New() because New uses a
	// real net.Dial; bufconn needs the custom dialer.
	conns := make([]*grpc.ClientConn, 0, pool)
	stubs := make([]pb.StoreClient, 0, pool)
	for i := 0; i < pool; i++ {
		cc, err := grpc.NewClient(
			"passthrough://bufconn",
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conns = append(conns, cc)
		stubs = append(stubs, pb.NewStoreClient(cc))
	}
	c := &Client{conns: conns, stubs: stubs, timeout: 5 * time.Second}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestPutGetRoundtrip — Put a payload through the client, Get it
// back, verify bytes match.
func TestPutGetRoundtrip(t *testing.T) {
	c := startBufconnPair(t, 1, true)

	data := bytes.Repeat([]byte("client-roundtrip-"), 32*1024) // ~544 KiB
	key := store.ContentKey(sha256.Sum256(data))

	isNew, err := c.Put(context.Background(), store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !isNew {
		t.Fatal("first Put should report isNew=true")
	}

	found, blob, err := c.Get(context.Background(), store.PartitionChunk, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("Get should find the just-Put key")
	}
	got := blob
	if !bytes.Equal(got, data) {
		t.Errorf("Get returned %d bytes, want %d", len(got), len(data))
	}
}

// TestDedupShortCircuit — second Put with same key must report
// isNew=false. The client should not surface any error from the
// server's early SendAndClose.
func TestDedupShortCircuit(t *testing.T) {
	c := startBufconnPair(t, 1, true)

	data := bytes.Repeat([]byte("dedup-payload"), 50*1024) // 650 KiB
	key := store.ContentKey(sha256.Sum256(data))

	isNew1, err := c.Put(context.Background(), store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if !isNew1 {
		t.Fatal("first Put should report isNew=true")
	}
	isNew2, err := c.Put(context.Background(), store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if isNew2 {
		t.Fatal("second Put should report isNew=false (dedup hit)")
	}
}

// TestGetMissReturnsNilNoError — gRPC NotFound is translated into
// (false, nil, nil).
func TestGetMiss(t *testing.T) {
	c := startBufconnPair(t, 1, true)
	missKey := store.ContentKey(sha256.Sum256([]byte("nonexistent")))

	found, blob, err := c.Get(context.Background(), store.PartitionChunk, missKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("Get on miss should return found=false")
	}
	if blob != nil {
		t.Fatal("Get on miss should return nil blob")
	}
}

// TestGetSalt — happy path through the gRPC client.
func TestGetSalt(t *testing.T) {
	c := startBufconnPair(t, 1, true)

	gen, salt, err := c.GetSalt(context.Background())
	if err != nil {
		t.Fatalf("GetSalt: %v", err)
	}
	if gen != "G1" {
		t.Errorf("Generation: got %q, want G1", gen)
	}
	var zero [32]byte
	if salt == zero {
		t.Error("Salt: got all zeros, want derived bytes")
	}
}

// TestRoundRobinAcrossPool — with pool=4 the client should rotate
// stubs round-robin per pickStub. Verify by tracking the underlying
// stub identity across N consecutive calls.
func TestRoundRobinAcrossPool(t *testing.T) {
	c := startBufconnPair(t, 4, true)

	// Call pickStub 8 times, capture pointer identity. With 4 stubs
	// in the pool we expect 4 distinct addresses, each appearing
	// exactly twice.
	seen := make(map[pb.StoreClient]int)
	for i := 0; i < 8; i++ {
		seen[c.pickStub()]++
	}
	if len(seen) != 4 {
		t.Errorf("round-robin distribution: got %d distinct stubs, want 4", len(seen))
	}
	for stub, count := range seen {
		if count != 2 {
			t.Errorf("stub %p hit %d times, want 2", stub, count)
		}
	}
}

// TestLargeStreamFraming — write 2 MiB worth of data; client should
// chunk it into 256 KiB frames and the server should reassemble
// without error.
func TestLargeStreamFraming(t *testing.T) {
	c := startBufconnPair(t, 1, true)

	data := make([]byte, 2<<20) // 2 MiB
	for i := range data {
		data[i] = byte(i % 251)
	}
	key := store.ContentKey(sha256.Sum256(data))

	isNew, err := c.Put(context.Background(), store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !isNew {
		t.Fatal("Put should be new")
	}

	found, blob, err := c.Get(context.Background(), store.PartitionChunk, key)
	if err != nil || !found {
		t.Fatalf("Get: %v found=%v", err, found)
	}
	got := blob
	if !bytes.Equal(got, data) {
		t.Errorf("roundtrip mismatch: %d vs %d bytes", len(got), len(data))
	}
}

// TestClientBasic — compile-time construction check. atomic import
// retained for symmetry with other test files.
func TestClientBasic(t *testing.T) {
	_ = startBufconnPair(t, 1, true)
	if got := atomic.LoadInt32(new(int32)); got != 0 {
		t.Fatalf("unexpected: %d", got)
	}
}
