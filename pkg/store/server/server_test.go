package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

// startBufconnServer wires a Server to a bufconn listener and
// returns a connected gRPC client. Cleans up via t.Cleanup.
func startBufconnServer(t *testing.T, opts Options) (pb.StoreClient, *Server) {
	t.Helper()
	srv, err := New(opts)
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
	conn, err := grpc.NewClient(
		"passthrough://bufconn",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewStoreClient(conn), srv
}

func newFSStore(t *testing.T, generation string) *fs.Store {
	t.Helper()
	root := t.TempDir()
	if err := fs.Init(fs.Config{Root: root}, generation); err != nil {
		t.Fatalf("fs.Init: %v", err)
	}
	s, err := fs.New(fs.Config{Root: root, VerifyKey: true})
	if err != nil {
		t.Fatalf("fs.New: %v", err)
	}
	return s
}

// TestGetSalt — happy path: server returns an opaque 32-byte salt.
func TestGetSalt(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	resp, err := cli.GetSalt(context.Background(), &pb.GetSaltRequest{})
	if err != nil {
		t.Fatalf("GetSalt: %v", err)
	}
	if len(resp.GetSalt()) != 32 {
		t.Errorf("Salt: got %d bytes, want 32", len(resp.GetSalt()))
	}
}

func TestOpaqueSaltIsStableWithinAndIsolatedAcrossWriteDomains(t *testing.T) {
	a, err := New(Options{Backend: newFSStore(t, "G1")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Options{Backend: newFSStore(t, "G1")})
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Options{Backend: newFSStore(t, "G2")})
	if err != nil {
		t.Fatal(err)
	}
	if a.salt != b.salt {
		t.Fatal("the same store write domain produced different salts")
	}
	if a.salt == c.salt {
		t.Fatal("different store write domains produced the same salt")
	}
}

// streamPut is a helper for tests: take a key + payload, drive the
// gRPC client-streaming Put RPC end-to-end with 256 KiB framing,
// return the response.
func streamPut(t *testing.T, cli pb.StoreClient, partition pb.Partition, key store.ContentKey, data []byte) (*pb.PutResponse, error) {
	t.Helper()
	stream, err := cli.Put(context.Background())
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pb.PutRequest{
		Body: &pb.PutRequest_Header{
			Header: &pb.PutHeader{Partition: partition, Key: key[:]},
		},
	}); err != nil {
		return nil, err
	}
	const frame = 256 * 1024
	for off := 0; off < len(data); off += frame {
		end := off + frame
		if end > len(data) {
			end = len(data)
		}
		err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Data{Data: data[off:end]}})
		if err != nil {
			if err == io.EOF {
				break // server closed early
			}
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}

// streamGet drains a Get into a single byte slice. Returns nil
// bytes + nil error on miss (the caller checks the gRPC status of
// the first error explicitly).
func streamGet(t *testing.T, cli pb.StoreClient, partition pb.Partition, key store.ContentKey) ([]byte, error) {
	t.Helper()
	stream, err := cli.Get(context.Background(), &pb.GetRequest{Partition: partition, Key: key[:]})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return buf.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		buf.Write(msg.GetData())
	}
}

// TestPutGetRoundtrip — write 384 KiB through a client streaming
// Put, read it back through server streaming Get. Bytes should
// match.
func TestPutGetRoundtrip(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	data := bytes.Repeat([]byte("ABCD"), 96*1024) // 384 KiB
	key := store.ContentKey(sha256.Sum256(data))

	resp, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !resp.GetIsNew() {
		t.Errorf("Put.IsNew: got false, want true")
	}

	got, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Get returned %d bytes, want %d", len(got), len(data))
	}
}

// TestDedupShortCircuit — second Put with the same key must return
// IsNew=false. The server should SendAndClose right after the
// header, so the client may not even finish streaming data.
func TestDedupShortCircuit(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	data := bytes.Repeat([]byte("dedup"), 100*1024) // 500 KiB
	key := store.ContentKey(sha256.Sum256(data))

	// First Put writes the file.
	resp1, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, data)
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if !resp1.GetIsNew() {
		t.Fatal("first Put should be IsNew=true")
	}

	// Second Put — server should short-circuit.
	resp2, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, key, data)
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if resp2.GetIsNew() {
		t.Errorf("dedup hit should report IsNew=false, got true")
	}
}

// TestVerifyKeyMismatch — client claims a key that doesn't match the
// streamed bytes. With verify on, server must reject and the temp
// file should be cleaned up.
func TestVerifyKeyMismatch(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	data := []byte("real bytes")
	wrongKey := store.ContentKey(sha256.Sum256([]byte("not these bytes")))

	_, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, wrongKey, data)
	if err == nil {
		t.Fatal("Put with mismatched key should fail")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("Put error code: got %v, want InvalidArgument", st.Code())
	}

	// Verify the wrong-key path is NOT readable (since commit
	// failed) — Get should return NotFound.
	_, err = streamGet(t, cli, pb.Partition_PARTITION_CHUNK, wrongKey)
	if err == nil {
		t.Fatal("Get on rejected key should miss")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("Get error code: got %v, want NotFound", st.Code())
	}
}

// TestVerifyOff — VerifyKey=false bypasses the server-side hash
// check; whatever key the client claims is honoured (dangerous, but
// useful for trusted-loader scenarios). The mismatch combination
// should still write successfully and be retrievable by the claimed
// key.
func TestVerifyOff(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: false})

	data := []byte("real bytes")
	wrongKey := store.ContentKey(sha256.Sum256([]byte("not these bytes")))

	resp, err := streamPut(t, cli, pb.Partition_PARTITION_CHUNK, wrongKey, data)
	if err != nil {
		t.Fatalf("Put with verify off: %v", err)
	}
	if !resp.GetIsNew() {
		t.Fatal("Put with verify off should still write")
	}

	got, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, wrongKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Get returned %d bytes, want %d", len(got), len(data))
	}
}

// TestGetMiss — Get on an unknown key returns gRPC NotFound, no
// data frames.
func TestGetMiss(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	missKey := store.ContentKey(sha256.Sum256([]byte("nonexistent")))
	_, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, missKey)
	if err == nil {
		t.Fatal("Get on miss should return an error")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("error code: got %v, want NotFound", st.Code())
	}
}

// TestPutHeaderAfterDataRejected — protocol enforcement: a second
// PutHeader mid-stream should be rejected with InvalidArgument.
func TestPutHeaderAfterDataRejected(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	data := []byte("payload")
	key := store.ContentKey(sha256.Sum256(data))

	stream, err := cli.Put(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// Send valid header.
	if err := stream.Send(&pb.PutRequest{
		Body: &pb.PutRequest_Header{
			Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_CHUNK, Key: key[:]},
		},
	}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	// Send some data.
	if err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Data{Data: data}}); err != nil {
		// EOF here means server already short-circuited (e.g. dedup);
		// not a header-after-data scenario. Ignore.
		_ = err
	}
	// Try to inject another header mid-stream.
	_ = stream.Send(&pb.PutRequest{
		Body: &pb.PutRequest_Header{Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_CHUNK, Key: key[:]}},
	})
	_, err = stream.CloseAndRecv()
	if err == nil {
		t.Fatal("header-after-data should fail")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("error code: got %v, want InvalidArgument", st.Code())
	}
}

// TestServerInterfaceCheck — fs.Store satisfies our Backend
// interface (compile-time guard via this assertion + a runtime
// no-op check).
func TestServerInterfaceCheck(t *testing.T) {
	var b Backend = newFSStore(t, "G1")
	if got := atomic.LoadInt32(new(int32)); got != 0 {
		// purely to silence "unused package" diagnostics — atomic is
		// imported only here for symmetry with other test files.
		t.Fatalf("unexpected: %d", got)
	}
	_ = b
}
