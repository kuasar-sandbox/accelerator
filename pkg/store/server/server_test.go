package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"net"
	"reflect"
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
	if opts.Generations == nil {
		opts.Generations = func() []store.Generation { return []store.Generation{"G1"} }
	}
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

func newFSStore(t *testing.T, _ string) *fs.Store {
	t.Helper()
	root := t.TempDir()
	s, err := fs.New(fs.Config{Root: root})
	if err != nil {
		t.Fatalf("fs.New: %v", err)
	}
	return s
}

// TestAdmitWrite returns the newest generation and its opaque salt.
func TestAdmitWrite(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	resp, err := cli.AdmitWrite(context.Background(), &pb.AdmitWriteRequest{})
	if err != nil {
		t.Fatalf("AdmitWrite: %v", err)
	}
	if resp.GetGeneration() != "G1" {
		t.Errorf("Generation = %q, want G1", resp.GetGeneration())
	}
	if len(resp.GetSalt()) != 32 {
		t.Errorf("Salt: got %d bytes, want 32", len(resp.GetSalt()))
	}
}

func TestAdmitWriteForCurrentGeneration(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{
		Backend:     newFSStore(t, "G1"),
		Generations: func() []store.Generation { return []store.Generation{"G1", "G2"} },
		VerifyKey:   true,
	})

	resp, err := cli.AdmitWrite(context.Background(), &pb.AdmitWriteRequest{Generation: "G1"})
	if err != nil {
		t.Fatalf("AdmitWrite(G1): %v", err)
	}
	wantSalt, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetGeneration() != "G1" || !bytes.Equal(resp.GetSalt(), wantSalt[:]) {
		t.Fatalf("admission = %q/%x, want G1/%x", resp.GetGeneration(), resp.GetSalt(), wantSalt)
	}
}

func TestAdmitWriteForRemovedAndInvalidGeneration(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{
		Backend:     newFSStore(t, "G2"),
		Generations: func() []store.Generation { return []store.Generation{"G2"} },
		VerifyKey:   true,
	})

	for _, tc := range []struct {
		generation string
		code       codes.Code
	}{
		{generation: "G1", code: codes.FailedPrecondition},
		{generation: "bad/generation", code: codes.InvalidArgument},
	} {
		_, err := cli.AdmitWrite(context.Background(), &pb.AdmitWriteRequest{Generation: tc.generation})
		if status.Code(err) != tc.code {
			t.Fatalf("AdmitWrite(%q) code = %v, want %v", tc.generation, status.Code(err), tc.code)
		}
	}
}

func TestRolloutChangesNewAdmissionWhileListedOldAdmissionCanFinish(t *testing.T) {
	current := []store.Generation{"G1"}
	cli, _ := startBufconnServer(t, Options{
		Backend:     newFSStore(t, "G1"),
		Generations: func() []store.Generation { return current },
		VerifyKey:   true,
	})
	oldAdmission, err := cli.AdmitWrite(context.Background(), &pb.AdmitWriteRequest{})
	if err != nil || oldAdmission.GetGeneration() != "G1" {
		t.Fatalf("old admission = %v, %v", oldAdmission, err)
	}

	current = []store.Generation{"G1", "G2"}
	newAdmission, err := cli.AdmitWrite(context.Background(), &pb.AdmitWriteRequest{})
	if err != nil || newAdmission.GetGeneration() != "G2" {
		t.Fatalf("new admission = %v, %v", newAdmission, err)
	}
	if bytes.Equal(oldAdmission.GetSalt(), newAdmission.GetSalt()) {
		t.Fatal("rollout reused the previous generation salt")
	}

	data := []byte("old admission completes after rollout")
	key := store.ContentKey(sha256.Sum256(data))
	size := uint64(len(data))
	response, err := streamPutHeader(t, cli, &pb.PutHeader{
		Partition:  pb.Partition_PARTITION_CHUNK,
		Key:        key[:],
		Generation: oldAdmission.GetGeneration(),
		Size:       &size,
	}, data)
	if err != nil || !response.GetIsNew() {
		t.Fatalf("listed old admission Put = %v, %v", response, err)
	}
}

func TestOpaqueSaltIsStableWithinAndIsolatedAcrossWriteDomains(t *testing.T) {
	a, err := New(Options{Backend: newFSStore(t, "G1"), Generations: func() []store.Generation { return []store.Generation{"G1"} }})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Options{Backend: newFSStore(t, "G1"), Generations: func() []store.Generation { return []store.Generation{"G1"} }})
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Options{Backend: newFSStore(t, "G2"), Generations: func() []store.Generation { return []store.Generation{"G2"} }})
	if err != nil {
		t.Fatal(err)
	}
	saltG1A, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	saltG1B, err := store.SaltForGeneration("G1")
	if err != nil {
		t.Fatal(err)
	}
	saltG2, err := store.SaltForGeneration("G2")
	if err != nil {
		t.Fatal(err)
	}
	if saltG1A != saltG1B {
		t.Fatal("the same store write domain produced different salts")
	}
	if saltG1A == saltG2 {
		t.Fatal("different store write domains produced the same salt")
	}
	_, _, _ = a, b, c
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
	size := uint64(len(data))
	if err := stream.Send(&pb.PutRequest{
		Body: &pb.PutRequest_Header{
			Header: &pb.PutHeader{Partition: partition, Key: key[:], Generation: "G1", Size: &size},
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

func streamPutHeader(t *testing.T, cli pb.StoreClient, header *pb.PutHeader, frames ...[]byte) (*pb.PutResponse, error) {
	t.Helper()
	stream, err := cli.Put(context.Background())
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Header{Header: header}}); err != nil {
		return nil, err
	}
	for _, frame := range frames {
		if err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Data{Data: frame}}); err != nil && err != io.EOF {
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
// streamed bytes. With verify on, server must reject and its owned
// direct-final file should be cleaned up.
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
			Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_CHUNK, Key: key[:], Generation: "G1", Size: func() *uint64 { n := uint64(len(data)); return &n }()},
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
		Body: &pb.PutRequest_Header{Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_CHUNK, Key: key[:], Generation: "G1"}},
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

func TestPutHeaderOptionalSizeAbsentZeroAndOverflow(t *testing.T) {
	cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})

	emptyKey := store.ContentKey(sha256.Sum256(nil))
	zero := uint64(0)
	response, err := streamPutHeader(t, cli, &pb.PutHeader{
		Partition:  pb.Partition_PARTITION_CHUNK,
		Key:        emptyKey[:],
		Generation: "G1",
		Size:       &zero,
	})
	if err != nil || !response.GetIsNew() {
		t.Fatalf("zero-size Put = %v, %v", response, err)
	}

	data := []byte("size absent")
	key := store.ContentKey(sha256.Sum256(data))
	response, err = streamPutHeader(t, cli, &pb.PutHeader{
		Partition:  pb.Partition_PARTITION_CHUNK,
		Key:        key[:],
		Generation: "G1",
	}, data)
	if err != nil || !response.GetIsNew() {
		t.Fatalf("absent-size Put = %v, %v", response, err)
	}

	overflow := uint64(math.MaxInt64) + 1
	_, err = streamPutHeader(t, cli, &pb.PutHeader{
		Partition:  pb.Partition_PARTITION_CHUNK,
		Key:        key[:],
		Generation: "G1",
		Size:       &overflow,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("overflow status = %v, want InvalidArgument", err)
	}

	_, err = streamPutHeader(t, cli, &pb.PutHeader{
		Partition:  pb.Partition_PARTITION_CHUNK,
		Key:        key[:],
		Generation: "../unsafe",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsafe generation status = %v, want InvalidArgument", err)
	}
}

func TestPutRejectsShortAndOversizedPayloads(t *testing.T) {
	for _, test := range []struct {
		name     string
		data     []byte
		expected uint64
	}{
		{name: "short", data: []byte("abc"), expected: 4},
		{name: "oversized", data: []byte("abcd"), expected: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			cli, _ := startBufconnServer(t, Options{Backend: newFSStore(t, "G1"), VerifyKey: true})
			key := store.ContentKey(sha256.Sum256(test.data))
			_, err := streamPutHeader(t, cli, &pb.PutHeader{
				Partition:  pb.Partition_PARTITION_CHUNK,
				Key:        key[:],
				Generation: "G1",
				Size:       &test.expected,
			}, test.data)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("status = %v, want InvalidArgument", err)
			}
			if _, err := streamGet(t, cli, pb.Partition_PARTITION_CHUNK, key); status.Code(err) != codes.NotFound {
				t.Fatalf("failed Put left readable path: %v", err)
			}
		})
	}
}

func TestPutRejectsAdmissionRemovedFromCurrentList(t *testing.T) {
	current := []store.Generation{"G1"}
	cli, _ := startBufconnServer(t, Options{
		Backend:     newFSStore(t, "G1"),
		Generations: func() []store.Generation { return current },
		VerifyKey:   true,
	})
	admission, err := cli.AdmitWrite(context.Background(), &pb.AdmitWriteRequest{})
	if err != nil || admission.GetGeneration() != "G1" {
		t.Fatalf("AdmitWrite = %v, %v", admission, err)
	}
	current = []store.Generation{"G2"}
	data := []byte("stale admission")
	key := store.ContentKey(sha256.Sum256(data))
	size := uint64(len(data))
	_, err = streamPutHeader(t, cli, &pb.PutHeader{
		Partition:  pb.Partition_PARTITION_CHUNK,
		Key:        key[:],
		Generation: admission.GetGeneration(),
		Size:       &size,
	}, data)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale admission status = %v", err)
	}
}

type lookupBackend struct {
	order []store.Generation
	hit   store.Generation
	data  []byte
}

func (b *lookupBackend) Get(_ context.Context, generation store.Generation, _ store.Partition, _ store.ContentKey) (bool, []byte, error) {
	b.order = append(b.order, generation)
	if generation == b.hit {
		return true, b.data, nil
	}
	return false, nil, nil
}

func (*lookupBackend) Exists(context.Context, store.Generation, store.Partition, store.ContentKey, *int64) (bool, error) {
	return false, nil
}

func (*lookupBackend) OpenPut(store.Generation, store.Partition, store.ContentKey, *int64) (store.PutHandle, error) {
	return nil, errors.New("not implemented")
}

func TestGetObjectSnapshotsOnceAndSearchesNewestFirst(t *testing.T) {
	backend := &lookupBackend{hit: "G1", data: []byte("oldest")}
	loads := 0
	server, err := New(Options{
		Backend: backend,
		Generations: func() []store.Generation {
			loads++
			return []store.Generation{"G1", "G2", "G3"}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	loads = 0 // exclude New's startup validation
	found, data, err := server.GetObject(context.Background(), store.PartitionChunk, store.ContentKey{})
	if err != nil || !found || string(data) != "oldest" {
		t.Fatalf("GetObject = %v, %q, %v", found, data, err)
	}
	if loads != 1 {
		t.Fatalf("generation loads = %d, want 1", loads)
	}
	wantOrder := []store.Generation{"G3", "G2", "G1"}
	if !reflect.DeepEqual(backend.order, wantOrder) {
		t.Fatalf("lookup order = %v, want %v", backend.order, wantOrder)
	}
}
