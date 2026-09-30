package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/wire"
	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
)

const runTestTimeout = 5 * time.Second

func runTestAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// startRunTest always stops and joins the service, including assertion failures.
func startRunTest(t *testing.T, cfg Config, opts Options) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done, stopped := make(chan error, 1), make(chan struct{})
	go func() { defer close(stopped); done <- Run(ctx, cfg, opts) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(runTestTimeout):
			t.Error("Run cleanup did not finish")
		}
	})
	return cancel, done
}

func TestRunPutGetGenerationRefreshAndReadOnlyCache(t *testing.T) {
	addr, cacheAddr := runTestAddress(t), runTestAddress(t)
	cfg := runTestConfig(t, addr)
	cfg.CacheListen = cacheAddr
	reload := make(chan struct{}, 1)
	var generations atomic.Value
	generations.Store([]pkgstore.Generation{"G1"})
	cancel, done := startRunTest(t, cfg, Options{Reload: reload, Generations: &GenerationSource{
		Load: func(context.Context) ([]pkgstore.Generation, error) {
			return generations.Load().([]pkgstore.Generation), nil
		},
	}})
	conn := waitForRunTestStore(t, addr)
	_ = conn.Close()
	client, err := storeclient.New(addr, 1, runTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, stop := context.WithTimeout(context.Background(), runTestTimeout)
	defer stop()
	admission, err := client.AdmitWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("store app protocol payload")
	key := pkgstore.ContentKey(sha256.Sum256(data))
	if fresh, err := client.Put(ctx, admission, pkgstore.PartitionBlob, key, data); err != nil || !fresh {
		t.Fatalf("Put fresh=%v error=%v", fresh, err)
	}
	checkGet := func() {
		t.Helper()
		found, got, err := client.Get(ctx, pkgstore.PartitionBlob, key)
		if err != nil || !found || string(got) != string(data) {
			t.Fatalf("Get found=%v data=%q error=%v", found, got, err)
		}
	}
	checkGet()
	wireRaw, err := net.DialTimeout("tcp", cacheAddr, runTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wireRaw.Close() })
	if err := wireRaw.SetDeadline(time.Now().Add(runTestTimeout)); err != nil {
		t.Fatal(err)
	}
	wireConn := wire.NewConn(wireRaw)
	if err := wireConn.WriteRequest(&wire.Request{Opcode: wire.OpcodeObjectGet, Namespace: wire.NSBlob, Hash: key}); err != nil {
		t.Fatal(err)
	}
	resp, err := wireConn.ReadResponse(cache.DefaultPool)
	if err != nil {
		t.Fatal(err)
	}
	var value string
	if resp.Value != nil {
		value = string(resp.Value.Bytes())
		resp.Value.Release()
	}
	if resp.Status != wire.StatusHit || value != string(data) {
		t.Fatalf("cache get status=%d data=%q", resp.Status, value)
	}
	if err := wireConn.WriteRequest(&wire.Request{Opcode: wire.OpcodeObjectPut, Namespace: wire.NSBlob, Hash: key, Value: []byte("no")}); err != nil {
		t.Fatal(err)
	}
	resp, err = wireConn.ReadResponse(cache.DefaultPool)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Value != nil {
		resp.Value.Release()
	}
	if resp.Status != wire.StatusError || resp.ErrMsg != "writes not supported" {
		t.Fatalf("cache put status=%d error=%q", resp.Status, resp.ErrMsg)
	}
	_ = wireConn.Close()
	generations.Store([]pkgstore.Generation{"G1", "G2"})
	reload <- struct{}{}
	for {
		admission, err = client.AdmitWrite(ctx)
		if err == nil && admission.Generation == "G2" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("generation did not refresh: admission=%+v error=%v", admission, err)
		case <-time.After(time.Millisecond):
		}
	}
	checkGet() // data in retained G1 remains readable after the G2 rollout.
	finishRunTest(t, cancel, done, addr)
}

func TestRunCancellationCancelsActivePut(t *testing.T) {
	addr := runTestAddress(t)
	cfg := runTestConfig(t, addr)
	cancel, done := startRunTest(t, cfg, Options{Generations: &GenerationSource{
		Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"G1"}, nil },
	}})
	conn := waitForRunTestStore(t, addr)
	t.Cleanup(func() { _ = conn.Close() })
	ctx, stop := context.WithTimeout(context.Background(), runTestTimeout)
	defer stop()
	stream, err := pb.NewStoreClient(conn).Put(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := sha256.Sum256([]byte("unfinished"))
	if err := stream.Send(&pb.PutRequest{Body: &pb.PutRequest_Header{Header: &pb.PutHeader{Partition: pb.Partition_PARTITION_BLOB, Key: key[:], Generation: "G1"}}}); err != nil {
		t.Fatal(err)
	}
	// OpenPut creates the owned final inode before waiting for more stream data.
	// Observing it proves the server is active, not merely a buffered client Send.
	hexKey := fmt.Sprintf("%x", key)
	partial := filepath.Join(cfg.FS.Root, "blob", "G1", hexKey[:2], hexKey[2:4], hexKey)
	for {
		if _, err := os.Stat(partial); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("server never opened the active Put")
		case <-time.After(time.Millisecond):
		}
	}
	finishRunTest(t, cancel, done, addr)
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("cancelled Put appeared successfully drained")
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Fatalf("cancelled Put retained partial file: %v", err)
	}
}

func runTestConfig(t *testing.T, addr string) Config {
	t.Helper()
	return Config{
		Listen: addr, Backend: "fs", StatsInterval: "off",
		FS: &storeconfig.FSConfig{Root: t.TempDir()},
	}
}

func waitForRunTestStore(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runTestTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial Store: %v", err)
	}
	return conn
}

func requireRunTestRPC(t *testing.T, conn *grpc.ClientConn, generation string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runTestTimeout)
	defer cancel()
	response, err := pb.NewStoreClient(conn).AdmitWrite(ctx, &pb.AdmitWriteRequest{})
	if err != nil {
		t.Fatalf("AdmitWrite: %v", err)
	}
	if got := response.GetGeneration(); got != generation {
		t.Fatalf("AdmitWrite generation = %q, want %q", got, generation)
	}
}

func finishRunTest(t *testing.T, cancel context.CancelFunc, done <-chan error, addr string) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancellation: %v", err)
		}
	case <-time.After(runTestTimeout):
		t.Fatal("Run did not return after cancellation")
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen address was not released: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunDirectFSInjectedGenerationsServesAndReleasesPort(t *testing.T) {
	addr := runTestAddress(t)
	cfg := runTestConfig(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, Options{Generations: &GenerationSource{
			Load: func(context.Context) ([]pkgstore.Generation, error) {
				return []pkgstore.Generation{"injected"}, nil
			},
		}})
	}()
	conn := waitForRunTestStore(t, addr)
	requireRunTestRPC(t, conn, "injected")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	finishRunTest(t, cancel, done, addr)
}

func TestRunBuiltInInlineGenerationsServes(t *testing.T) {
	addr := runTestAddress(t)
	cfg := runTestConfig(t, addr)
	cfg.Generations = &storeconfig.GenerationsConfig{Config: []string{"inline"}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, Options{}) }()
	conn := waitForRunTestStore(t, addr)
	requireRunTestRPC(t, conn, "inline")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	finishRunTest(t, cancel, done, addr)
}

func TestRunServingCancellationIsClean(t *testing.T) {
	addr := runTestAddress(t)
	cfg := runTestConfig(t, addr)
	cfg.Generations = &storeconfig.GenerationsConfig{Config: []string{"one"}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, Options{}) }()
	conn := waitForRunTestStore(t, addr)
	requireRunTestRPC(t, conn, "one")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	finishRunTest(t, cancel, done, addr)
}

func TestRunCancellationDuringLoadDoesNotOpenListener(t *testing.T) {
	addr := runTestAddress(t)
	occupied, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	cfg := runTestConfig(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, Options{Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) {
			close(entered)
			<-release
			return []pkgstore.Generation{"one"}, nil
		}}})
	}()
	<-entered
	cancel()
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after startup cancellation: %v", err)
		}
	case <-time.After(runTestTimeout):
		t.Fatal("Run did not return after startup cancellation")
	}
}

func TestRunSecondListenerFailureRollsBackPrimary(t *testing.T) {
	primary := runTestAddress(t)
	blocked, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.Close()
	cfg := runTestConfig(t, primary)
	cfg.CacheListen = blocked.Addr().String()
	err = Run(context.Background(), cfg, Options{Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) {
		return []pkgstore.Generation{"one"}, nil
	}}})
	if err == nil {
		t.Fatal("Run succeeded with occupied cache listener")
	}
	l, err := net.Listen("tcp", primary)
	if err != nil {
		t.Fatalf("primary listener was not rolled back: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunServingDeadlineIsErrorAndReleasesPort(t *testing.T) {
	addr := runTestAddress(t)
	cfg := runTestConfig(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, Options{Generations: &GenerationSource{Load: func(context.Context) ([]pkgstore.Generation, error) {
			return []pkgstore.Generation{"one"}, nil
		}}})
	}()
	conn := waitForRunTestStore(t, addr)
	requireRunTestRPC(t, conn, "one")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run error = %v, want deadline exceeded", err)
		}
	case <-time.After(runTestTimeout):
		t.Fatal("Run did not return after deadline")
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen address was not released: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
