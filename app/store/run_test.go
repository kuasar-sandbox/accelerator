package store

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
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
