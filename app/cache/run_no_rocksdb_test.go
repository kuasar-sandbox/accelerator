//go:build no_rocksdb

package cache

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}

func startTestRedis(t *testing.T) (string, func()) {
	t.Helper()
	if _, err := exec.LookPath("redis-server"); err != nil {
		t.Skip("redis-server unavailable")
	}
	dir := t.TempDir()
	sock := filepath.Join(dir, "redis.sock")
	cmd := exec.Command("redis-server", "--save", "", "--appendonly", "no", "--port", "0", "--unixsocket", sock, "--unixsocketperm", "700")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sock); err == nil {
			return sock, func() { _ = cmd.Process.Signal(os.Interrupt); _ = cmd.Wait() }
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	t.Fatal("redis socket not ready")
	return "", nil
}

func TestRunRedisWireAndPprofLifecycle(t *testing.T) {
	sock, stopRedis := startTestRedis(t)
	defer stopRedis()
	data, health, pprof := freeAddr(t), freeAddr(t), freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{Mode: "local", Type: "redis", Listen: data, HealthListen: health, PprofListen: pprof, StatsInterval: "0", Redis: &runtime.RedisConfig{Endpoint: sock, GetPool: 2, SetPool: 1, Timeout: "1s"}}, Options{})
	}()
	var c client.TierCloser
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		c, err = client.New(data, client.Options{Pool: 1, Timeout: time.Second})
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c == nil {
		cancel()
		t.Fatal("cache did not become ready")
	}
	key := store.ContentKey(sha256.Sum256([]byte("cache-run")))
	want := []byte("cache-run-value")
	if err := c.Fill(context.Background(), store.PartitionChunk, key, want); err != nil {
		t.Fatal(err)
	}
	res, blob, err := c.Get(context.Background(), store.PartitionChunk, key)
	if err != nil || blob == nil {
		t.Fatalf("get result=%v blob=%v err=%v", res, blob, err)
	}
	if got := string(blob.Bytes()); got != string(want) {
		blob.Release()
		t.Fatalf("value=%q want=%q", got, want)
	}
	blob.Release()
	_ = c.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	for name, addr := range map[string]string{"data": data, "health": health, "pprof": pprof} {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("%s listener not released: %v", name, err)
		}
		_ = l.Close()
	}
}

func TestRunRejectsIncompleteMembershipBinding(t *testing.T) {
	cfg := Config{Mode: "tiered", Listen: "127.0.0.1:1", Tiers: []runtime.TierConfig{{Type: "ec", Cluster: &runtime.ECClusterConfig{DataShards: 1, ParityShards: 1, Peers: []runtime.PeerConfig{{ID: "a", Endpoint: "127.0.0.1:1"}, {ID: "b", Endpoint: "127.0.0.1:2"}}}}}, Origin: &runtime.OriginConfig{Type: "upstream", Upstream: &runtime.UpstreamClientConfig{Endpoint: "127.0.0.1:3"}}}
	if err := Run(context.Background(), cfg, Options{Reload: make(chan struct{}, 1)}); err == nil || fmt.Sprint(err) == "" {
		t.Fatal("expected incomplete membership binding error")
	}
}
