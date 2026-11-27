package cache

import (
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

// embeddedTier constructs an embedded TierConfig rooted at a fresh tmpdir.
func embeddedTier(t *testing.T) runtime.TierConfig {
	t.Helper()
	return runtime.TierConfig{
		Type: "embedded",
		Rocks: &runtime.RocksConfig{
			Path:      t.TempDir(),
			DiskBytes: "64MiB",
			MemRatio:  0.1,
		},
	}
}

// ecTier returns a minimally-valid EC TierConfig. `ec.NewTier` does not open
// connections at construction time — it only builds the encoder/router — so a
// loopback endpoint is enough to exercise the happy-path construction.
func ecTier() runtime.TierConfig {
	return runtime.TierConfig{
		Type: "ec",
		Cluster: &runtime.ECClusterConfig{
			DataShards:   4,
			ParityShards: 1,
			Peers: []runtime.PeerConfig{
				{ID: "p0", Endpoint: "127.0.0.1:1"},
				{ID: "p1", Endpoint: "127.0.0.1:2"},
				{ID: "p2", Endpoint: "127.0.0.1:3"},
				{ID: "p3", Endpoint: "127.0.0.1:4"},
				{ID: "p4", Endpoint: "127.0.0.1:5"},
			},
		},
	}
}

// badEcTier returns a TierConfig whose ec.NewTier call is guaranteed to fail
// at construction (empty peer list → router refuses). Used for partial-failure
// rollback tests.
func badEcTier() runtime.TierConfig {
	return runtime.TierConfig{
		Type: "ec",
		Cluster: &runtime.ECClusterConfig{
			DataShards:   4,
			ParityShards: 1,
			Peers:        nil, // NewRouter errors on empty peers
		},
	}
}

func passiveRedisSocket(t *testing.T) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "redis.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var acceptWg sync.WaitGroup
	acceptWg.Add(1)
	go func() {
		defer acceptWg.Done()
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var buf [1]byte
				_, _ = conn.Read(buf[:])
			}()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		acceptWg.Wait()
	})
	return socket
}

func TestBuildTieredChain_RejectsRedisMaxInflightWithoutValidation(t *testing.T) {
	cfg := &runtime.Config{Mode: "tiered", Tiers: []runtime.TierConfig{{
		Type: "redis", MaxInflight: 1,
		Redis: &runtime.RedisConfig{Endpoint: "/run/cache/redis.sock", GetPool: 1, SetPool: 1},
	}}}
	comps, err := buildTieredChain(cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "redis.get_pool and redis.set_pool") {
		t.Fatalf("buildTieredChain comps=%v err=%v", comps, err)
	}
}

func TestBuildTieredChain_RejectsNegativeMaxInflightWithoutValidation(t *testing.T) {
	tier := ecTier()
	tier.MaxInflight = -1
	comps, err := buildTieredChain(&runtime.Config{Mode: "tiered", Tiers: []runtime.TierConfig{tier}}, nil)
	if err == nil || !strings.Contains(err.Error(), "max_inflight must be >= 0") {
		t.Fatalf("buildTieredChain comps=%v err=%v", comps, err)
	}
}

func TestBuildTieredChain_RedisOnly(t *testing.T) {
	socket := passiveRedisSocket(t)

	cfg := &runtime.Config{
		Mode: "tiered",
		Tiers: []runtime.TierConfig{{
			Type: "redis",
			Redis: &runtime.RedisConfig{
				Endpoint: socket,
				GetPool:  1,
				SetPool:  1,
				Timeout:  "1s",
			},
		}},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatalf("buildTieredChain: %v", err)
	}
	if len(comps.Tiers) != 1 || len(comps.TierSpecs) != 1 {
		t.Fatalf("unexpected component lengths: tiers=%d specs=%d", len(comps.Tiers), len(comps.TierSpecs))
	}
	if comps.TierSpecs[0].Type != "redis" || comps.TierSpecs[0].RedisStore == nil {
		t.Fatalf("unexpected Redis tier spec: %+v", comps.TierSpecs[0])
	}
	if !comps.TierSpecs[0].RedisStore.Healthy() {
		t.Fatal("Redis tier should have connected GET and SET workers")
	}
	if err := comps.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildTieredChain_ECThenRedisPreservesOrder(t *testing.T) {
	socket := passiveRedisSocket(t)
	cfg := &runtime.Config{
		Mode: "tiered",
		Tiers: []runtime.TierConfig{
			ecTier(),
			{
				Type: "redis",
				Redis: &runtime.RedisConfig{
					Endpoint: socket,
					GetPool:  1,
					SetPool:  1,
					Timeout:  "1s",
				},
			},
		},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatalf("buildTieredChain: %v", err)
	}
	defer comps.Close()

	if len(comps.TierSpecs) != 2 || comps.TierSpecs[0].Type != "ec" || comps.TierSpecs[1].Type != "redis" {
		t.Fatalf("tier order: got %+v, want [ec, redis]", comps.TierSpecs)
	}
}

func TestBuildTieredChain_RedisRollsBackOnLaterFailure(t *testing.T) {
	err := requireRedisRollbackAfter(t, runtime.TierConfig{Type: "unknown"})
	if !strings.Contains(err.Error(), "tiers[1]: unknown type") {
		t.Fatalf("wrong failure stage: %v", err)
	}
}

// requireRedisRollbackAfter proves both real workers opened by the first tier
// are closed after the supplied second tier fails. It applies to both builds.
func requireRedisRollbackAfter(t *testing.T, failingTier runtime.TierConfig) error {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "redis.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	accepted := make(chan net.Conn, 2)
	acceptErr := make(chan error, 1)
	go func() {
		for i := 0; i < cap(accepted); i++ {
			conn, err := lis.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- conn
		}
	}()

	cfg := &runtime.Config{
		Mode: "tiered",
		Tiers: []runtime.TierConfig{
			{
				Type: "redis",
				Redis: &runtime.RedisConfig{
					Endpoint: socket,
					GetPool:  1,
					SetPool:  1,
					Timeout:  "1s",
				},
			},
			failingTier,
		},
	}
	comps, buildErr := buildTieredChain(cfg, nil)
	if buildErr == nil {
		t.Fatal("buildTieredChain should reject the later tier")
	}
	if comps != nil {
		t.Fatal("comps should be nil on partial failure")
	}

	for i := 0; i < cap(accepted); i++ {
		var conn net.Conn
		select {
		case conn = <-accepted:
		case err := <-acceptErr:
			t.Fatalf("accept Redis worker %d: %v", i, err)
		case <-time.After(time.Second):
			t.Fatalf("timed out accepting Redis worker %d", i)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		var one [1]byte
		_, err = conn.Read(one[:])
		_ = conn.Close()
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Redis worker %d was not closed by rollback: %v", i, err)
		}
	}
	return buildErr
}

func TestBuildTieredChain_ECOnly(t *testing.T) {
	cfg := &runtime.Config{
		Mode:  "tiered",
		Tiers: []runtime.TierConfig{ecTier()},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatalf("buildTieredChain: %v", err)
	}
	defer comps.Close()

	if got := len(comps.Tiers); got != 1 {
		t.Fatalf("Tiers length: got %d, want 1", got)
	}
	if comps.EmbeddedStore != nil {
		t.Fatal("EmbeddedStore should be nil for [ec] chain")
	}
}

// TestBuildTieredChain_UpstreamHappyPath exercises the upstream tier
// wiring end-to-end at the builder level: a loopback TCP listener
// accepts and holds the connections that DialConnPool opens (reader
// pool + writer pool), buildTieredChain constructs a *client.Tier,
// and Close tears everything down without panicking.
//
// The listener is intentionally passive — it never reads or writes on
// accepted connections. DialConnPool only needs the dial to succeed;
// it does not issue a handshake, so a bare Accept loop is enough.
func TestBuildTieredChain_UpstreamHappyPath(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	// Accept loop holds references to incoming connections so the
	// dialer side of DialConnPool doesn't see its peer go away. Exit
	// condition: lis.Close() → Accept returns error → goroutine joins
	// via WaitGroup. Post-join we close the held conns.
	var (
		mu       sync.Mutex
		held     []net.Conn
		acceptWg sync.WaitGroup
	)
	acceptWg.Add(1)
	go func() {
		defer acceptWg.Done()
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	defer func() {
		_ = lis.Close()
		acceptWg.Wait()
		mu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		mu.Unlock()
	}()

	cfg := &runtime.Config{
		Mode: "tiered",
		Tiers: []runtime.TierConfig{
			{
				Type:     "upstream",
				Endpoint: lis.Addr().String(),
				Pool:     1,
				Timeout:  "500ms",
			},
		},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err != nil {
		t.Fatalf("buildTieredChain: %v", err)
	}
	defer comps.Close()

	if got := len(comps.Tiers); got != 1 {
		t.Fatalf("Tiers length: got %d, want 1", got)
	}
	if _, ok := comps.Tiers[0].(client.TierCloser); !ok {
		t.Fatalf("Tiers[0] type: got %T, want client.TierCloser", comps.Tiers[0])
	}
	if comps.EmbeddedStore != nil {
		t.Fatal("EmbeddedStore should be nil for upstream-only chain")
	}
}

// TestBuildTieredChain_UpstreamDialFails asserts the rollback path:
// when the reader or writer dial fails, buildTieredChain surfaces a
// wrapped error and comps is nil. 127.0.0.1:1 is RFC-reserved and
// refuses connections immediately, so DialConnPool errors out at the
// first TCP dial.
func TestBuildTieredChain_UpstreamDialFails(t *testing.T) {
	cfg := &runtime.Config{
		Mode: "tiered",
		Tiers: []runtime.TierConfig{
			{
				Type:     "upstream",
				Endpoint: "127.0.0.1:1",
				Pool:     1,
				Timeout:  "200ms",
			},
		},
	}
	comps, err := buildTieredChain(cfg, nil)
	if err == nil {
		t.Fatal("buildTieredChain should fail when dial is refused")
	}
	if comps != nil {
		t.Fatal("comps should be nil on dial failure")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Fatalf("error should mention 'dial', got: %v", err)
	}
}
