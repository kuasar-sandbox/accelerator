package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
)

func TestBenchmarkKeySaltIsolatesRounds(t *testing.T) {
	if benchmarkKey("cold", "round-1", 7) == benchmarkKey("cold", "round-2", 7) {
		t.Fatal("different round salts produced the same key")
	}
	if benchmarkKey("warm", "round-1", 7) == benchmarkKey("cold", "round-1", 7) {
		t.Fatal("warm and cold key spaces overlap")
	}
	if benchmarkKey("cold", "round-1", 7) != benchmarkKey("cold", "round-1", 7) {
		t.Fatal("benchmark key generation is not deterministic")
	}
}

func TestSnapshotFinalCountersWaitsForRedisDrainAndReconnect(t *testing.T) {
	calls := 0
	fetch := func(string, time.Duration) (cache.DaemonStats, error) {
		calls++
		redis := &cache.RedisStats{
			GetPoolSize: 1,
			SetPoolSize: 1,
			Draining:    1,
		}
		if calls > 1 {
			redis.GetConnected = 1
			redis.SetConnected = 1
			redis.Draining = 0
			redis.LateBytesDrained = 4096
			redis.Reconnects = 1
		}
		return cache.DaemonStats{Redis: redis}, nil
	}

	finals, haveFinal, settled := snapshotFinalCounters(
		[]string{"127.0.0.1:7701"}, []bool{true}, time.Second, fetch,
	)
	if !settled {
		t.Fatal("counter snapshot did not settle")
	}
	if calls < 2 {
		t.Fatalf("fetch calls = %d, want at least 2", calls)
	}
	if !haveFinal[0] || finals[0].Redis == nil {
		t.Fatalf("final Redis snapshot missing: %#v", finals)
	}
	if got := finals[0].Redis.LateBytesDrained; got != 4096 {
		t.Fatalf("late bytes = %d, want 4096", got)
	}
}

func TestStringListFlagPreservesEndpoints(t *testing.T) {
	var endpoints stringListFlag
	for _, endpoint := range []string{"127.0.0.1:7701", "127.0.0.1:7702"} {
		if err := endpoints.Set(endpoint); err != nil {
			t.Fatalf("Set(%q): %v", endpoint, err)
		}
	}
	if got, want := []string(endpoints), []string{"127.0.0.1:7701", "127.0.0.1:7702"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoints = %v, want %v", got, want)
	}
	if got, want := endpoints.String(), "127.0.0.1:7701,127.0.0.1:7702"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestDiffCountersIncludesRedisWindow(t *testing.T) {
	beforeRedis := &cache.RedisStats{
		Endpoint:         "/run/redis.sock",
		Transport:        "unix",
		GetHits:          100,
		GetMisses:        10,
		Sets:             20,
		Cancelled:        3,
		LateBytesDrained: 1024,
		Reconnects:       1,
		ProtocolErrors:   2,
		BackendErrors:    4,
		GetP99Ns:         123,
	}
	afterRedis := &cache.RedisStats{
		Endpoint:         "/run/redis.sock",
		Transport:        "unix",
		GetPoolSize:      32,
		SetPoolSize:      8,
		GetConnected:     31,
		SetConnected:     8,
		GetInflight:      2,
		SetInflight:      1,
		PoolWaiters:      5,
		Draining:         6,
		GetHits:          109,
		GetMisses:        12,
		Sets:             27,
		Cancelled:        8,
		LateBytesDrained: 4096,
		Reconnects:       2,
		ProtocolErrors:   5,
		BackendErrors:    8,
		GetP99Ns:         999,
	}

	before := cache.DaemonStats{
		Redis: beforeRedis,
		Tiered: &cache.TieredStats{Tiers: []cache.TierStats{{
			Type:  "redis",
			Redis: beforeRedis,
		}}},
	}
	after := cache.DaemonStats{
		Mode:        "tiered",
		BackendType: "redis",
		Redis:       afterRedis,
		Tiered: &cache.TieredStats{Tiers: []cache.TierStats{{
			Type:  "redis",
			Redis: afterRedis,
		}}},
	}

	want := &cache.RedisStats{
		Endpoint:         "/run/redis.sock",
		Transport:        "unix",
		GetPoolSize:      32,
		SetPoolSize:      8,
		GetConnected:     31,
		SetConnected:     8,
		GetInflight:      2,
		SetInflight:      1,
		PoolWaiters:      5,
		Draining:         6,
		GetHits:          9,
		GetMisses:        2,
		Sets:             7,
		Cancelled:        5,
		LateBytesDrained: 3072,
		Reconnects:       1,
		ProtocolErrors:   3,
		BackendErrors:    4,
	}

	got := diffCounters(before, after)
	if got.Mode != "tiered" || got.BackendType != "redis" {
		t.Fatalf("metadata mismatch: %#v", got)
	}
	if !reflect.DeepEqual(got.Redis, want) {
		t.Fatalf("top-level Redis delta mismatch:\n got: %#v\nwant: %#v", got.Redis, want)
	}
	if got.Tiered == nil || len(got.Tiered.Tiers) != 1 {
		t.Fatalf("tiered delta missing: %#v", got.Tiered)
	}
	if !reflect.DeepEqual(got.Tiered.Tiers[0].Redis, want) {
		t.Fatalf("tier Redis delta mismatch:\n got: %#v\nwant: %#v", got.Tiered.Tiers[0].Redis, want)
	}
}
