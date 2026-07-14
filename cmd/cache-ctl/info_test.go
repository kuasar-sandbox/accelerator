package main

import (
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
)

func TestRedisInfoProtoRoundTrip(t *testing.T) {
	redis := &cache.RedisStats{
		Socket:           "/run/kuasar-cache/redis.sock",
		GetPoolSize:      32,
		SetPoolSize:      8,
		GetConnected:     31,
		SetConnected:     7,
		GetInflight:      5,
		SetInflight:      3,
		PoolWaiters:      2,
		Draining:         1,
		GetHits:          101,
		GetMisses:        11,
		Sets:             23,
		Cancelled:        7,
		LateBytesDrained: 4096,
		Reconnects:       4,
		ProtocolErrors:   2,
		BackendErrors:    6,
		GetP50Ns:         100_000,
		GetP99Ns:         900_000,
		GetP999Ns:        2_000_000,
		SetP50Ns:         200_000,
		SetP99Ns:         1_000_000,
		SetP999Ns:        3_000_000,
		PoolWaitP99Ns:    50_000,
	}
	want := cache.DaemonStats{
		Mode:        "tiered",
		BackendType: "redis",
		UptimeSec:   42,
		Server:      cache.ServerStats{Hits: 3, Misses: 4, Fills: 5},
		Redis:       redis,
		Tiered: &cache.TieredStats{
			Tiers: []cache.TierStats{{
				Type:   "redis",
				Hits:   9,
				Misses: 8,
				Fills:  7,
				Errors: 6,
				Redis:  redis,
			}},
			Origin: cache.OriginStats{Type: "store", Hits: 5, Misses: 4, Errors: 3, Endpoint: "127.0.0.1:7100"},
		},
	}

	got := fromPb(toPb(want))
	if got.Mode != want.Mode || got.BackendType != want.BackendType || got.UptimeSec != want.UptimeSec || got.Server != want.Server {
		t.Fatalf("top-level round trip mismatch: got=%#v want=%#v", got, want)
	}
	if !reflect.DeepEqual(got.Redis, redis) {
		t.Fatalf("top-level Redis round trip mismatch: got=%#v want=%#v", got.Redis, redis)
	}
	if got.Tiered == nil || len(got.Tiered.Tiers) != 1 {
		t.Fatalf("tiered round trip mismatch: %#v", got.Tiered)
	}
	if !reflect.DeepEqual(got.Tiered.Tiers[0].Redis, redis) {
		t.Fatalf("tier Redis round trip mismatch: got=%#v want=%#v", got.Tiered.Tiers[0].Redis, redis)
	}
	if got.Tiered.Tiers[0].Type != "redis" || got.Tiered.Tiers[0].Hits != 9 || got.Tiered.Origin != want.Tiered.Origin {
		t.Fatalf("tier metadata round trip mismatch: got=%#v want=%#v", got.Tiered, want.Tiered)
	}
}
