package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/ec"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/redisstore"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

// parseDurationOrDefault parses a Go duration string and falls back to
// def when s is empty or malformed. Used for YAML duration fields that
// are optional; time.ParseDuration("") itself returns an error.
func parseDurationOrDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// tieredComponents bundles a fully-assembled tier chain plus the resources
// it owns. Callers `defer comps.Close()` for cascaded cleanup; on construction
// failure `buildTieredChain` returns a non-nil error AND releases anything it
// managed to construct before the failure (partial-assembly safety).
type tieredComponents struct {
	// Tiers is the tier chain in YAML declaration order — never reordered.
	Tiers []cache.Tier

	// TierSpecs is parallel to Tiers and carries tier type + concrete
	// refs (rocks.Store / ec.Tier / upstream endpoint) for the Info
	// gRPC service to produce per-tier detail. Populated in lockstep
	// with Tiers inside the build loop.
	TierSpecs []TierSpec

	// EmbeddedStore is the embedded RocksDB store if the chain contains
	// a `type: embedded` tier, otherwise nil. The Info gRPC service
	// reads CF properties via EmbeddedStore.AllStats() when building
	// per-tier stats.
	EmbeddedStore rocks.Interface

	// Close releases every resource held by the chain in reverse order
	// of construction. Safe to call exactly once.
	Close func() error
}

// buildTieredChain assembles a tier chain from cfg.Tiers, preserving the
// YAML declaration order verbatim. No special-casing, no type-based
// reordering, no "embedded must be first" implicit rule — YAML order is
// authoritative.
//
// Construction is all-or-nothing: if any tier fails to build, everything
// constructed so far is Close()d in reverse order and the partial result
// is discarded. Callers get either a fully-alive chain or a clean error.
//
// blobPool is threaded into every wire-client-bearing tier (EC peers,
// upstream) so 512KB-ish read payloads come from a shared pool instead
// of fresh make() calls on every ReadResponse. Pass nil to fall back to
// cache.DefaultPool (no pooling).
func buildTieredChain(cfg *runtime.Config, blobPool cache.BlobPool) (*tieredComponents, error) {
	comps := &tieredComponents{}
	var closers []func() error

	closeAll := func() error {
		var errs []error
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i](); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	rollback := func(wrapped error) (*tieredComponents, error) {
		_ = closeAll()
		return nil, wrapped
	}

	for i, t := range cfg.Tiers {
		if t.MaxInflight < 0 {
			return rollback(fmt.Errorf("tiered: tiers[%d]: max_inflight must be >= 0", i))
		}
		switch t.Type {
		case "embedded":
			// Defensive: Validate already enforces embedded ≤1, but we
			// guard here too so buildTieredChain is safe to call without
			// a prior Validate pass (e.g. from tests that construct Config
			// directly).
			if comps.EmbeddedStore != nil {
				return rollback(fmt.Errorf("tiered: more than one embedded tier at tiers[%d]", i))
			}
			if t.Rocks == nil {
				return rollback(fmt.Errorf("tiered: tiers[%d] (embedded): rocks config is nil", i))
			}
			store, err := rocks.Open(*t.Rocks, cfg.Freq)
			if err != nil {
				return rollback(fmt.Errorf("tiered: tiers[%d] (embedded): open rocks: %w", i, err))
			}
			closers = append(closers, store.Close)
			comps.EmbeddedStore = store
			// rocks.Interface implements cache.Tier directly — no wrapper.
			comps.Tiers = append(comps.Tiers, cache.NewTierAdapter(store, t.MaxInflight))
			comps.TierSpecs = append(comps.TierSpecs, TierSpec{
				Type:          "embedded",
				EmbeddedStore: store,
			})

		case "redis":
			if t.MaxInflight != 0 {
				return rollback(fmt.Errorf("tiered: tiers[%d] (redis): max_inflight is not valid; use redis.get_pool and redis.set_pool", i))
			}
			if t.Redis == nil {
				return rollback(fmt.Errorf("tiered: tiers[%d] (redis): redis config is nil", i))
			}
			store, err := redisstore.Open(*t.Redis, blobPool)
			if err != nil {
				return rollback(fmt.Errorf("tiered: tiers[%d] (redis): open store: %w", i, err))
			}
			closers = append(closers, store.Close)
			comps.Tiers = append(comps.Tiers, store)
			comps.TierSpecs = append(comps.TierSpecs, TierSpec{
				Type:       "redis",
				RedisStore: store,
			})

		case "ec":
			if t.Cluster == nil {
				return rollback(fmt.Errorf("tiered: tiers[%d] (ec): cluster config is nil", i))
			}
			peers := make([]ec.Peer, len(t.Cluster.Peers))
			for j, p := range t.Cluster.Peers {
				peers[j] = ec.Peer{ID: p.ID, Endpoint: p.Endpoint}
			}
			ecTier, err := ec.New(ec.Config{
				Cluster: ec.ClusterConfig{
					DataShards:   t.Cluster.DataShards,
					ParityShards: t.Cluster.ParityShards,
					Peers:        peers,
					Pool:         t.Cluster.Pool,
					Timeout:      t.Cluster.Timeout,
				},
				BlobPool: blobPool,
			})
			if err != nil {
				return rollback(fmt.Errorf("tiered: tiers[%d] (ec): create tier: %w", i, err))
			}
			closers = append(closers, ecTier.Close)
			comps.Tiers = append(comps.Tiers, cache.NewTierAdapter(ecTier, t.MaxInflight))
			comps.TierSpecs = append(comps.TierSpecs, TierSpec{
				Type: "ec",
				EC:   ecTier,
			})

		case "upstream":
			// upstream tier: a single client.New handle against the
			// remote cache-ctl. The remote must accept writes so that
			// TieredCache backfill populates it, which in turn means
			// the remote must run in `local` mode. tiered/shard modes
			// reject ObjectPut at the wire handler; that failure
			// surfaces on the first Fill as a StatusError.
			if t.Endpoint == "" {
				return rollback(fmt.Errorf("tiered: tiers[%d] (upstream): endpoint is required", i))
			}
			c, err := client.New(t.Endpoint, client.Options{
				Pool:     t.Pool, // 0 falls through to client-side default (4)
				Timeout:  parseDurationOrDefault(t.Timeout, 2*time.Second),
				BlobPool: blobPool,
			})
			if err != nil {
				return rollback(fmt.Errorf("tiered: tiers[%d] (upstream): dial %s: %w", i, t.Endpoint, err))
			}
			closers = append(closers, c.Close)
			comps.Tiers = append(comps.Tiers, cache.NewTierAdapter(c, t.MaxInflight))
			comps.TierSpecs = append(comps.TierSpecs, TierSpec{
				Type:       "upstream",
				UpstreamEP: t.Endpoint,
				Upstream:   c,
			})

		default:
			// Should not reach here: Validate catches unknown types. This
			// branch exists so buildTieredChain is safe to call without
			// Validate.
			return rollback(fmt.Errorf("tiered: tiers[%d]: unknown type %q", i, t.Type))
		}
	}

	comps.Close = closeAll
	return comps, nil
}
