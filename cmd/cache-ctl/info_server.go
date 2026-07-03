package main

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/client"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/ec"
	cachepb "github.com/kuasar-sandbox/accelerator/pkg/cache/pb"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/server"
)

// TierSpec carries a tier's type plus the one concrete reference that
// matches that type. Populated alongside tier construction in
// tiered_build.go; parallel to TieredCache.tiers.
//
// Exactly one of EmbeddedStore / EC / UpstreamEP is set, per Type.
type TierSpec struct {
	Type          string
	EmbeddedStore rocks.Interface   // "embedded"
	EC            ec.Interface      // "ec"
	UpstreamEP    string            // "upstream"
	Upstream      client.TierCloser // "upstream"; retained for future
}

// OriginSpec carries the origin's type + the one type-specific detail.
// The cascade-view counters (hits/misses) are owned by TieredCache;
// this struct only carries display-side metadata.
type OriginSpec struct {
	Type      string
	StorePath string // "store"
	Endpoint  string // "upstream"
}

// InfoServer is the gRPC handler for cac.cache.v1.Info/Get. It holds
// references to every counter-owning component and assembles a
// DaemonStats snapshot on each request. No latency data is collected.
type InfoServer struct {
	cachepb.UnimplementedInfoServer

	start time.Time
	mode  string

	handler       *server.CacheHandler // non-nil: always (wire counters source)
	tiered        *cache.TieredCache   // non-nil when mode == "tiered"
	tierSpecs     []TierSpec           // parallel to tiered.tiers
	origin        *OriginSpec          // non-nil when mode == "tiered"
	topLevelRocks rocks.Interface      // non-nil when mode != "tiered"
}

// NewInfoServer creates a fresh InfoServer with the wire handler
// attached. Subsequent SetTiered / SetTopLevelRocks calls fill in
// mode-specific sources.
func NewInfoServer(mode string, handler *server.CacheHandler) *InfoServer {
	return &InfoServer{
		start:   time.Now(),
		mode:    mode,
		handler: handler,
	}
}

// SetTiered attaches the tiered cache and its parallel TierSpecs +
// OriginSpec. Only valid when mode == "tiered".
func (s *InfoServer) SetTiered(tc *cache.TieredCache, specs []TierSpec, origin *OriginSpec) {
	s.tiered = tc
	s.tierSpecs = specs
	s.origin = origin
}

// SetTopLevelRocks attaches the primary rocks.Store for local/shard
// mode — that is, the one rocks.Store backing the daemon's direct-
// write path. In tiered mode top-level rocks is always nil (per-tier
// rocks properties appear inside TieredStats.Tiers[].Rocks instead).
func (s *InfoServer) SetTopLevelRocks(r rocks.Interface) {
	s.topLevelRocks = r
}

// Get implements cachepb.InfoServer. Assembles a cache.DaemonStats
// first (pure Go, no proto), then converts to proto at the boundary.
func (s *InfoServer) Get(ctx context.Context, req *cachepb.InfoRequest) (*cachepb.InfoReply, error) {
	ds := s.snapshot()
	return toPb(ds), nil
}

// WaitFills blocks until all in-flight fill goroutines tracked by the
// TieredCache have drained, or the client's ctx deadline fires. In
// non-tiered modes there's no fill-inflight tracking, so this returns
// immediately. The client is expected to set a ctx deadline — a stuck
// peer could otherwise block the call indefinitely.
func (s *InfoServer) WaitFills(ctx context.Context, req *cachepb.WaitFillsRequest) (*cachepb.WaitFillsReply, error) {
	if s.tiered != nil {
		if err := s.tiered.WaitFills(ctx); err != nil {
			return nil, err
		}
	}
	return &cachepb.WaitFillsReply{}, nil
}

// snapshot builds the pure-Go DaemonStats. Split from Get so we can
// test the assembly logic without touching proto types.
func (s *InfoServer) snapshot() cache.DaemonStats {
	ds := cache.DaemonStats{
		Mode:      s.mode,
		UptimeSec: int64(time.Since(s.start).Seconds()),
	}
	if s.handler != nil {
		ds.Server = s.handler.ServerStats()
	}
	if s.tiered != nil {
		cnt := s.tiered.Counters()
		tiers := make([]cache.TierStats, len(s.tierSpecs))
		for i, spec := range s.tierSpecs {
			t := cache.TierStats{
				Type:   spec.Type,
				Hits:   cnt.TierHits[i],
				Misses: cnt.TierMisses[i],
				Fills:  cnt.TierFills[i],
				Errors: cnt.TierErrors[i],
			}
			switch spec.Type {
			case "embedded":
				if spec.EmbeddedStore != nil {
					t.Rocks = toRocksCFStats(spec.EmbeddedStore.AllStats())
				}
			case "ec":
				if spec.EC != nil {
					t.Peers = spec.EC.PeersStats()
				}
			case "upstream":
				t.Endpoint = spec.UpstreamEP
			}
			tiers[i] = t
		}
		origin := cache.OriginStats{
			Hits:   cnt.OriginHits,
			Misses: cnt.OriginMisses,
			Errors: cnt.OriginErrors,
		}
		if s.origin != nil {
			origin.Type = s.origin.Type
			origin.StorePath = s.origin.StorePath
			origin.Endpoint = s.origin.Endpoint
		}
		ds.Tiered = &cache.TieredStats{Tiers: tiers, Origin: origin}
	}
	if s.topLevelRocks != nil {
		ds.Rocks = toRocksCFStats(s.topLevelRocks.AllStats())
	}
	return ds
}

// toRocksCFStats is a plain field copy from rocks.CFStats to the
// cache package's JSON-tagged type. The Compactions / BlobStats
// fields on rocks.CFStats are dropped — they're verbose and not
// exposed in the current Info API.
func toRocksCFStats(in []rocks.CFStats) []cache.RocksCFStats {
	out := make([]cache.RocksCFStats, len(in))
	for i, s := range in {
		out[i] = cache.RocksCFStats{
			Name:      s.Name,
			NumKeys:   s.NumKeys,
			DiskUsage: s.DiskUsage,
			MemUsage:  s.MemUsage,
		}
	}
	return out
}

// toPb mechanically copies cache.DaemonStats into cachepb.InfoReply.
// No reflection or generics — fields are scalar + simple nested slices.
func toPb(ds cache.DaemonStats) *cachepb.InfoReply {
	reply := &cachepb.InfoReply{
		Mode:      ds.Mode,
		UptimeSec: ds.UptimeSec,
		Server: &cachepb.ServerStats{
			Hits:   ds.Server.Hits,
			Misses: ds.Server.Misses,
			Fills:  ds.Server.Fills,
		},
	}
	if ds.Tiered != nil {
		tiers := make([]*cachepb.TierStats, len(ds.Tiered.Tiers))
		for i, t := range ds.Tiered.Tiers {
			tiers[i] = &cachepb.TierStats{
				Type:     t.Type,
				Hits:     t.Hits,
				Misses:   t.Misses,
				Fills:    t.Fills,
				Errors:   t.Errors,
				Rocks:    rocksToPb(t.Rocks),
				Peers:    peersToPb(t.Peers),
				Endpoint: t.Endpoint,
			}
		}
		reply.Tiered = &cachepb.TieredStats{
			Tiers: tiers,
			Origin: &cachepb.OriginStats{
				Type:      ds.Tiered.Origin.Type,
				Hits:      ds.Tiered.Origin.Hits,
				Misses:    ds.Tiered.Origin.Misses,
				Errors:    ds.Tiered.Origin.Errors,
				StorePath: ds.Tiered.Origin.StorePath,
				Endpoint:  ds.Tiered.Origin.Endpoint,
			},
		}
	}
	if len(ds.Rocks) > 0 {
		reply.Rocks = rocksToPb(ds.Rocks)
	}
	return reply
}

func rocksToPb(in []cache.RocksCFStats) []*cachepb.RocksCFStats {
	out := make([]*cachepb.RocksCFStats, len(in))
	for i, s := range in {
		out[i] = &cachepb.RocksCFStats{
			Name:      s.Name,
			NumKeys:   s.NumKeys,
			DiskUsage: s.DiskUsage,
			MemUsage:  s.MemUsage,
		}
	}
	return out
}

func peersToPb(in []cache.PeerStats) []*cachepb.PeerStats {
	out := make([]*cachepb.PeerStats, len(in))
	for i, p := range in {
		out[i] = &cachepb.PeerStats{
			Id:        p.ID,
			Endpoint:  p.Endpoint,
			Hits:      p.Hits,
			Misses:    p.Misses,
			Errors:    p.Errors,
			Cancelled: p.Cancelled,
			Fills:     p.Fills,
		}
	}
	return out
}
