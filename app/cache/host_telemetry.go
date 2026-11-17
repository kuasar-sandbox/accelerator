package cache

import (
	"context"
	"strconv"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/hostmetrics"
	pkgcache "github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/server"
)

func cacheTelemetryPoints(mode string, stats server.WireStats, conns int, tier *pkgcache.TieredCounters, observed time.Time) []hostmetrics.Point {
	group := map[string]string{"cache.mode": mode}
	samples := make([]hostmetrics.Point, 0, 20)
	add := func(name, unit string, v uint64, attrs map[string]string) {
		val := v
		samples = append(samples, hostmetrics.Point{Name: name, Unit: unit, Time: observed, Uint: &val, Attributes: attrs})
	}
	add("cache.get.requests", "{request}", stats.GetN, group)
	add("cache.put.requests", "{request}", stats.PutN, group)
	add("cache.error.requests", "{request}", stats.ErrN, group)
	add("cache.get.bytes", "By", stats.GetBytes, group)
	add("cache.put.bytes", "By", stats.PutBytes, group)
	add("cache.hit.requests", "{request}", stats.Hits, group)
	add("cache.miss.requests", "{request}", stats.Misses, group)
	add("cache.fill.requests", "{request}", stats.Fills, group)
	samples = append(samples,
		hostmetrics.Point{Name: "cache.get.latency", Unit: "ns", Time: observed, Histogram: &stats.GetHist, Attributes: group},
		hostmetrics.Point{Name: "cache.put.latency", Unit: "ns", Time: observed, Histogram: &stats.PutHist, Attributes: group},
		hostmetrics.Point{Name: "cache.inflight", Unit: "{request}", Time: observed, Value: float64(stats.Inflight), Attributes: group},
		hostmetrics.Point{Name: "cache.connections", Unit: "{connection}", Time: observed, Value: float64(conns), Attributes: group})
	if tier != nil {
		for idx, count := range tier.TierHits {
			add("cache.tier.hits", "{request}", count, map[string]string{"tier": strconv.Itoa(idx), "cache.mode": mode})
		}
		add("cache.origin.hits", "{request}", tier.OriginHits, group)
	}
	return samples
}

func publishCacheTelemetry(ctx context.Context, sender *hostmetrics.Publisher, mode string, ws *server.WireServer, tc *pkgcache.TieredCache) {
	stats := ws.Handler().WireStats()
	var tier *pkgcache.TieredCounters
	if tc != nil {
		v := tc.Counters()
		tier = &v
	}
	_ = sender.Send(ctx, cacheTelemetryPoints(mode, stats, ws.ConnCount(), tier, time.Now()))
}

// One fixed-cadence Host metrics observer per cache-ctl process. Disabling the
// adaptive stderr stats line does not disable node telemetry.
func runCacheHostMetrics(ctx context.Context, mode string, ws *server.WireServer, tc *pkgcache.TieredCache) {
	sender := hostmetrics.New("cache-ctl", "cache", nil)
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !sender.Available() {
				continue
			}
			publishCacheTelemetry(ctx, sender, mode, ws, tc)
		}
	}
}
