package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/rocks"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/server"
)

// cacheSampler returns an obstat.RunAdaptive sample func over the wire server:
// it diffs successive handler snapshots and renders one stats line per window,
// reporting the window inactive (silent) when no get/put completed and nothing
// is in flight. tc (tiered counters, may be nil) adds the per-tier hit cascade;
// rk (rocks handle, may be nil) adds slow-moving rocksdb gauges.
func cacheSampler(mode string, ws *server.WireServer, tc *cache.TieredCache, rk rocks.Interface) func(float64) (string, bool) {
	h := ws.Handler()
	prev := h.WireStats()
	var prevTier cache.TieredCounters
	if tc != nil {
		prevTier = tc.Counters()
	}
	return func(elapsed float64) (string, bool) {
		cur := h.WireStats()
		dGet := cur.GetN - prev.GetN
		dPut := cur.PutN - prev.PutN
		dErr := cur.ErrN - prev.ErrN
		if dGet == 0 && dPut == 0 && cur.Inflight == 0 {
			prev = cur
			if tc != nil {
				prevTier = tc.Counters()
			}
			return "", false
		}
		gw := cur.GetHist.Sub(prev.GetHist)
		pw := cur.PutHist.Sub(prev.PutHist)
		dHits, dMiss := cur.Hits-prev.Hits, cur.Misses-prev.Misses

		var b strings.Builder
		fmt.Fprintf(&b, "cache stat %s | get %s/s", mode, obstat.FmtCount(float64(dGet)/elapsed))
		if dGet > 0 {
			fmt.Fprintf(&b, " %s p50 %s/p99 %s/max %s",
				obstat.FmtBytesPerSec(float64(cur.GetBytes-prev.GetBytes)/elapsed),
				obstat.FmtNs(gw.P50()), obstat.FmtNs(gw.P99()), obstat.FmtNs(gw.MaxNs))
		}
		if dHits+dMiss > 0 {
			fmt.Fprintf(&b, " · hit %.0f%%", pct(dHits, dHits+dMiss))
		}
		fmt.Fprintf(&b, " · put %s/s", obstat.FmtCount(float64(dPut)/elapsed))
		if dPut > 0 {
			fmt.Fprintf(&b, " %s p50 %s/p99 %s/max %s",
				obstat.FmtBytesPerSec(float64(cur.PutBytes-prev.PutBytes)/elapsed),
				obstat.FmtNs(pw.P50()), obstat.FmtNs(pw.P99()), obstat.FmtNs(pw.MaxNs))
		}
		fmt.Fprintf(&b, " | inflight %d conns %d", cur.Inflight, ws.ConnCount())

		if tc != nil {
			curTier := tc.Counters()
			if seg := tierCascade(prevTier, curTier); seg != "" {
				fmt.Fprintf(&b, " | %s", seg)
			}
			prevTier = curTier
		}
		if rk != nil {
			if seg := rocksGauge(rk); seg != "" {
				fmt.Fprintf(&b, " | %s", seg)
			}
		}
		if dErr > 0 {
			fmt.Fprintf(&b, " | err %d", dErr)
		}
		prev = cur
		return b.String(), true
	}
}

// tierCascade renders where the window's hits landed across the cache tiers and
// the origin, e.g. "hits L0 88% L1 6% origin 6%". Empty when no hits this window.
func tierCascade(prev, cur cache.TieredCounters) string {
	dT := make([]uint64, len(cur.TierHits))
	var total uint64
	for i := range cur.TierHits {
		dT[i] = cur.TierHits[i] - prev.TierHits[i]
		total += dT[i]
	}
	dO := cur.OriginHits - prev.OriginHits
	total += dO
	if total == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("hits")
	for i, v := range dT {
		fmt.Fprintf(&b, " L%d %.0f%%", i, float64(v)/float64(total)*100)
	}
	fmt.Fprintf(&b, " origin %.0f%%", float64(dO)/float64(total)*100)
	return b.String()
}

// rocksGauge renders the absolute rocksdb column-family gauges (keys, live data
// size, running compactions), e.g. "rocks chunk 1.2M keys/3.4GiB comp 0".
func rocksGauge(rk rocks.Interface) string {
	cfs := rk.AllStats()
	if len(cfs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("rocks")
	for _, cf := range cfs {
		fmt.Fprintf(&b, " %s %skeys/%s", cf.Name, compactNum(cf.NumKeys), humanBytes(cf.DiskUsage))
		if c := strings.TrimSpace(cf.Compactions); c != "" && c != "0" {
			fmt.Fprintf(&b, " comp %s", c)
		}
	}
	return b.String()
}

// pct is num/den as a percentage (0 when den == 0).
func pct(num, den uint64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den) * 100
}

// compactNum renders a rocksdb count property (a decimal string) compactly
// (1.2M); the raw string is returned on a parse miss.
func compactNum(s string) string {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return strings.TrimSpace(s)
	}
	return obstat.FmtCount(v)
}

// humanBytes renders a rocksdb byte-count property (a decimal string) with
// binary units; the raw string is returned on a parse miss.
func humanBytes(s string) string {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return strings.TrimSpace(s) + "B"
	}
	const k = 1024.0
	switch {
	case v < k:
		return fmt.Sprintf("%.0fB", v)
	case v < k*k:
		return fmt.Sprintf("%.0fKiB", v/k)
	case v < k*k*k:
		return fmt.Sprintf("%.0fMiB", v/(k*k))
	default:
		return fmt.Sprintf("%.1fGiB", v/(k*k*k))
	}
}
