package main

import (
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

// storeSampler returns an obstat.RunAdaptive sample func over a store Server: it
// diffs successive stat snapshots and renders one stats line per window,
// reporting the window inactive (so nothing prints) when no Get/Put/GetSalt
// completed and nothing is in flight. The closure keeps the previous snapshot.
func storeSampler(srv *server.Server) func(float64) (string, bool) {
	prev := srv.Stats()
	return func(elapsed float64) (string, bool) {
		cur := srv.Stats()
		dGet := cur.GetN - prev.GetN
		dPut := cur.PutN - prev.PutN
		dSalt := cur.SaltN - prev.SaltN
		dErr := cur.ErrN - prev.ErrN
		if dGet == 0 && dPut == 0 && dSalt == 0 && cur.Inflight == 0 {
			prev = cur
			return "", false
		}
		gw := cur.GetHist.Sub(prev.GetHist)
		pw := cur.PutHist.Sub(prev.PutHist)

		var b strings.Builder
		fmt.Fprintf(&b, "store stat | get %s/s", obstat.FmtCount(float64(dGet)/elapsed))
		if dGet > 0 {
			fmt.Fprintf(&b, " %.0f%%hit %s p50 %s/p99 %s/max %s",
				pct(cur.GetHits-prev.GetHits, dGet),
				obstat.FmtBytesPerSec(float64(cur.GetBytes-prev.GetBytes)/elapsed),
				obstat.FmtNs(gw.P50()), obstat.FmtNs(gw.P99()), obstat.FmtNs(gw.MaxNs))
		}
		fmt.Fprintf(&b, " · put %s/s", obstat.FmtCount(float64(dPut)/elapsed))
		if dPut > 0 {
			fmt.Fprintf(&b, " %s dedup %.0f%% p50 %s/p99 %s/max %s",
				obstat.FmtBytesPerSec(float64(cur.PutBytes-prev.PutBytes)/elapsed),
				pct(cur.PutDedup-prev.PutDedup, dPut),
				obstat.FmtNs(pw.P50()), obstat.FmtNs(pw.P99()), obstat.FmtNs(pw.MaxNs))
		}
		if dSalt > 0 {
			fmt.Fprintf(&b, " · salt %s/s", obstat.FmtCount(float64(dSalt)/elapsed))
		}
		fmt.Fprintf(&b, " | inflight %d", cur.Inflight)
		if dErr > 0 {
			fmt.Fprintf(&b, " | err %d", dErr)
		}
		prev = cur
		return b.String(), true
	}
}

// pct is num/den as a percentage (0 when den == 0).
func pct(num, den uint64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den) * 100
}
