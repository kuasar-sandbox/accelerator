package cache

import (
	"testing"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/server"
)

func TestCacheHostTelemetryUsesActualCounters(t *testing.T) {
	got := cacheTelemetryPoints("local", server.WireStats{GetHist: obstat.HistSnapshot{Count: 4}, PutHist: obstat.HistSnapshot{Count: 2}, GetN: 5, PutN: 2, GetBytes: 4096, Inflight: 1}, 2, nil, time.Now())
	histograms := map[string]uint64{}
	found := map[string]float64{}
	for _, p := range got {
		if p.Histogram != nil {
			if p.Unit != "ns" {
				t.Fatalf("unexpected histogram unit %q", p.Unit)
			}
			histograms[p.Name] = p.Histogram.Count
		} else if p.Uint != nil {
			found[p.Name] = float64(*p.Uint)
		} else {
			found[p.Name] = p.Value
		}
	}
	if histograms["cache.get.latency"] != 4 || histograms["cache.put.latency"] != 2 {
		t.Fatalf("latency histogram sources %+v", histograms)
	}
	if found["cache.get.requests"] != 5 || found["cache.get.bytes"] != 4096 || found["cache.inflight"] != 1 || found["cache.connections"] != 2 {
		t.Fatalf("actual counters %+v", found)
	}
}
