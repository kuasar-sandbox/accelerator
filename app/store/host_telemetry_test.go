package store

import (
	"testing"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"time"

	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

func TestStoreHostTelemetryUsesActualCounters(t *testing.T) {
	got := storeTelemetryPoints(storeserver.StatsSnapshot{GetHist: obstat.HistSnapshot{Count: 4}, PutHist: obstat.HistSnapshot{Count: 2}, GetN: 5, PutN: 2, PutDedup: 1, Inflight: 3}, time.Now())
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
	if histograms["store.get.latency"] != 4 || histograms["store.put.latency"] != 2 {
		t.Fatalf("latency histogram sources %+v", histograms)
	}
	if found["store.get.requests"] != 5 || found["store.put.requests"] != 2 || found["store.put.deduplicated"] != 1 || found["store.inflight"] != 3 {
		t.Fatalf("actual counters %+v", found)
	}
}
