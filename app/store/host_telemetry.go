package store

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/hostmetrics"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

func storeTelemetryPoints(stats storeserver.StatsSnapshot, observed time.Time) []hostmetrics.Point {
	points := make([]hostmetrics.Point, 0, 10)
	add := func(name, unit string, v uint64) {
		value := v
		points = append(points, hostmetrics.Point{Name: name, Unit: unit, Time: observed, Counter: true, Uint: &value})
	}
	add("store.get.requests", "{request}", stats.GetN)
	add("store.get.hits", "{request}", stats.GetHits)
	add("store.get.bytes", "By", stats.GetBytes)
	add("store.put.requests", "{request}", stats.PutN)
	add("store.put.bytes", "By", stats.PutBytes)
	add("store.put.deduplicated", "{request}", stats.PutDedup)
	add("store.admit.requests", "{request}", stats.AdmitN)
	add("store.error.requests", "{request}", stats.ErrN)
	points = append(points,
		hostmetrics.Point{Name: "store.get.latency", Unit: "ns", Time: observed, Histogram: &stats.GetHist},
		hostmetrics.Point{Name: "store.put.latency", Unit: "ns", Time: observed, Histogram: &stats.PutHist},
		hostmetrics.Point{Name: "store.inflight", Unit: "{request}", Time: observed, Value: float64(stats.Inflight)},
	)
	return points
}

func publishStoreTelemetry(ctx context.Context, sender *hostmetrics.Publisher, srv *storeserver.Server) {
	_ = sender.Send(ctx, storeTelemetryPoints(srv.Stats(), time.Now()))
}

// Host telemetry is independent of the optional adaptive stderr stats printer.
func runStoreHostMetrics(ctx context.Context, srv *storeserver.Server) {
	sender := hostmetrics.New("store-ctl", "store", nil)
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
			publishStoreTelemetry(ctx, sender, srv)
		}
	}
}
