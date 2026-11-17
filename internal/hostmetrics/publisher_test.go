package hostmetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
)

func TestOTLPJSONProducerPreservesMetricSemanticsAndIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan map[string]any, 1)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/metrics" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("bad wire request %+v", r)
			w.WriteHeader(400)
			return
		}
		data, err := io.ReadAll(io.LimitReader(r.Body, 4096))
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		got <- raw
	})}
	go srv.Serve(ln)
	defer srv.Shutdown(context.Background())
	t.Setenv("KS_HOST_OTLP_SOCKET", path)
	publisher := New("cache-ctl", "cache", map[string]string{"deployment.environment.name": "test"})
	a, b := uint64(123), uint64(0)
	var latency obstat.Hist
	latency.Record(time.Microsecond)
	latency.Record(2 * time.Second)
	snap := latency.Snapshot()
	now := time.Now().UTC()
	if err := publisher.Send(context.Background(), []Point{
		{Name: "cache.get.requests", Unit: "{request}", Time: now, Uint: &a, Counter: true, Attributes: map[string]string{"cache.mode": "local"}},
		{Name: "cache.get.requests", Unit: "{request}", Time: now, Uint: &b, Counter: true, Attributes: map[string]string{"cache.mode": "shard"}},
		{Name: "cache.get.latency", Unit: "ns", Time: now, Histogram: &snap},
		{Name: "cache.inflight", Unit: "{request}", Time: now, Value: 1},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-got:
		resources := payload["resourceMetrics"].([]any)
		if len(resources) != 1 {
			t.Fatalf("resource groups %d", len(resources))
		}
		resource := resources[0].(map[string]any)["resource"].(map[string]any)
		attrs := map[string]string{}
		for _, a := range resource["attributes"].([]any) {
			aa := a.(map[string]any)
			attrs[aa["key"].(string)] = aa["value"].(map[string]any)["stringValue"].(string)
		}
		for k, want := range map[string]string{"service.name": "cache-ctl", "sandbox.telemetry.source": "cache", "service.instance.id": processInstanceID, "deployment.environment.name": "test"} {
			if attrs[k] != want {
				t.Errorf("attribute %s=%s want %s", k, attrs[k], want)
			}
		}
		if _, exists := attrs["sandbox.id"]; exists {
			t.Error("fabricated SID")
		}
		scopes := resources[0].(map[string]any)["scopeMetrics"].([]any)
		metrics := scopes[0].(map[string]any)["metrics"].([]any)
		if len(metrics) != 3 {
			t.Fatalf("metrics=%d", len(metrics))
		}
		latencyMetric := metrics[1].(map[string]any)
		hist := latencyMetric["histogram"].(map[string]any)
		if _, gauge := latencyMetric["gauge"]; gauge || hist["aggregationTemporality"] != float64(2) {
			t.Fatalf("wrong OTLP histogram: %+v", latencyMetric)
		}
		point := hist["dataPoints"].([]any)[0].(map[string]any)
		bounds := point["explicitBounds"].([]any)
		counts := point["bucketCounts"].([]any)
		if point["count"] != "2" || point["sum"] != nil || point["max"] != nil ||
			len(bounds) != 21 || len(counts) != 22 || bounds[0] != float64(1000) || bounds[20] != float64(1e9) || counts[0] != "1" || counts[21] != "1" {
			t.Fatalf("latency lost on the wire: %+v", point)
		}
		counter := metrics[0].(map[string]any)
		sum := counter["sum"].(map[string]any)
		if counter["gauge"] != nil || counter["histogram"] != nil ||
			sum["aggregationTemporality"] != float64(2) || sum["isMonotonic"] != true {
			t.Fatalf("wrong cumulative counter on the wire: %+v", counter)
		}
		points := sum["dataPoints"].([]any)
		if len(points) != 2 {
			t.Fatalf("points=%d", len(points))
		}
		if points[0].(map[string]any)["asInt"] != "123" || points[1].(map[string]any)["asInt"] != "0" {
			t.Errorf("integer metrics %#v", points)
		}
		start := strconv.FormatInt(processStart.UnixNano(), 10)
		for _, point := range points {
			dp := point.(map[string]any)
			if dp["startTimeUnixNano"] != start || dp["timeUnixNano"] != strconv.FormatInt(now.UnixNano(), 10) {
				t.Fatalf("counter interval lost: %+v", dp)
			}
		}
		if point["startTimeUnixNano"] != start {
			t.Fatalf("histogram start differs from counter start: %+v", point)
		}
		gauge := metrics[2].(map[string]any)
		dp := gauge["gauge"].(map[string]any)["dataPoints"].([]any)[0].(map[string]any)
		if gauge["sum"] != nil || dp["startTimeUnixNano"] != nil || dp["asDouble"] != float64(1) {
			t.Fatalf("inflight gauge changed: %+v", gauge)
		}
	case <-time.After(time.Second):
		t.Fatal("no receiver sample")
	}
}

func TestMissingSocketAndBadInputAreBounded(t *testing.T) {
	t.Setenv("KS_HOST_OTLP_SOCKET", filepath.Join(t.TempDir(), "absent.sock"))
	p := New("store-ctl", "store", nil)
	start := time.Now()
	if err := p.Send(context.Background(), []Point{{Name: "store.requests", Time: time.Now(), Value: 1}}); err == nil {
		t.Fatal("missing socket accepted")
	}
	if time.Since(start) > time.Second {
		t.Fatal("blocked on missing telemetry")
	}
	if err := p.Send(context.Background(), []Point{{Name: "x", Time: time.Now(), Value: math.NaN()}}); err == nil {
		t.Fatal("invalid sample accepted")
	}
}

func TestOTLPPartialSuccess(t *testing.T) {
	for _, test := range []struct {
		name, body string
		rejected   bool
	}{
		{"warning", `{"partialSuccess":{"errorMessage":"warning"}}`, false},
		{"partial", `{"partialSuccess":{"rejectedDataPoints":"1","errorMessage":"invalid point"}}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "host.sock")
			ln, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, test.body)
			})}
			go srv.Serve(ln)
			defer srv.Close()
			t.Setenv("KS_HOST_OTLP_SOCKET", path)
			p := New("test", "test", nil)
			err = p.Send(t.Context(), []Point{{Name: "test.example", Time: time.Now(), Value: 1}})
			if test.rejected && (err == nil || !strings.Contains(err.Error(), "invalid point")) {
				t.Fatalf("expected rejected data point, got %v", err)
			}
			if !test.rejected && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLatencyHistogramPointOmitsUnverifiableSummaries(t *testing.T) {
	for _, count := range []uint64{1, 2} {
		// The second Record may update sum/max after Snapshot has loaded count.
		// Matching count and bucket totals therefore also need this coverage.
		s := obstat.HistSnapshot{Count: count, SumNs: 2_000_001_000, MaxNs: 2_000_000_000}
		s.Buckets[0] = 1
		point := latencyHistogramPoint(&s, "123", nil)
		raw, err := json.Marshal(point)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]any
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		if point.Count != "1" || point.BucketCounts[0] != "1" || wire["sum"] != nil || wire["max"] != nil {
			t.Fatalf("inconsistent histogram from concurrent snapshot: %s", raw)
		}
	}
}

func TestDaemonInstanceIdentity(t *testing.T) {
	if os.Getenv("ACCELERATOR_HOSTMETRICS_ID_TEST") == "1" {
		p := New("cache-ctl", "cache", nil)
		fmt.Print(p.resource["service.instance.id"])
		os.Exit(0)
	}
	first := New("cache-ctl", "cache", nil)
	second := New("cache-ctl", "cache", nil)
	id := first.resource["service.instance.id"]
	if id == "" || second.resource["service.instance.id"] != id {
		t.Fatal("publishers in one process must retain a nonempty instance identity")
	}
	seen := map[string]bool{id: true}
	for range 2 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDaemonInstanceIdentity$")
		cmd.Env = append(os.Environ(), "ACCELERATOR_HOSTMETRICS_ID_TEST=1")
		raw, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		childID := string(raw)
		if childID == "" || seen[childID] {
			t.Fatalf("independent daemon processes share an identity: %q", childID)
		}
		seen[childID] = true
	}
}
