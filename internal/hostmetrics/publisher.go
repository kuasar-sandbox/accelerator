package hostmetrics

// Minimal OTLP/HTTP JSON writer for trusted Host components. Keep the source and
// transport independent of orchestrator's business and authentication APIs.
import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
)

const DefaultSocket = "/run/sandbox-telemetry/host.sock"

// Cache/store cumulative accumulators begin empty at process startup. Keep
// their start time and instance identity stable across publishers in this process.
var (
	processStart      = time.Now()
	processInstanceID = rand.Text()
)

type Point struct {
	Name  string
	Unit  string
	Time  time.Time
	Value float64
	Uint  *uint64
	// Counter marks a nonnegative process-lifetime cumulative total.
	Counter    bool
	Histogram  *obstat.HistSnapshot
	Attributes map[string]string
}
type Publisher struct {
	socket   string
	client   *http.Client
	resource map[string]string
	scope    string
}

type otlpValue struct {
	StringValue string `json:"stringValue"`
}
type otlpAttribute struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}
type otlpDP struct {
	StartTimeUnixNano string          `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string          `json:"timeUnixNano"`
	AsDouble          *float64        `json:"asDouble,omitempty"`
	AsInt             string          `json:"asInt,omitempty"`
	Attributes        []otlpAttribute `json:"attributes,omitempty"`
}
type otlpGauge struct {
	DataPoints []otlpDP `json:"dataPoints"`
}

type otlpSum struct {
	AggregationTemporality int      `json:"aggregationTemporality"`
	IsMonotonic            bool     `json:"isMonotonic"`
	DataPoints             []otlpDP `json:"dataPoints"`
}

type otlpHistogramPoint struct {
	StartTimeUnixNano string          `json:"startTimeUnixNano"`
	TimeUnixNano      string          `json:"timeUnixNano"`
	Attributes        []otlpAttribute `json:"attributes,omitempty"`
	Count             string          `json:"count"`
	ExplicitBounds    []float64       `json:"explicitBounds"`
	BucketCounts      []string        `json:"bucketCounts"`
}

type otlpHistogram struct {
	AggregationTemporality int                  `json:"aggregationTemporality"`
	DataPoints             []otlpHistogramPoint `json:"dataPoints"`
}

type otlpMetric struct {
	Name      string         `json:"name"`
	Unit      string         `json:"unit,omitempty"`
	Gauge     *otlpGauge     `json:"gauge,omitempty"`
	Sum       *otlpSum       `json:"sum,omitempty"`
	Histogram *otlpHistogram `json:"histogram,omitempty"`
}

// Native buckets and summaries are read independently. Derive Count from the
// transmitted buckets. Sum and Max are optional in OTLP and are omitted because
// even a matching native Count cannot prove those summaries belong to these buckets.
func latencyHistogramPoint(s *obstat.HistSnapshot, timestamp string, attrs []otlpAttribute) otlpHistogramPoint {
	bounds := obstat.LatencyBoundsNs()
	point := otlpHistogramPoint{
		StartTimeUnixNano: strconv.FormatInt(processStart.UnixNano(), 10),
		TimeUnixNano:      timestamp, Attributes: attrs,
		ExplicitBounds: make([]float64, len(bounds)), BucketCounts: make([]string, len(s.Buckets)),
	}
	for i, ns := range bounds {
		point.ExplicitBounds[i] = float64(ns)
	}
	var count uint64
	for i, n := range s.Buckets {
		point.BucketCounts[i] = strconv.FormatUint(n, 10)
		count += n
	}
	point.Count = strconv.FormatUint(count, 10)

	return point
}

type otlpScopeMetrics struct {
	Scope struct {
		Name string `json:"name"`
	} `json:"scope"`
	Metrics []*otlpMetric `json:"metrics"`
}
type otlpResourceMetrics struct {
	Resource struct {
		Attributes []otlpAttribute `json:"attributes"`
	} `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}
type otlpExport struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

func attributes(raw map[string]string) []otlpAttribute {
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]otlpAttribute, 0, len(keys))
	for _, key := range keys {
		if raw[key] != "" {
			out = append(out, otlpAttribute{Key: key, Value: otlpValue{StringValue: raw[key]}})
		}
	}
	return out
}

func New(service, source string, resource map[string]string) *Publisher {
	socket := os.Getenv("KS_HOST_OTLP_SOCKET")
	if socket == "" {
		socket = DefaultSocket
	}
	attrs := make(map[string]string, len(resource)+2)
	for k, v := range resource {
		if v != "" {
			attrs[k] = v
		}
	}
	// Node services must remain distinguishable when a central exporter receives
	// data from multiple hosts. Run-scoped publishers provide SandboxID themselves.
	if attrs["sandbox.id"] == "" {
		if name, err := os.Hostname(); err == nil && name != "" {
			attrs["host.name"] = name
		}
	}
	attrs["service.instance.id"] = processInstanceID
	attrs["service.name"] = service
	attrs["sandbox.telemetry.source"] = source
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 350 * time.Millisecond}).DialContext(ctx, "unix", socket)
	}}
	return &Publisher{socket: socket, client: &http.Client{Transport: transport, Timeout: 750 * time.Millisecond}, resource: attrs, scope: service}
}

// Available is a best-effort check before a producer performs expensive
// sampling. Sending still checks the actual connect/response.
func (p *Publisher) Available() bool {
	if p == nil {
		return false
	}
	info, err := os.Stat(p.socket)
	return err == nil && info.Mode()&os.ModeSocket != 0
}

// Send is best effort. All points share one Resource identity and retain their
// observation timestamps; no values are re-sampled or re-timestamped here.
func (p *Publisher) Send(ctx context.Context, points []Point) error {
	if p == nil || len(points) == 0 {
		return nil
	}
	if len(points) > 512 {
		return errors.New("too many host telemetry points")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rm := otlpResourceMetrics{}
	rm.Resource.Attributes = attributes(p.resource)
	var scope otlpScopeMetrics
	scope.Scope.Name = p.scope
	index := map[string]*otlpMetric{}
	for _, point := range points {
		if point.Name == "" || point.Time.IsZero() || (point.Uint == nil && (math.IsNaN(point.Value) || math.IsInf(point.Value, 0))) {
			return errors.New("invalid host telemetry point")
		}
		key := point.Name + "\x00" + point.Unit
		metric := index[key]
		if metric == nil {
			metric = &otlpMetric{Name: point.Name, Unit: point.Unit}
			index[key] = metric
			scope.Metrics = append(scope.Metrics, metric)
		}
		timestamp := strconv.FormatInt(point.Time.UnixNano(), 10)
		attrs := attributes(point.Attributes)
		if point.Histogram != nil {
			if metric.Gauge != nil || metric.Sum != nil || point.Uint != nil || point.Counter {
				return errors.New("mixed histogram and numeric host metric")
			}
			if metric.Histogram == nil {
				// OTLP AggregationTemporality.CUMULATIVE.
				metric.Histogram = &otlpHistogram{AggregationTemporality: 2}
			}
			metric.Histogram.DataPoints = append(metric.Histogram.DataPoints, latencyHistogramPoint(point.Histogram, timestamp, attrs))
			continue
		}
		if metric.Histogram != nil {
			return errors.New("mixed histogram and numeric host metric")
		}
		dp := otlpDP{TimeUnixNano: timestamp, Attributes: attrs}
		if point.Uint != nil {
			// OTLP integer data points are signed int64. Large unsigned native
			// counters retain an approximate value rather than invalid JSON.
			if *point.Uint <= math.MaxInt64 {
				dp.AsInt = strconv.FormatUint(*point.Uint, 10)
			} else {
				value := float64(*point.Uint)
				dp.AsDouble = &value
			}
		} else {
			v := point.Value
			dp.AsDouble = &v
		}
		if point.Counter {
			if metric.Gauge != nil || point.Uint == nil && point.Value < 0 {
				return errors.New("invalid cumulative host counter")
			}
			if metric.Sum == nil {
				metric.Sum = &otlpSum{AggregationTemporality: 2, IsMonotonic: true}
			}
			dp.StartTimeUnixNano = strconv.FormatInt(processStart.UnixNano(), 10)
			metric.Sum.DataPoints = append(metric.Sum.DataPoints, dp)
		} else {
			if metric.Sum != nil {
				return errors.New("mixed gauge and cumulative host counter")
			}
			if metric.Gauge == nil {
				metric.Gauge = &otlpGauge{}
			}
			metric.Gauge.DataPoints = append(metric.Gauge.DataPoints, dp)
		}
	}
	rm.ScopeMetrics = []otlpScopeMetrics{scope}
	body, err := json.Marshal(otlpExport{ResourceMetrics: []otlpResourceMetrics{rm}})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/v1/metrics", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("host telemetry HTTP %d", response.StatusCode)
	}
	responseBytes, err := io.ReadAll(io.LimitReader(response.Body, 64<<10+1))
	if err != nil {
		return fmt.Errorf("host telemetry read response: %w", err)
	}
	if len(responseBytes) > 64<<10 {
		return errors.New("host telemetry response too large")
	}
	if len(responseBytes) == 0 {
		return nil
	}
	var result struct {
		PartialSuccess struct {
			RejectedDataPoints json.RawMessage `json:"rejectedDataPoints"`
			ErrorMessage       string          `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	if err := json.Unmarshal(responseBytes, &result); err != nil {
		return fmt.Errorf("host telemetry invalid response: %w", err)
	}
	raw := bytes.Trim(result.PartialSuccess.RejectedDataPoints, "\"")
	if len(raw) > 0 {
		count, err := strconv.ParseUint(string(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("host telemetry invalid rejectedDataPoints: %w", err)
		}
		if count > 0 {
			return fmt.Errorf("host telemetry partial acceptance: rejected=%d: %s", count, result.PartialSuccess.ErrorMessage)
		}
	}
	// A zero rejection count with a message is an accepted request with a warning.
	return nil
}
