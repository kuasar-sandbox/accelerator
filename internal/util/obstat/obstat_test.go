package obstat

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestHistPercentilesAndWindow(t *testing.T) {
	var h Hist
	base := h.Snapshot()
	// 1000 samples at ~1ms, plus a 5% tail at 200ms and one 500ms peak.
	for range 1000 {
		h.Record(1 * time.Millisecond)
	}
	for range 50 {
		h.Record(200 * time.Millisecond)
	}
	h.Record(500 * time.Millisecond)

	win := h.Snapshot().Sub(base)
	if win.Count != 1051 {
		t.Fatalf("window count = %d, want 1051", win.Count)
	}
	// p50 sits in the 1ms cluster, well under 10ms.
	if p50 := win.P50(); p50 > 2_000_000 {
		t.Errorf("p50 = %s, want ~1ms", FmtNs(p50))
	}
	// p99 is pulled into the 200ms tail (>= 100ms).
	if p99 := win.P99(); p99 < 100_000_000 {
		t.Errorf("p99 = %s, want tail >= 100ms", FmtNs(p99))
	}
	// max captures the 500ms peak exactly (new peak this window).
	if win.MaxNs != uint64((500 * time.Millisecond).Nanoseconds()) {
		t.Errorf("max = %s, want 500ms", FmtNs(win.MaxNs))
	}
}

func TestHistConcurrentRecord(t *testing.T) {
	var h Hist
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				h.Record(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if got := h.Snapshot().Count; got != 8000 {
		t.Fatalf("count = %d, want 8000", got)
	}
}

func TestRunAdaptiveSilentWhenIdle(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	logf := func(f string, a ...any) {
		mu.Lock()
		lines = append(lines, f)
		mu.Unlock()
	}
	// Alternate active/idle windows; only active ones should print.
	var calls int
	sample := func(_ float64) (string, bool) {
		mu.Lock()
		calls++
		active := calls == 1 // active only on the first window
		mu.Unlock()
		if active {
			return "tick", true
		}
		return "", false
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { RunAdaptive(ctx, 10*time.Millisecond, sample, logf); close(done) }()
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(lines) != 1 {
		t.Fatalf("printed %d lines, want exactly 1 (active window only); got %v", len(lines), lines)
	}
	if calls < 2 {
		t.Errorf("sample called %d times, expected multiple polls", calls)
	}
}

func TestRunAdaptiveDisabled(t *testing.T) {
	called := false
	// base <= 0 must return immediately without ticking.
	RunAdaptive(context.Background(), 0, func(float64) (string, bool) { called = true; return "", false }, func(string, ...any) {})
	if called {
		t.Error("sample should not be called when base <= 0")
	}
}
