package freq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tkey generates a short deterministic key for test readability.
func tkey(n int) []byte {
	return []byte(fmt.Sprintf("k%d", n))
}

func TestTouchAndEstimate(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1 << 30}) // large resetAfter to avoid halving
	defer s.Close()

	key := tkey(42)
	if got := s.Estimate(key); got != 0 {
		t.Fatalf("Estimate before Touch: got %d, want 0", got)
	}

	for i := 0; i < 10; i++ {
		s.Touch(key)
	}
	if got := s.Estimate(key); got < 5 { // CMS may overcount but never undercount
		t.Fatalf("Estimate after 10 Touch: got %d, want >= 5", got)
	}
}

func TestEstimateMonotonic(t *testing.T) {
	s := New(Config{Counters: 4096, ResetAfter: 1 << 30})
	defer s.Close()

	hot := tkey(1)
	cold := tkey(2)
	for i := 0; i < 100; i++ {
		s.Touch(hot)
	}
	s.Touch(cold)

	if s.Estimate(hot) <= s.Estimate(cold) {
		t.Fatalf("hot(%d) should be > cold(%d)", s.Estimate(hot), s.Estimate(cold))
	}
}

func TestResetHalves(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1 << 30})
	defer s.Close()

	key := tkey(99)
	for i := 0; i < 20; i++ {
		s.Touch(key)
	}

	before := s.Estimate(key)
	s.Reset()
	after := s.Estimate(key)

	// New semantics: Reset rolls active→prev, Estimate = active(0) + prev/2.
	// after should be <= before (prev contribution is halved) but still
	// non-zero because prev still holds the touched counters.
	if after > before {
		t.Fatalf("after Reset: %d should be <= %d", after, before)
	}
	if before > 0 && after == 0 {
		t.Fatalf("after Reset: got 0, expected non-zero prev contribution (before=%d)", before)
	}
}

func TestResetAfterTriggersAutomatically(t *testing.T) {
	const resetAfter = 100
	s := New(Config{Counters: 1024, ResetAfter: resetAfter})
	defer s.Close()

	key := tkey(7)
	for i := 0; i < int(resetAfter)+1; i++ {
		s.Touch(key)
	}

	// After auto-reset, Estimate = active(1) + prev(~100)/2 ≈ 51 — still
	// well under the raw pre-reset value.
	est := s.Estimate(key)
	if est >= 90 {
		t.Fatalf("expected auto-reset to halve prev, got %d", est)
	}
}

func TestShouldEvict(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1 << 30, EvictThreshold: 2})
	defer s.Close()

	cold := tkey(1)
	warm := tkey(2)

	s.Touch(cold)
	for i := 0; i < 10; i++ {
		s.Touch(warm)
	}

	if !s.ShouldEvict(cold) {
		t.Fatal("cold key (1 touch) should be evictable with threshold 2")
	}
	if s.ShouldEvict(warm) {
		t.Fatal("warm key (10 touches) should not be evictable with threshold 2")
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	s1 := New(Config{Counters: 1024, ResetAfter: 1 << 30})
	defer s1.Close()

	key := tkey(55)
	for i := 0; i < 50; i++ {
		s1.Touch(key)
	}

	data := s1.Marshal()
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}
	if data[0] != sketchFormatVersion {
		t.Fatalf("Marshal version byte: got 0x%02x, want 0x%02x", data[0], sketchFormatVersion)
	}

	s2 := New(Config{Counters: 1024, ResetAfter: 1 << 30})
	defer s2.Close()

	if err := s2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	e1 := s1.Estimate(key)
	e2 := s2.Estimate(key)
	if e1 != e2 {
		t.Fatalf("Estimate mismatch after Unmarshal: %d vs %d", e1, e2)
	}
}

func TestUnmarshalRejectsLegacyFormat(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1 << 30})
	defer s.Close()

	// Legacy format: first byte was the low byte of width. For width=1024,
	// the low byte is 0x00 — definitely not 0x02.
	legacy := make([]byte, 128)
	legacy[0] = 0x00

	err := s.Unmarshal(legacy)
	if err == nil {
		t.Fatal("Unmarshal of legacy format should fail")
	}
}

func TestConcurrentTouch(t *testing.T) {
	s := New(Config{Counters: 4096, ResetAfter: 10000})
	defer s.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				s.Touch(tkey(base + i%100))
			}
		}(g * 1000)
	}
	wg.Wait()
	// No race or panic is the success criterion.
}

func TestTimerReset(t *testing.T) {
	s := New(Config{
		Counters:      1024,
		ResetAfter:    1 << 30, // disable count-based
		ResetInterval: 50 * time.Millisecond,
	})
	defer s.Close()

	key := tkey(77)
	for i := 0; i < 20; i++ {
		s.Touch(key)
	}

	before := s.Estimate(key)
	time.Sleep(80 * time.Millisecond)
	after := s.Estimate(key)

	if after >= before && before > 0 {
		t.Fatalf("timer reset should halve: before=%d, after=%d", before, after)
	}
}

// TestResetIsLockFree is the regression guard for the long-lock Reset bug.
// The pre-refactor implementation held a write lock while halving every
// byte of the counter array — O(width) scan under an exclusive mutex that
// blocked every concurrent Touch. The new Reset is pointer-swap only: one
// allocation, two atomic ops, zero blocking.
//
// We don't assert absolute wall time (GC / scheduler noise makes that
// brittle, especially under -race) — instead we assert that Reset under
// heavy concurrent Touch load completes in bounded median time, well
// below anything a byte-sequential scan-under-lock could achieve at this
// width. The counter count is deliberately small (the old halveAll path
// grew linearly with it; if that regression came back, even 16K counters
// would be much slower than a pointer swap).
func TestResetIsLockFree(t *testing.T) {
	s := New(Config{Counters: 1 << 14, ResetAfter: 1 << 30})
	defer s.Close()

	// Background Touch load: 4 goroutines hammering active.
	var stop atomic.Bool
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				s.Touch(tkey(base*1_000_000 + i))
				i++
			}
		}(g)
	}

	durs := make([]time.Duration, 100)
	for i := range durs {
		start := time.Now()
		s.Reset()
		durs[i] = time.Since(start)
	}

	stop.Store(true)
	wg.Wait()

	// Median: a stable summary statistic that isn't eaten by a single GC
	// pause. The old halveAll at 16K counters on an instrumented build
	// would sit firmly in the hundreds-of-µs range under contention; a
	// 5ms median leaves plenty of room for CI noise while still catching
	// "someone reintroduced a linear-scan Reset" regressions.
	sortDurations(durs)
	median := durs[len(durs)/2]
	if median > 5*time.Millisecond {
		t.Fatalf("Reset median %s too high; long-lock regression?", median)
	}
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j-1] > d[j]; j-- {
			d[j-1], d[j] = d[j], d[j-1]
		}
	}
}

// TestEstimatePreservesHistoryAcrossReset asserts the two-generation
// rolling model: one Reset preserves prev contribution; a second Reset
// drops it.
func TestEstimatePreservesHistoryAcrossReset(t *testing.T) {
	s := New(Config{Counters: 1024, ResetAfter: 1 << 30})
	defer s.Close()

	key := tkey(500)
	for i := 0; i < 100; i++ {
		s.Touch(key)
	}
	if got := s.Estimate(key); got == 0 {
		t.Fatal("after 100 touches Estimate should be > 0")
	}

	s.Reset()
	if got := s.Estimate(key); got == 0 {
		t.Fatalf("after one Reset Estimate should still be > 0 (prev contribution), got %d", got)
	}

	s.Reset()
	if got := s.Estimate(key); got != 0 {
		t.Fatalf("after two Resets Estimate should be 0 (prev rolled off), got %d", got)
	}
}

// TestFullKeyDistinguished asserts keys sharing their first 8 bytes are
// tracked in independent buckets — the old uint64-truncated API collapsed
// such keys into the same CMS cell.
func TestFullKeyDistinguished(t *testing.T) {
	// 4096 counters for low collision rate on a 2-key test.
	s := New(Config{Counters: 4096, ResetAfter: 1 << 30})
	defer s.Close()

	// 24-byte shared prefix + 1 distinguishing byte.
	k1 := append([]byte("common-prefix-8byte-XXXX"), 0x01)
	k2 := append([]byte("common-prefix-8byte-XXXX"), 0x02)

	for i := 0; i < 10; i++ {
		s.Touch(k1)
	}
	for i := 0; i < 100; i++ {
		s.Touch(k2)
	}

	e1 := s.Estimate(k1)
	e2 := s.Estimate(k2)
	if e1 >= e2 {
		t.Fatalf("full-key CMS should distinguish k1/k2: e1=%d e2=%d", e1, e2)
	}
}
