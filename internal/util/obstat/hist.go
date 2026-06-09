// Package obstat provides the shared building blocks for the store-ctl /
// cache-ctl periodic, adaptive stats output: a concurrent latency histogram, an
// adaptive ticker that only prints when there was traffic, and compact value
// formatters. The histogram bucket scheme matches sandbox-ctl's uffd/vhost
// stats so latency percentiles are comparable across the platform.
package obstat

import (
	"sync/atomic"
	"time"
)

const numBuckets = 21

// bucketNs are exponential upper bounds from 1µs to 1s; a 22nd implicit
// overflow bucket (index numBuckets) catches anything slower than 1s.
var bucketNs = [numBuckets]uint64{
	1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 512_000,
	1_000_000, 2_000_000, 4_000_000, 8_000_000, 16_000_000, 32_000_000, 64_000_000,
	128_000_000, 256_000_000, 512_000_000, 1_000_000_000,
}

// Hist is a concurrent latency histogram. The zero value is ready to use;
// Record is lock-free (per-bucket atomic add), so request handlers can call it
// directly. Read a consistent view with Snapshot.
type Hist struct {
	buckets [numBuckets + 1]atomic.Uint64 // +1 = overflow (> 1s)
	count   atomic.Uint64
	sumNs   atomic.Uint64
	maxNs   atomic.Uint64
}

// Record adds one latency sample (clamped to >= 0).
func (h *Hist) Record(d time.Duration) {
	var ns uint64
	if d > 0 {
		ns = uint64(d.Nanoseconds())
	}
	h.count.Add(1)
	h.sumNs.Add(ns)
	i := numBuckets
	for j := range numBuckets {
		if ns <= bucketNs[j] {
			i = j
			break
		}
	}
	h.buckets[i].Add(1)
	for { // max via CAS
		cur := h.maxNs.Load()
		if ns <= cur || h.maxNs.CompareAndSwap(cur, ns) {
			break
		}
	}
}

// HistSnapshot is an immutable read of a Hist, or a windowed delta of two.
type HistSnapshot struct {
	Buckets [numBuckets + 1]uint64
	Count   uint64
	SumNs   uint64
	MaxNs   uint64
}

// Snapshot reads the histogram. Buckets are read independently so a Snapshot
// taken under concurrent Record may be slightly skewed across buckets — fine
// for periodic stats, never used for correctness.
func (h *Hist) Snapshot() HistSnapshot {
	var s HistSnapshot
	for i := range h.buckets {
		s.Buckets[i] = h.buckets[i].Load()
	}
	s.Count = h.count.Load()
	s.SumNs = h.sumNs.Load()
	s.MaxNs = h.maxNs.Load()
	return s
}

// Sub returns the per-window delta s-prev (counts/sum/buckets subtract). MaxNs
// is exact when a new peak appeared this window, else the highest non-empty
// bucket's upper bound (coarse) — the cumulative max can't be un-summed.
func (s HistSnapshot) Sub(prev HistSnapshot) HistSnapshot {
	w := HistSnapshot{Count: s.Count - prev.Count, SumNs: s.SumNs - prev.SumNs}
	var hi uint64
	for i := range s.Buckets {
		w.Buckets[i] = s.Buckets[i] - prev.Buckets[i]
		if w.Buckets[i] > 0 {
			if i < numBuckets {
				hi = bucketNs[i]
			} else {
				hi = bucketNs[numBuckets-1] * 2
			}
		}
	}
	if s.MaxNs > prev.MaxNs {
		w.MaxNs = s.MaxNs
	} else {
		w.MaxNs = hi
	}
	return w
}

// Percentile estimates the p-quantile (0..1) by linear interpolation across the
// histogram buckets; 0 when empty, clamped to the observed max.
func (s HistSnapshot) Percentile(p float64) uint64 {
	var total uint64
	for _, v := range s.Buckets {
		total += v
	}
	if total == 0 {
		return 0
	}
	if p < 0 {
		p = 0
	} else if p > 1 {
		p = 1
	}
	target := uint64(float64(total) * p)
	if target == 0 {
		target = 1
	}
	var cum, prevBound, result uint64
	for i, v := range s.Buckets {
		next := cum + v
		var bound uint64
		switch {
		case i < numBuckets:
			bound = bucketNs[i]
		case s.MaxNs > 0:
			bound = s.MaxNs
		default:
			bound = bucketNs[numBuckets-1] * 2
		}
		if next >= target {
			if v == 0 {
				result = bound
			} else {
				result = prevBound + (bound-prevBound)*(target-cum)/v
			}
			break
		}
		cum, prevBound = next, bound
	}
	if s.MaxNs > 0 && result > s.MaxNs {
		return s.MaxNs
	}
	return result
}

func (s HistSnapshot) P50() uint64  { return s.Percentile(0.50) }
func (s HistSnapshot) P99() uint64  { return s.Percentile(0.99) }
func (s HistSnapshot) P999() uint64 { return s.Percentile(0.999) }
