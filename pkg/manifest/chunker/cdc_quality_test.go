package chunker

import (
	"bytes"
	"math/bits"
	"math/rand"
	"testing"
)

// TestDerivedNormalizationLevel_AsymmetricCases locks the (n1, n2)
// outputs of derivedNormalizationLevel against representative
// configs. If the formula or bound logic drifts, this catches it
// before the CDCQuality test (which only sees aggregate behaviour).
func TestDerivedNormalizationLevel_AsymmetricCases(t *testing.T) {
	// n2 uses coefficient 6 (e^-6 forced-cut safety margin) — see
	// derivedNormalizationLevel docstring. Expected values reflect
	// that.
	cases := []struct {
		name             string
		min, avg, max    uint32
		wantN1, wantN2   int
		desc             string
	}{
		{
			name: "default 128K/512K/1M", min: 128 << 10, avg: 512 << 10, max: 1024 << 10,
			wantN1: 3, wantN2: 3,
			desc: "p1: log2(10*0.75)=2.91→3   p2: log2(6*1)=log2(6)→3",
		},
		{
			name: "tight max 128K/512K/768K", min: 128 << 10, avg: 512 << 10, max: 768 << 10,
			wantN1: 3, wantN2: 4,
			desc: "p1 same; p2: log2(6*2)=log2(12)→4",
		},
		{
			name: "paper-style loose max", min: 128 << 10, avg: 512 << 10, max: 4 << 20,
			wantN1: 3, wantN2: 1,
			desc: "p1 same; p2 floor at 1 (max=8×avg, span ≫ phase2 mask)",
		},
		{
			name: "tight max + loose min", min: 64 << 10, avg: 512 << 10, max: 600 << 10,
			wantN1: 3, wantN2: 6,
			desc: "p1: 10*0.875=8.75 → t=8 → bits.Len64(7)=3 (slightly understates true 4 by integer truncation; phase-1 cut rate ≈ 10.4% vs 10% target — tolerable)   p2: log2(6*512/88)=log2(34.9)→6",
		},
		{
			name: "very tight max", min: 128 << 10, avg: 512 << 10, max: 640 << 10,
			wantN1: 3, wantN2: 5,
			desc: "p1 same; p2: log2(6*512/128)=log2(24)→5",
		},
		{
			name: "tight min", min: 384 << 10, avg: 512 << 10, max: 1024 << 10,
			wantN1: 1, wantN2: 3,
			desc: "p1: log2(10*0.25)=1.32 → t=2 → bits.Len64(1)=1   p2: log2(6)=3",
		},
		{
			name: "very loose min", min: 16 << 10, avg: 512 << 10, max: 4 << 20,
			wantN1: 4, wantN2: 1,
			desc: "p1: log2(10*0.969)=3.28→4   p2: paper-style floor",
		},
		{
			name: "degenerate max=avg", min: 128 << 10, avg: 512 << 10, max: 512 << 10,
			wantN1: 3, wantN2: 1,
			desc: "max≤avg falls into floor branch for p2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n1, n2 := derivedNormalizationLevel(tc.min, tc.avg, tc.max)
			if n1 != tc.wantN1 || n2 != tc.wantN2 {
				t.Errorf("derivedNormalizationLevel(%d, %d, %d) = (%d, %d), want (%d, %d) — %s",
					tc.min, tc.avg, tc.max, n1, n2, tc.wantN1, tc.wantN2, tc.desc)
			}
		})
	}
}

// TestMakeMask_BitCount — deterministic mask carries exactly nbits.
// Locks the contract that fastcdc relies on for centring observed
// avg on the configured avg.
func TestMakeMask_BitCount(t *testing.T) {
	for n := 0; n <= 64; n++ {
		got := bits.OnesCount64(makeMask(n))
		if got != n {
			t.Errorf("makeMask(%d): got %d bits set, want %d", n, got, n)
		}
	}
}

// TestMakeMask_Deterministic — same input → same output across calls.
func TestMakeMask_Deterministic(t *testing.T) {
	for n := 5; n <= 25; n += 5 {
		first := makeMask(n)
		for i := range 5 {
			if got := makeMask(n); got != first {
				t.Errorf("makeMask(%d) round %d drift: %x vs %x", n, i, got, first)
			}
		}
	}
}

// TestCDCQuality_AvgMatchesConfig — the central regression guard.
// Across three avg targets, observed avg must be within ±20% of
// configured avg, and forced-max-pinned cuts must be < 5% of all
// chunks. Failure here usually means the mask derivation or
// normalization-level shifted away from "centred on avgSize".
func TestCDCQuality_AvgMatchesConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CDC quality (allocates 64 MiB random data)")
	}

	// Use math/rand with a fixed seed so the test is reproducible —
	// CDC behaviour against random content varies less than 0.5% on
	// re-seeds at this sample size, but pinning the seed makes the
	// occasional flaky tail call deterministic.
	const sampleSize = 64 << 20
	rng := rand.New(rand.NewSource(0xC4DC))
	data := make([]byte, sampleSize)
	for i := range data {
		data[i] = byte(rng.Intn(256))
	}

	cases := []struct {
		name             string
		min, avg, max    uint32
		minObservedRatio float64 // ≥
		maxObservedRatio float64 // ≤
	}{
		{"256K avg", 64 << 10, 256 << 10, 512 << 10, 0.80, 1.20},
		{"512K avg (default)", 128 << 10, 512 << 10, 1024 << 10, 0.80, 1.20},
		{"1M avg", 256 << 10, 1024 << 10, 2048 << 10, 0.80, 1.20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chk := &cdcChunker{min: tc.min, avg: tc.avg, max: tc.max}
			var sizes []uint32
			err := chk.Chunk(bytes.NewReader(data), func(cr ChunkResult) error {
				sizes = append(sizes, cr.Size)
				return nil
			})
			if err != nil {
				t.Fatalf("Chunk: %v", err)
			}
			if len(sizes) < 50 {
				t.Fatalf("only %d chunks — sample too small to evaluate", len(sizes))
			}

			var sum uint64
			forcedMax := 0
			for _, s := range sizes {
				sum += uint64(s)
				if s == tc.max {
					forcedMax++
				}
			}
			obsAvg := float64(sum) / float64(len(sizes))
			ratio := obsAvg / float64(tc.avg)
			forcedRate := float64(forcedMax) / float64(len(sizes))

			t.Logf("chunks=%d  observed avg=%.0f  configured avg=%d  ratio=%.3f  forced-max=%.2f%%",
				len(sizes), obsAvg, tc.avg, ratio, forcedRate*100)

			if ratio < tc.minObservedRatio || ratio > tc.maxObservedRatio {
				t.Errorf("observed/configured avg ratio = %.3f, want [%.2f, %.2f]",
					ratio, tc.minObservedRatio, tc.maxObservedRatio)
			}
			if forcedRate > 0.05 {
				t.Errorf("forced-max-pinned rate = %.2f%%, want ≤ 5%%", forcedRate*100)
			}
		})
	}
}
