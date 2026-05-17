package chunker

import (
	"bytes"
	"crypto/sha256"
	"math/rand"
	"sort"
	"testing"
)

// TestCDCConfigSweep is an analysis tool, not a regression test:
// it runs every (min, avg, max) candidate in cases against the same
// 100 MiB random-data sample and reports a table of quality
// metrics. Skipped under -short. Intended to be run with -v:
//
//	go test -v -count=1 -run TestCDCConfigSweep ./pkg/chunker/
//
// The output column meanings:
//
//	chunks    number of chunks produced
//	avg       observed mean chunk size (KiB)
//	avg/cfg   observed avg ÷ configured avg (closer to 1.0 = better
//	          centred; > 1.0 = chunker tends to overshoot avg)
//	p25 p50 p75 p95   chunk-size percentiles (KiB)
//	IQR/avg   (p75 - p25) / avg, a unitless spread measure (smaller
//	          = tighter distribution)
//	forced%   chunks at exactly maxSize (% of total). High here =
//	          forced cuts; bad for dedup
//	tinyN     chunks ≤ 1.1 × min (heuristic for "phase-1 stuck near
//	          min" pile-up; bad for centring)
//	jaccard   chunk-hash similarity between the original sample and
//	          a copy with a 1 KiB random insertion at offset 50 MiB.
//	          Higher = more chunks survive a single localised edit
//	          = better real-world dedup robustness against insertions
//
// The "best" config depends on which metric the deployment cares
// about most:
//   - For minimum manifest size + maximum dedup density, prefer
//     larger avg (fewer entries per image).
//   - For tightest avg centring, prefer narrower min..max with
//     n2 ≥ 3 (low forced%).
//   - For dedup robustness against incremental edits, prefer wider
//     phase-2 span (paper-style max ≥ 2×avg) — n2=1 keeps cut
//     locations more content-defined and less mask-dictated.
func TestCDCConfigSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CDC config sweep (allocates 100+ MiB)")
	}

	const sampleSize = 100 << 20

	rng := rand.New(rand.NewSource(0xC4DC))
	original := make([]byte, sampleSize)
	for i := range original {
		original[i] = byte(rng.Intn(256))
	}

	// Two insertion sites for sensitivity analysis:
	//   mid    — half-way (50 MiB), affects every chunk past the cut
	//   late   — 90% through (90 MiB), only the trailing few chunks
	//            re-roll; this differentiates resync speed across
	//            configs (faster resync = more chunks preserved).
	insert := make([]byte, 1024)
	rng.Read(insert)
	makeMod := func(offset int) []byte {
		out := make([]byte, sampleSize+len(insert))
		copy(out, original[:offset])
		copy(out[offset:], insert)
		copy(out[offset+len(insert):], original[offset:])
		return out
	}
	modifiedMid := makeMod(sampleSize / 2)
	modifiedLate := makeMod(sampleSize * 9 / 10)

	cases := []struct {
		min, avg, max uint32
		label         string
	}{
		// Vary max ratio (avg fixed at 512K, min fixed at 128K):
		{128 << 10, 512 << 10, 768 << 10, "128K/512K/768K  (tight max=1.5×)"},
		{128 << 10, 512 << 10, 1024 << 10, "128K/512K/1M    (current default, max=2×)"},
		{128 << 10, 512 << 10, 2 << 20, "128K/512K/2M    (max=4×, FastCDC paper-ish)"},
		{128 << 10, 512 << 10, 4 << 20, "128K/512K/4M    (max=8×, very loose)"},
		// Vary min ratio (avg=512K, max=1M):
		{64 << 10, 512 << 10, 1024 << 10, "64K/512K/1M     (old default, min=avg/8)"},
		{256 << 10, 512 << 10, 1024 << 10, "256K/512K/1M    (min=avg/2)"},
		{384 << 10, 512 << 10, 1024 << 10, "384K/512K/1M    (very tight min)"},
		// Tight max plus varying min:
		{64 << 10, 512 << 10, 768 << 10, "64K/512K/768K   (loose min, tight max)"},
		{256 << 10, 512 << 10, 768 << 10, "256K/512K/768K  (tight on both sides)"},
	}

	t.Logf("\n%-50s %5s %7s %7s %5s %5s %5s %5s %7s %7s %5s %5s %5s",
		"config", "n1,n2", "chunks", "avg(K)", "p25", "p50", "p75", "p95",
		"IQR/avg", "forced%", "tiny", "kpMid", "kpLat")
	t.Logf("%s", repeat("─", 138))

	for _, tc := range cases {
		n1, n2 := derivedNormalizationLevel(tc.min, tc.avg, tc.max)
		statsA := chunkStats(t, original, tc.min, tc.avg, tc.max)
		statsMid := chunkStats(t, modifiedMid, tc.min, tc.avg, tc.max)
		statsLate := chunkStats(t, modifiedLate, tc.min, tc.avg, tc.max)
		// "kept" = chunks of the original that survive the modification.
		// As a percentage of original chunk count, this directly reads
		// as "what fraction of dedup state survives a 1 KiB edit".
		keptMid := percentKept(statsA.hashes, statsMid.hashes)
		keptLate := percentKept(statsA.hashes, statsLate.hashes)

		t.Logf("%-50s (%d,%d) %7d %7.0f %5.0f %5.0f %5.0f %5.0f %7.3f %7.2f %5d %5.1f %5.1f",
			tc.label, n1, n2, len(statsA.sizes),
			float64(statsA.avg)/1024,
			float64(statsA.p25)/1024,
			float64(statsA.p50)/1024,
			float64(statsA.p75)/1024,
			float64(statsA.p95)/1024,
			float64(statsA.p75-statsA.p25)/float64(statsA.avg),
			statsA.forcedPct,
			statsA.tinyCount,
			keptMid,
			keptLate,
		)
	}
}

func percentKept(orig, modified map[[32]byte]struct{}) float64 {
	if len(orig) == 0 {
		return 0
	}
	preserved := 0
	for h := range orig {
		if _, ok := modified[h]; ok {
			preserved++
		}
	}
	return 100.0 * float64(preserved) / float64(len(orig))
}

type cdcStats struct {
	sizes     []uint32
	hashes    map[[32]byte]struct{}
	avg       uint32
	p25, p50  uint32
	p75, p95  uint32
	forcedPct float64
	tinyCount int
}

func chunkStats(t *testing.T, data []byte, min, avg, max uint32) cdcStats {
	t.Helper()
	var sizes []uint32
	hashes := make(map[[32]byte]struct{})
	tinyThreshold := uint32(float64(min) * 1.1)
	tinyCount := 0
	forcedCount := 0

	chk := &cdcChunker{min: min, avg: avg, max: max}
	if err := chk.Chunk(bytes.NewReader(data), func(cr ChunkResult) error {
		sizes = append(sizes, cr.Size)
		// Hash the actual content so insertion analysis sees content
		// drift, not just size drift.
		var h [32]byte
		if cr.Data != nil {
			h = sha256.Sum256(cr.Data)
		} else {
			// Zero-chunk: use a sentinel so all zero-chunks of the
			// same size collide, modelling the dedup-collapse path.
			h = sha256.Sum256(append([]byte("ZERO"), byte(cr.Size>>24), byte(cr.Size>>16), byte(cr.Size>>8), byte(cr.Size)))
		}
		hashes[h] = struct{}{}
		if cr.Size <= tinyThreshold {
			tinyCount++
		}
		if cr.Size == max {
			forcedCount++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	sorted := append([]uint32(nil), sizes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum uint64
	for _, s := range sizes {
		sum += uint64(s)
	}
	return cdcStats{
		sizes:     sizes,
		hashes:    hashes,
		avg:       uint32(sum / uint64(len(sizes))),
		p25:       sorted[len(sorted)*25/100],
		p50:       sorted[len(sorted)*50/100],
		p75:       sorted[len(sorted)*75/100],
		p95:       sorted[len(sorted)*95/100],
		forcedPct: 100.0 * float64(forcedCount) / float64(len(sizes)),
		tinyCount: tinyCount,
	}
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}
