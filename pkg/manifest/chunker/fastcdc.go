package chunker

import (
	"io"
	"math/bits"
)

// readBufSize is the size of the read buffer used to minimize syscalls.
const readBufSize = 4 * 1024 * 1024 // 4 MiB

// derivedNormalizationLevel computes asymmetric FastCDC normalization
// offsets from the chunker bounds:
//
//	bits_S = avgBits + n1   (phase-1 mask, used in [min, avg))
//	bits_L = avgBits - n2   (phase-2 mask, used in [avg, max])
//
// FastCDC's classical normalization uses one symmetric level N where
// n1 == n2 — that's a simplifying convention, not a mathematical
// requirement. Phase 1 and phase 2 have independent goals (suppress
// early cuts vs avoid forced cuts), so we size each side just tight
// enough to meet its own constraint and no tighter. This preserves
// natural CDC variance — important for dedup robustness against
// insertions — when one side has plenty of span available.
//
//  1. n1 (phase 1 — keep early cuts ≤ ~10%):
//
//     P(p1_cut) = 1 - exp(-(avg-min) / 2^(avgBits + n1)) ≤ 0.10
//     ⇒ n1 ≥ log2(9.5 × (avg - min) / avg)
//
//  2. n2 (phase 2 — keep forced-max cuts ≤ ~0.25%, i.e. e^-6):
//
//     P(forced) ≈ exp(-(max - avg) / 2^(avgBits - n2)) ≤ 0.0025
//     ⇒ n2 ≥ log2(6 × avg / (max - avg))
//
//     The coefficient 6 (vs the natural 4 from the e^-4 ≈ 1.8% target)
//     adds one bit of safety margin in phase 2: with K=4 a tight-max
//     128K/512K/768K config sees observed avg drift to ~1.09× configured
//     because phase 2 cut mean = 2^(avgBits-3) = 64K → mean chunk =
//     avg+64K. Bumping K to 6 lifts n2 from 3 to 4 for that config,
//     halves the phase 2 mean cut offset to 32K, and centres observed
//     avg on ~1.04× configured. The default 128K/512K/1M (max=2×avg)
//     sits at n2=3 unaffected by the K change, since its larger phase 2
//     span keeps the floor binding regardless of K∈[4,8].
//
// Bounded to [1, 10]. Lower bound 1 keeps a meaningful split between
// the two masks (n=0 collapses both to avgBits, killing the
// normalization mechanism). Upper bound 10 keeps mask bit count
// within the 64-bit hash space; only triggers for pathological max ≈
// avg configs.
//
// Examples:
//   - 128K/512K/1M   (default, max=2×):       (3, 3)
//   - 128K/512K/768K (tight max, 1.5×):       (3, 4)
//   - 128K/512K/4M   (paper-style loose max): (3, 1)
//   - 64K/512K/600K  (tight max, loose min):  (3, 6)
//
// The n1 formula's true coefficient is 1/(-ln 0.9) ≈ 9.49 (the value
// that makes P(p1_cut) hit exactly 10%). Using an integer multiplier
// of 10 stays in integer arithmetic and is slightly stricter than
// 9.49 — the half-bit difference doesn't change ceil(log2) outcomes
// for any realistic min/avg ratio. A multiplier of 9 (one bit looser)
// would underestimate n1 by 1 for spans near the bit-boundary, e.g.
// avg-min ≈ 0.97×avg.
func derivedNormalizationLevel(minSize, avgSize, maxSize uint32) (n1, n2 int) {
	n1 = 1
	if avgSize > 0 && minSize < avgSize {
		span := uint64(avgSize) - uint64(minSize)
		t := (10 * span) / uint64(avgSize)
		if t >= 2 {
			n1 = bits.Len64(t - 1)
		}
	}

	n2 = 1
	if avgSize > 0 && maxSize > avgSize {
		span := uint64(maxSize) - uint64(avgSize)
		t := (6 * uint64(avgSize)) / span
		if t >= 2 {
			n2 = bits.Len64(t - 1)
		}
	}

	return clampN(n1), clampN(n2)
}

func clampN(n int) int {
	if n < 1 {
		return 1
	}
	if n > 10 {
		return 10
	}
	return n
}

// cdcMasks holds the two Gear-hash match masks derived from the
// caller's average chunk size: one harder mask (more bits set, lower
// match probability) for phase 1 in [min, avg), and one easier mask
// for phase 2 in [avg, max]. Centring observed chunk size on the
// configured avg requires the bit counts to track avgSize:
//
//	avgBits = ceil(log2(avgSize))
//	small   = makeMask(avgBits + N)
//	large   = makeMask(avgBits - N)
//
// Hardcoded constants would only be calibrated for a single avgSize
// — every other config would silently drift away from its target,
// which is exactly the bug this struct fixes.
type cdcMasks struct {
	small uint64
	large uint64
}

// deriveMasks computes (small, large) from the chunker bounds.
// Each side's bit count is sized independently from its phase span
// (see derivedNormalizationLevel) so neither mask is over- or
// under-tightened.
func deriveMasks(minSize, avgSize, maxSize uint32) cdcMasks {
	if avgSize == 0 {
		// Defensive: caller validates this elsewhere, but make the
		// math safe rather than panicking on log2(0).
		return cdcMasks{small: ^uint64(0), large: 0}
	}
	avgBits := bits.Len32(avgSize - 1) // ceil(log2(avgSize))
	n1, n2 := derivedNormalizationLevel(minSize, avgSize, maxSize)
	return cdcMasks{
		small: makeMask(avgBits + n1),
		large: makeMask(avgBits - n2),
	}
}

// chunkCDC performs normalized two-phase FastCDC chunking.
//
// Phase 1 (minSize to avgSize): use the harder small mask.
// Phase 2 (avgSize to maxSize): use the easier large mask.
// Hard cut at maxSize.
//
// Masks are derived from avgSize so observed chunk size matches the
// caller's intent. Zero-chunk optimization: if all bytes in a chunk
// are zero, IsZero is set to true and Data is nil.
func chunkCDC(r io.Reader, minSize, avgSize, maxSize uint32, cb Callback) error {
	masks := deriveMasks(minSize, avgSize, maxSize)
	// buf holds buffered input data; pending[0:pendingN] is the unconsumed portion.
	buf := make([]byte, readBufSize+int(maxSize))
	pendingN := 0
	fileOffset := uint64(0)
	eof := false

	for {
		// Fill the buffer if we have room and haven't hit EOF.
		if !eof && pendingN < int(maxSize) {
			n, err := io.ReadAtLeast(r, buf[pendingN:], 1)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					eof = true
				} else {
					return err
				}
			}
			pendingN += n
		}

		// Nothing left to process.
		if pendingN == 0 {
			return nil
		}

		// Determine the chunk boundary within buf[0:pendingN].
		limit := pendingN
		if limit > int(maxSize) {
			limit = int(maxSize)
		}

		cutPoint := cdcCutPoint(buf[:limit], minSize, avgSize, maxSize, masks)

		// Detect zero chunk.
		isZero := isAllZero(buf[:cutPoint])

		var data []byte
		if !isZero {
			data = make([]byte, cutPoint)
			copy(data, buf[:cutPoint])
		}

		if err := cb(ChunkResult{
			Offset: fileOffset,
			Size:   uint32(cutPoint),
			Data:   data,
			IsZero: isZero,
		}); err != nil {
			return err
		}

		fileOffset += uint64(cutPoint)

		// Slide the buffer: move unconsumed bytes to front.
		remaining := pendingN - cutPoint
		if remaining > 0 {
			copy(buf, buf[cutPoint:pendingN])
		}
		pendingN = remaining
	}
}

// cdcCutPoint finds the CDC boundary within data using normalized
// two-phase Gear-hash chunking. Masks must be derived from the
// caller's avgSize via deriveMasks for the chunker to centre on
// the configured average.
func cdcCutPoint(data []byte, minSize, avgSize, maxSize uint32, masks cdcMasks) int {
	n := len(data)
	if n <= int(minSize) {
		return n
	}

	// Phase 1: scan from minSize to min(avgSize, n) using the small mask.
	phase1End := int(avgSize)
	if phase1End > n {
		phase1End = n
	}

	var hash uint64
	for i := int(minSize); i < phase1End; i++ {
		hash = gearHash(hash, data[i])
		if hash&masks.small == 0 {
			return alignDown(i+1, int(minSize))
		}
	}

	// Phase 2: scan from avgSize to min(maxSize, n) using the large mask.
	phase2End := int(maxSize)
	if phase2End > n {
		phase2End = n
	}

	for i := phase1End; i < phase2End; i++ {
		hash = gearHash(hash, data[i])
		if hash&masks.large == 0 {
			return alignDown(i+1, int(minSize))
		}
	}

	// Hard cut at maxSize (or end of data if shorter).
	return phase2End
}

// alignDown rounds pos down to the nearest PageSize boundary.
// If the aligned value would be less than minSize, returns pos unchanged
// (the last chunk of a stream may not be page-aligned).
func alignDown(pos, minSize int) int {
	aligned := pos &^ (PageSize - 1)
	if aligned >= minSize {
		return aligned
	}
	return pos
}

// isAllZero reports whether every byte in b is zero.
func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
