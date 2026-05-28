package chunker

// makeMask returns a 64-bit value with exactly nbits bits set, placed
// at deterministically-chosen positions across the 64-bit space. Same
// nbits → same mask, every build, every machine.
//
// Bit placement uses a Fisher-Yates shuffle of [0, 63] driven by the
// same LCG constants as the gear table generator (gear.go), so all
// chunker-side determinism flows from the same seed family.
//
// Why deterministic placement matters: the mask gates CDC cut points
// via `hash & mask == 0`. Two independent chunker invocations on the
// same input must hit the same cut points to keep ingest output
// byte-identical (a prerequisite for cross-image dedup at the
// content store).
//
// Why we skip a hand-tuned bit layout: the rolling Gear hash spreads
// each byte's contribution across all 64 bits within ~64 steps, so
// any reasonably-spread mask works equally well in practice. A
// uniform random distribution from a fixed seed is the simplest
// choice that doesn't bias toward any particular content pattern.
func makeMask(nbits int) uint64 {
	if nbits <= 0 {
		return 0
	}
	if nbits >= 64 {
		return ^uint64(0)
	}
	var positions [64]int
	for i := range positions {
		positions[i] = i
	}
	state := uint64(0xC0FFEE)
	for i := 63; i > 0; i-- {
		state = state*6364136223846793005 + 1442695040888963407
		j := int(state>>32) % (i + 1)
		positions[i], positions[j] = positions[j], positions[i]
	}
	var m uint64
	for k := 0; k < nbits; k++ {
		m |= uint64(1) << positions[k]
	}
	return m
}
