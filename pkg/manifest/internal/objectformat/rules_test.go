package objectformat

import (
	"math"
	"testing"
)

// TestDeltaBeneficialBoundaries checks both halves of DeltaBeneficial at their
// edges. A delta must save at least 4 KiB and be at most 80% of the independent
// object. Exact boundaries are accepted and one byte past either limit is
// rejected. A delta larger than the independent object is rejected, and so is a
// zero independent size. The cases near math.MaxUint64 confirm the 80% check
// does not overflow.
func TestDeltaBeneficialBoundaries(t *testing.T) {
	const ki = 1 << 10
	tests := []struct {
		name         string
		indep, delta uint64
		want         bool
	}{
		{"exactly 4KiB and 20%", 20 * ki, 16 * ki, true},
		{"20% but saves 4KiB-1", 20*ki - 1, 16*ki - 1 + 1, false}, // saves only 4095
		{"saves 4KiB but under 20%", 40 * ki, 36 * ki, false},
		{"over 20% but saves under 4KiB", 10 * ki, 6*ki + 1, false},
		{"over 20% and over 4KiB", 100 * ki, 50 * ki, true},
		{"just over the 80% line", 100 * ki, 80*ki + 1, false},
		{"exactly on the 80% line", 100 * ki, 80 * ki, true},
		{"delta larger than independent", 10 * ki, 11 * ki, false},
		{"equal", 100 * ki, 100 * ki, false},
		{"zero independent", 0, 0, false},
		{"huge no overflow", math.MaxUint64, math.MaxUint64 / 2, true},
		{"huge on the line", math.MaxUint64, math.MaxUint64/5*4 + (math.MaxUint64%5*4)/5, true},
		{"huge above the line", math.MaxUint64, math.MaxUint64/5*4 + (math.MaxUint64%5*4)/5 + 1, false},
	}
	for _, tc := range tests {
		if got := DeltaBeneficial(tc.indep, tc.delta); got != tc.want {
			t.Errorf("%s: DeltaBeneficial(%d,%d)=%v want %v", tc.name, tc.indep, tc.delta, got, tc.want)
		}
	}
}

// TestDeltaBeneficialMatchesExactArithmetic checks that the 80% division form
// agrees with direct arithmetic (delta*5 <= indep*4) everywhere that product
// fits in a uint64. It samples independent sizes below 70,000 and several delta
// sizes around each boundary.
func TestDeltaBeneficialMatchesExactArithmetic(t *testing.T) {
	for indep := uint64(0); indep < 70000; indep += 7 {
		for _, delta := range []uint64{0, indep / 2, indep * 4 / 5, indep*4/5 + 1, indep - min(indep, 4096), indep} {
			exact := delta <= indep && indep-delta >= DeltaMinSavings && delta*5 <= indep*4
			if got := DeltaBeneficial(indep, delta); got != exact {
				t.Fatalf("indep=%d delta=%d got %v exact %v", indep, delta, got, exact)
			}
		}
	}
}

// TestDeltaPlausible checks the reader-side bound on a 64 KiB chunk. An object
// exactly 4 KiB under the raw size (plaintext+1) is plausible. Saving only 4095
// bytes, being larger than raw, or a chunk too small to save 4 KiB is not.
func TestDeltaPlausible(t *testing.T) {
	const pt = 64 << 10
	if !DeltaPlausible(pt, pt+1-4096) {
		t.Error("object exactly 4KiB under raw size must be plausible")
	}
	if DeltaPlausible(pt, pt+1-4095) {
		t.Error("object saving only 4095 bytes must be implausible")
	}
	if DeltaPlausible(pt, pt+2) {
		t.Error("object larger than raw must be implausible")
	}
	if DeltaPlausible(100, 50) {
		t.Error("tiny chunk cannot save 4 KiB")
	}
}

// TestCompressionBeneficialUnchanged pins the ordinary chunk compression rule,
// which is separate from deltas. Encoding 16 KiB to 12 KiB is beneficial, and one
// byte more is not. The rule is save at least 4 KiB and encode to at most 75%.
func TestCompressionBeneficialUnchanged(t *testing.T) {
	if !CompressionBeneficial(16<<10, 12<<10) || CompressionBeneficial(16<<10, 12<<10+1) {
		t.Error("compression rule drifted")
	}
}
