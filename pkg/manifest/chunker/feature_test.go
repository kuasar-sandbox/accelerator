package chunker

import (
	"bytes"
	"math/rand"
	"testing"
)

func featureRand(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func splitAll(c *FeatureCutter, data []byte) [][]byte {
	var out [][]byte
	c.Split(data, func(p []byte) { out = append(out, append([]byte(nil), p...)) })
	return out
}

// TestFeatureCutterValidation checks that NewFeatureCutter rejects bounds that
// cannot define a cutter: a zero minimum, and minimum, target and maximum that are
// not strictly increasing. Sizes that are not page-aligned must be accepted,
// because feature pieces do not need alignment.
func TestFeatureCutterValidation(t *testing.T) {
	for _, tc := range [][3]uint32{{0, 1024, 4096}, {256, 256, 4096}, {1024, 256, 4096}, {256, 1024, 1024}} {
		if _, err := NewFeatureCutter(tc[0], tc[1], tc[2]); err == nil {
			t.Errorf("%v: expected error", tc)
		}
	}
	if _, err := NewFeatureCutter(256, 1000, 4000); err != nil {
		t.Fatalf("unaligned sizes must be accepted: %v", err)
	}
}

// TestFeatureCutterReassemblesAndBounds checks that splitting is lossless and
// bounded. Joining the pieces must return the input for sizes from empty to just
// over 1 MiB. No piece may exceed the 4 KiB maximum. Every piece except the last
// must be at least the 256-byte minimum.
func TestFeatureCutterReassemblesAndBounds(t *testing.T) {
	c, err := NewFeatureCutter(256, 1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 255, 256, 257, 1023, 4096, 4097, 128 << 10, 1<<20 + 13} {
		data := featureRand(int64(size), size)
		pieces := splitAll(c, data)
		if got := bytes.Join(pieces, nil); !bytes.Equal(got, data) {
			t.Fatalf("size %d: pieces do not reassemble", size)
		}
		for i, p := range pieces {
			if len(p) > 4096 {
				t.Fatalf("size %d piece %d: %d > max", size, i, len(p))
			}
			if i < len(pieces)-1 && len(p) < 256 {
				t.Fatalf("size %d piece %d: %d < min", size, i, len(p))
			}
		}
	}
}

// TestFeatureCutterIsUnaligned checks that most cut points are not page-aligned.
// Over 1 MiB of random data, more than half of the pieces (excluding the tail)
// must have a length that is not a multiple of PageSize. If they were aligned, the
// cutter would be behaving like the storage chunker.
func TestFeatureCutterIsUnaligned(t *testing.T) {
	c, _ := NewFeatureCutter(256, 1024, 4096)
	pieces := splitAll(c, featureRand(7, 1<<20))
	unaligned := 0
	for _, p := range pieces[:len(pieces)-1] {
		if len(p)%PageSize != 0 {
			unaligned++
		}
	}
	if unaligned < len(pieces)/2 {
		t.Fatalf("only %d/%d pieces unaligned; cutter looks page-aligned", unaligned, len(pieces))
	}
}

// TestFeatureCutterAverageNearTarget checks that over 4 MiB of random data the
// average piece length lands between 700 and 1700 bytes, around the 1024-byte
// target.
func TestFeatureCutterAverageNearTarget(t *testing.T) {
	c, _ := NewFeatureCutter(256, 1024, 4096)
	data := featureRand(9, 4<<20)
	pieces := splitAll(c, data)
	avg := float64(len(data)) / float64(len(pieces))
	if avg < 700 || avg > 1700 {
		t.Fatalf("average piece size %.0f far from 1024 target", avg)
	}
}

// TestFeatureCutterResynchronizesAfterInsertion checks the property similarity
// detection relies on. A 25-byte insertion must perturb only the pieces around it,
// and cut points must resynchronize afterward. At least 95% of the modified
// buffer's pieces must match pieces of the original.
func TestFeatureCutterResynchronizesAfterInsertion(t *testing.T) {
	c, _ := NewFeatureCutter(256, 1024, 4096)
	base := featureRand(11, 256<<10)
	mod := append(append(append([]byte(nil), base[:100<<10]...), []byte("0123456789-inserted-bytes")...), base[100<<10:]...)

	set := map[string]struct{}{}
	for _, p := range splitAll(c, base) {
		set[string(p)] = struct{}{}
	}
	pieces := splitAll(c, mod)
	shared := 0
	for _, p := range pieces {
		if _, ok := set[string(p)]; ok {
			shared++
		}
	}
	if frac := float64(shared) / float64(len(pieces)); frac < 0.95 {
		t.Fatalf("only %.2f of pieces survived a 25-byte insertion", frac)
	}
}

// TestFeatureCutterDeterministic checks that two cutters with the same bounds
// split the same input into identical pieces. Manifests depend on this for
// repeatable similarity results.
func TestFeatureCutterDeterministic(t *testing.T) {
	c1, _ := NewFeatureCutter(256, 1024, 4096)
	c2, _ := NewFeatureCutter(256, 1024, 4096)
	data := featureRand(13, 512<<10)
	a, b := splitAll(c1, data), splitAll(c2, data)
	if len(a) != len(b) {
		t.Fatal("different piece counts")
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("piece %d differs", i)
		}
	}
}

// TestFeatureCutterConstantData checks the cutter on 1 MiB of zeros. The gear hash
// never triggers a cut on constant bytes, so pieces run to the maximum. No piece
// may exceed 4 KiB, and the pieces must still cover the input exactly.
func TestFeatureCutterConstantData(t *testing.T) {
	c, _ := NewFeatureCutter(256, 1024, 4096)
	pieces := splitAll(c, make([]byte, 1<<20))
	for _, p := range pieces[:len(pieces)-1] {
		if len(p) > 4096 {
			t.Fatalf("piece %d exceeds max", len(p))
		}
	}
	if len(bytes.Join(pieces, nil)) != 1<<20 {
		t.Fatal("lost bytes")
	}
}
