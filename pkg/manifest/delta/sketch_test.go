package delta

import (
	"testing"
)

// Checks that sketching the same data twice gives an identical sketch, and that
// a sketch compared with an identical copy estimates similarity 1.
func TestSketchDeterministicAndIdentical(t *testing.T) {
	s, _ := testSketcher(t)
	data := randBytes(1, 256<<10)
	a, b := s.Sketch(data), s.Sketch(append([]byte(nil), data...))
	if a != b {
		t.Fatal("sketch not deterministic")
	}
	if sim := a.Compact().Similarity(b); sim != 1 {
		t.Fatalf("identical chunks: similarity %v, want 1", sim)
	}
}

// Checks that none of 58 unrelated random chunks estimates a similarity of 0.5 or
// more to the base.
func TestSketchUnrelatedIsLow(t *testing.T) {
	s, _ := testSketcher(t)
	a := s.Sketch(randBytes(1, 256<<10)).Compact()
	high := 0
	for seed := int64(2); seed < 60; seed++ {
		if a.Similarity(s.Sketch(randBytes(seed, 256<<10))) >= 0.5 {
			high++
		}
	}
	if high > 0 {
		t.Fatalf("%d/58 unrelated chunks estimated >= 0.5", high)
	}
}

// Checks that the similarity estimate tracks edits. Averaged over 20 trials, two
// small edits must estimate at least 0.8, and 150 edits must estimate lower than
// two.
func TestSketchSimilarityTracksEdits(t *testing.T) {
	s, _ := testSketcher(t)
	base := randBytes(3, 256<<10)
	bs := s.Sketch(base)
	sim := func(edits int) float64 {
		var sum float64
		const trials = 20
		for i := 0; i < trials; i++ {
			sum += bs.Compact().Similarity(s.Sketch(mutate(base, int64(100+i), edits, 64)))
		}
		return sum / trials
	}
	few, many := sim(2), sim(150)
	if few < 0.8 {
		t.Errorf("2 small edits: mean estimate %.2f, want >= 0.8", few)
	}
	if many >= few {
		t.Errorf("more edits must lower the estimate: %.2f !< %.2f", many, few)
	}
}

// Checks that a 4-byte input leaves some of the 16 slots empty, and that identical
// tiny inputs estimate similarity 1. Checks that two empty sketches estimate 0,
// since they share no information, and that a tiny input against an empty sketch
// also estimates 0.
func TestSketchSmallInputHasEmptySlots(t *testing.T) {
	s, _ := testSketcher(t)
	sk := s.Sketch([]byte("tiny"))
	c := sk.Compact()
	if c.Empty == 0 {
		t.Fatal("a 4-byte input cannot fill 16 slots")
	}
	if sim := c.Similarity(sk); sim != 1 {
		t.Fatalf("identical tiny inputs: %v", sim)
	}
	empty := s.Sketch(nil)
	if sim := empty.Compact().Similarity(empty); sim != 0 {
		t.Fatalf("two empty sketches carry no information, got %v", sim)
	}
	if sim := c.Similarity(empty); sim != 0 {
		t.Fatalf("tiny vs empty: %v", sim)
	}
}

// Checks that a full sketch yields the expected number of super-features, none of
// them zero and all different from each other. Checks that a tiny input yields at
// least one unusable (zero) super-feature.
func TestSuperFeatures(t *testing.T) {
	s, p := testSketcher(t)
	data := randBytes(5, 256<<10)
	sk := s.Sketch(data)
	sfs := s.SuperFeatures(sk, nil)
	if len(sfs) != p.SuperFeatures() {
		t.Fatalf("got %d super-features, want %d", len(sfs), p.SuperFeatures())
	}
	seen := map[uint64]bool{}
	for _, f := range sfs {
		if f == 0 {
			t.Fatal("full sketch must yield no empty super-feature")
		}
		if seen[f] {
			t.Fatal("super-features within a chunk must differ (index is mixed in)")
		}
		seen[f] = true
	}
	// A group with an empty slot yields the 0 sentinel rather than a bogus feature.
	tiny := s.SuperFeatures(s.Sketch([]byte("tiny")), nil)
	zero := 0
	for _, f := range tiny {
		if f == 0 {
			zero++
		}
	}
	if zero == 0 {
		t.Fatal("tiny input must have unusable super-features")
	}
}

// Checks the design's central parameter claim by measurement: with group size 2,
// two chunks at piece-set similarity about 0.5 share at least one super-feature
// about 90% of the time, while group size 4 finds only about 23%. The test
// requires at least 80% for group size 2, and that group size 4 recalls less.
func TestSuperFeatureRecall(t *testing.T) {
	recall := func(g int) float64 {
		p := testParams(t, func(c *Config) { c.Sketch.GroupSize = g })
		s, _ := NewSketcher(p)
		hits, trials := 0, 150
		for i := 0; i < trials; i++ {
			base := randBytes(int64(1000+i), 128<<10)
			// Two chunks of equal size sharing a fraction I of their pieces have
			// J = I/(2-I); J = 0.5 needs I = 2/3, i.e. overwrite one third.
			v := append([]byte(nil), base...)
			third := len(base) / 3
			copy(v[40<<10:40<<10+third], randBytes(int64(9000+i), third))
			a := s.SuperFeatures(s.Sketch(base), nil)
			b := s.SuperFeatures(s.Sketch(v), nil)
			shared := false
			for _, x := range a {
				for _, y := range b {
					if x != 0 && x == y {
						shared = true
					}
				}
			}
			if shared {
				hits++
			}
		}
		return float64(hits) / float64(trials)
	}
	g2, g4 := recall(2), recall(4)
	t.Logf("recall at J~0.5: g=2 %.2f, g=4 %.2f", g2, g4)
	if g2 < 0.80 {
		t.Errorf("g=2 recall %.2f too low", g2)
	}
	if g4 >= g2 {
		t.Errorf("larger groups must lower recall: g=4 %.2f vs g=2 %.2f", g4, g2)
	}
}
