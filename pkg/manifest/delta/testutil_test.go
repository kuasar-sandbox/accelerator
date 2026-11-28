package delta

import (
	"math/rand"
	"testing"
)

// randBytes returns n deterministic pseudo-random bytes for seed.
func randBytes(seed int64, n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

// mutate returns a copy of b with `edits` random small overwrites of editLen
// bytes each. Deterministic in seed.
func mutate(b []byte, seed int64, edits, editLen int) []byte {
	out := append([]byte(nil), b...)
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < edits; i++ {
		at := r.Intn(len(out) - editLen)
		r.Read(out[at : at+editLen])
	}
	return out
}

// testParams returns validated Params with delta enabled and test-sized limits.
// mut, if non-nil, may change the config before validation.
func testParams(t testing.TB, mut func(*Config)) Params {
	t.Helper()
	c := Config{Enabled: true}
	c.BasePool.MaxBytes = "64MiB"
	c.Index.KnownCapacity = 1 << 14
	c.Index.MaxRepresentations = 1 << 10
	if mut != nil {
		mut(&c)
	}
	p, err := c.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// testSketcher returns a Sketcher built from testParams, and the params used.
func testSketcher(t testing.TB) (*Sketcher, Params) {
	t.Helper()
	p := testParams(t, nil)
	s, err := NewSketcher(p)
	if err != nil {
		t.Fatal(err)
	}
	return s, p
}

// testSnapshotKey stands in for the customer key that seals snapshots.
func testSnapshotKey() ([32]byte, error) { return key(0xC5), nil }

// key returns a structured 32-byte key derived from b. Distinct b give distinct
// keys, but it is not a hash, so the bytes are not random.
func key(b byte) [32]byte {
	var k [32]byte
	for i := range k {
		k[i] = b ^ byte(i*7)
	}
	return k
}
