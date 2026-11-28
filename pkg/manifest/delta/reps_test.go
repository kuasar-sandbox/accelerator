package delta

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func hashKey(prefix string, i int) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("%s-%d", prefix, i)))
}

// Checks the Bloom filter with 20,000 keys: every added key must be found, and
// the false-positive rate on 50,000 keys never added must stay under 2% (about
// 0.8% is expected at 10 bits per key). The keys are SHA-256 outputs, as in
// production.
func TestBloomNoFalseNegativesAndBoundedFP(t *testing.T) {
	const n = 20000
	b := newBloom(n)
	for i := 0; i < n; i++ {
		b.Add(hashKey("in", i))
	}
	if b.Count() != n {
		t.Fatalf("count %d", b.Count())
	}
	for i := 0; i < n; i++ {
		if !b.Has(hashKey("in", i)) {
			t.Fatalf("false negative for %d", i)
		}
	}
	fp := 0
	const probes = 50000
	for i := 0; i < probes; i++ {
		if b.Has(hashKey("out", i)) {
			fp++
		}
	}
	if rate := float64(fp) / probes; rate > 0.02 {
		t.Fatalf("false positive rate %.4f too high at 10 bits/key (expect ~0.008)", rate)
	}
}

// Checks the representation index's eviction and update rules at capacity 3:
// a lookup refreshes recency, so the least recently used entry is evicted when a
// fourth is added. Overwriting a key replaces its value without growing the
// index, and deleting a key twice is safe.
func TestRepIndexLRU(t *testing.T) {
	r := newRepIndex(3)
	e := func(b byte) RepEntry {
		return RepEntry{DeltaObjectKey: key(b), BaseObjectKey: key(b + 1), ObjectSize: uint32(b)}
	}
	for i := byte(1); i <= 3; i++ {
		r.Put(key(i), e(i))
	}
	if got, ok := r.Lookup(key(1)); !ok || got != e(1) { // touch 1 => 2 is now oldest
		t.Fatalf("lookup 1: %v %+v", ok, got)
	}
	r.Put(key(4), e(4))
	if _, ok := r.Lookup(key(2)); ok {
		t.Fatal("2 should have been evicted as least recently used")
	}
	for _, k := range []byte{1, 3, 4} {
		if _, ok := r.Lookup(key(k)); !ok {
			t.Fatalf("%d should remain", k)
		}
	}
	r.Put(key(1), e(9)) // overwrite
	if got, _ := r.Lookup(key(1)); got != e(9) {
		t.Fatal("overwrite lost")
	}
	if r.Len() != 3 {
		t.Fatalf("len %d", r.Len())
	}
	r.Delete(key(1))
	r.Delete(key(1))
	if _, ok := r.Lookup(key(1)); ok || r.Len() != 2 {
		t.Fatal("delete failed")
	}
}

// Checks that items returns entries oldest first, after a lookup has refreshed one
// of them. Snapshot reload replays entries in this order, so recency survives a
// restart.
func TestRepIndexItemsOrderedOldestFirst(t *testing.T) {
	r := newRepIndex(10)
	for i := byte(1); i <= 4; i++ {
		r.Put(key(i), RepEntry{ObjectSize: uint32(i)})
	}
	r.Lookup(key(1)) // 1 becomes newest
	items := r.items()
	var order []uint32
	for _, it := range items {
		order = append(order, it.ObjectSize)
	}
	want := []uint32{2, 3, 4, 1}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order %v, want %v", order, want)
		}
	}
}
