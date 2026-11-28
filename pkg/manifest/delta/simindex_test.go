package delta

import (
	"fmt"
	"sync"
	"testing"
)

func newTestIndex(t *testing.T, maxEntries, maxPostings int) (*simIndex, *Sketcher) {
	t.Helper()
	s, _ := testSketcher(t)
	return newSimIndex(s, maxEntries, maxPostings), s
}

func info(b byte, size uint32) BaseInfo {
	return BaseInfo{ObjectKey: key(b), BaseKey: key(b + 100), Size: size}
}

// Checks that a lightly mutated copy of a stored base is returned first, with its
// key, base key, and size intact, and that every result meets the 0.5 similarity
// threshold. Checks that a completely unrelated chunk returns no candidates.
func TestSimIndexFindsSimilarAndIgnoresUnrelated(t *testing.T) {
	x, s := newTestIndex(t, 100, 64)
	base := randBytes(1, 128<<10)
	x.Register(info(1, 128<<10), s.Sketch(base))
	for i := 2; i < 12; i++ {
		x.Register(info(byte(i), 128<<10), s.Sketch(randBytes(int64(i), 128<<10)))
	}

	got := x.Lookup(s.Sketch(mutate(base, 7, 3, 64)), 0.5, 4)
	if len(got) == 0 || got[0].ObjectKey != key(1) {
		t.Fatalf("expected the near-duplicate base first, got %+v", got)
	}
	if got[0].BaseKey != key(101) || got[0].Size != 128<<10 {
		t.Fatalf("candidate lost its metadata: %+v", got[0])
	}
	for _, c := range got {
		if c.Similarity < 0.5 {
			t.Fatalf("candidate below threshold: %+v", c)
		}
	}
	if got := x.Lookup(s.Sketch(randBytes(999, 128<<10)), 0.5, 4); len(got) != 0 {
		t.Fatalf("unrelated chunk matched: %+v", got)
	}
}

// Checks that the closer of two bases ranks first. When two entries have identical
// sketches, the more recently registered one must rank first.
func TestSimIndexRanksBySimilarityThenRecency(t *testing.T) {
	x, s := newTestIndex(t, 100, 64)
	base := randBytes(1, 128<<10)
	near := mutate(base, 2, 1, 64)
	far := mutate(base, 3, 40, 64)
	x.Register(info(1, 1), s.Sketch(far))
	x.Register(info(2, 1), s.Sketch(near))
	got := x.Lookup(s.Sketch(base), 0.1, 4)
	if len(got) < 2 || got[0].ObjectKey != key(2) {
		t.Fatalf("closest base must rank first: %+v", got)
	}

	// Identical sketches tie; the newer registration wins.
	y, _ := newTestIndex(t, 100, 64)
	y.Register(info(5, 1), s.Sketch(base))
	y.Register(info(6, 1), s.Sketch(base))
	got = y.Lookup(s.Sketch(base), 0.5, 4)
	if len(got) != 2 || got[0].ObjectKey != key(6) {
		t.Fatalf("tie must prefer the most recent: %+v", got)
	}
}

// Checks that Lookup returns at most max candidates, even when more match.
func TestSimIndexLookupRespectsMax(t *testing.T) {
	x, s := newTestIndex(t, 100, 64)
	base := randBytes(1, 128<<10)
	for i := 0; i < 10; i++ {
		x.Register(info(byte(i), 1), s.Sketch(base))
	}
	if got := x.Lookup(s.Sketch(base), 0.5, 3); len(got) != 3 {
		t.Fatalf("got %d, want 3", len(got))
	}
}

// Checks that evicting every entry leaves no postings, keys, or list ends behind.
// A second evict of the same key must report false. Re-registering afterwards
// reuses a freed slot, so the slab does not grow.
func TestSimIndexEvictRemovesEveryPosting(t *testing.T) {
	x, s := newTestIndex(t, 100, 64)
	var keys [][32]byte
	for i := 0; i < 20; i++ {
		x.Register(info(byte(i), 1), s.Sketch(randBytes(int64(i), 64<<10)))
		keys = append(keys, key(byte(i)))
	}
	if x.Len() != 20 {
		t.Fatalf("len %d", x.Len())
	}
	for i, k := range keys {
		if !x.Evict(k) {
			t.Fatalf("evict %d: not found", i)
		}
		if x.Evict(k) {
			t.Fatalf("evict %d twice succeeded", i)
		}
	}
	if x.Len() != 0 || len(x.postings) != 0 || len(x.byKey) != 0 || x.head != nilID || x.tail != nilID {
		t.Fatalf("index not empty after evicting everything: len=%d postings=%d byKey=%d", x.Len(), len(x.postings), len(x.byKey))
	}
	// And it is reusable: ids are recycled.
	x.Register(info(1, 1), s.Sketch(randBytes(1, 64<<10)))
	if x.Len() != 1 || len(x.slab) != 20 {
		t.Fatalf("slab not recycled: len=%d slab=%d", x.Len(), len(x.slab))
	}
}

// Checks that with a cap of 3, registering six entries displaces the oldest three
// in order, returns their keys, and removes them from the index.
func TestSimIndexMaxEntriesDisplacesOldest(t *testing.T) {
	x, s := newTestIndex(t, 3, 64)
	var displaced [][32]byte
	for i := 0; i < 6; i++ {
		displaced = append(displaced, x.Register(info(byte(i), 1), s.Sketch(randBytes(int64(i), 64<<10)))...)
	}
	if x.Len() != 3 {
		t.Fatalf("len %d, want 3", x.Len())
	}
	want := [][32]byte{key(0), key(1), key(2)}
	if fmt.Sprint(displaced) != fmt.Sprint(want) {
		t.Fatalf("displaced %v, want oldest first %v", displaced, want)
	}
	if x.Evict(key(0)) {
		t.Fatal("displaced entry still present")
	}
}

// Checks that re-registering an existing key makes it the newest entry, so a later
// displacement removes the next-oldest key instead.
func TestSimIndexReRegisterRefreshes(t *testing.T) {
	x, s := newTestIndex(t, 3, 64)
	for i := 0; i < 3; i++ {
		x.Register(info(byte(i), 1), s.Sketch(randBytes(int64(i), 64<<10)))
	}
	x.Register(info(0, 1), s.Sketch(randBytes(0, 64<<10))) // refresh oldest
	d := x.Register(info(9, 1), s.Sketch(randBytes(9, 64<<10)))
	if len(d) != 1 || d[0] != key(1) {
		t.Fatalf("refreshed entry must survive; displaced %v", d)
	}
	if x.Len() != 3 {
		t.Fatalf("len %d", x.Len())
	}
}

// Checks that with a per-feature cap of 4, twenty identical sketches never leave a
// posting list longer than 4, and that lookup returns the newest entry first.
// Evicting everything afterwards must leave no leftover state.
func TestSimIndexPostingCap(t *testing.T) {
	x, s := newTestIndex(t, 1000, 4)
	base := randBytes(1, 128<<10)
	sk := s.Sketch(base)
	for i := 0; i < 20; i++ {
		x.Register(info(byte(i), 1), sk) // identical sketch => every posting list is shared
	}
	for f, list := range x.postings {
		if len(list) > 4 {
			t.Fatalf("posting list %x has %d entries, cap 4", f, len(list))
		}
	}
	got := x.Lookup(sk, 0.5, 100)
	if len(got) == 0 || len(got) > 4 {
		t.Fatalf("lookup returned %d candidates with cap 4", len(got))
	}
	// Newest registrations are the ones kept.
	if got[0].ObjectKey != key(19) {
		t.Fatalf("newest must be retained: %x", got[0].ObjectKey[:2])
	}
	// Evicting an entry that lost postings to the cap must not corrupt the lists.
	for i := 0; i < 20; i++ {
		x.Evict(key(byte(i)))
	}
	if len(x.postings) != 0 || x.Len() != 0 {
		t.Fatalf("leftover state: postings=%d len=%d", len(x.postings), x.Len())
	}
}

// Checks that concurrent register, lookup, and evict calls from eight goroutines
// leave every posting naming a live entry, and keep the count within the cap. Run
// under go test -race, it also catches data races.
func TestSimIndexConcurrent(t *testing.T) {
	x, s := newTestIndex(t, 50, 16)
	data := make([][]byte, 16)
	for i := range data {
		data[i] = randBytes(int64(i), 64<<10)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := byte((g*37 + i) % 90)
				switch i % 3 {
				case 0:
					x.Register(info(k, 1), s.Sketch(data[int(k)%len(data)]))
				case 1:
					x.Lookup(s.Sketch(data[int(k)%len(data)]), 0.5, 4)
				default:
					x.Evict(key(k))
				}
			}
		}(g)
	}
	wg.Wait()
	// Structural invariant after the storm: every posting names a live entry.
	for f, list := range x.postings {
		for _, id := range list {
			if !x.slab[id].live {
				t.Fatalf("posting %x names dead entry %d", f, id)
			}
		}
	}
	if x.Len() > 50 {
		t.Fatalf("exceeded max entries: %d", x.Len())
	}
}
