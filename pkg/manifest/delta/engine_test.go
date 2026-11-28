package delta

import (
	"bytes"
	"testing"
)

func newTestEngine(t *testing.T, mut func(*Config)) *Engine {
	t.Helper()
	e, err := NewEngine(testParams(t, mut), testSnapshotKey)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// Checks that a similar base is returned as a candidate with zero non-resident
// candidates while its plaintext is in the pool, and that AcquireBase returns that
// plaintext intact. After the pool copy is removed, the index still returns the
// base, now counted as non-resident, so it can be read back from the store.
func TestEngineCandidatesRequireResidentBase(t *testing.T) {
	e := newTestEngine(t, nil)
	d := e.Domain(key(1))
	base := randBytes(1, 128<<10)
	sk := e.Sketcher().Sketch(base)
	if !d.RegisterBase(BaseInfo{ObjectKey: key(10), BaseKey: key(11), Size: uint32(len(base))}, append([]byte(nil), base...), sk) {
		t.Fatal("register")
	}
	q := e.Sketcher().Sketch(mutate(base, 2, 2, 50))
	got, nr := d.Candidates(q)
	if len(got) != 1 || got[0].ObjectKey != key(10) || nr != 0 {
		t.Fatalf("candidates %+v notResident %d", got, nr)
	}
	plain, rel, ok := d.AcquireBase(got[0].ObjectKey)
	if !ok || !bytes.Equal(plain, base) {
		t.Fatal("base not resident")
	}
	rel()

	// Drop it from the pool only. The index still lists it, and a caller that
	// can read the store back may still use it.
	e.pool.Remove(key(10))
	got, nr = d.Candidates(q)
	if len(got) != 1 || got[0].ObjectKey != key(10) || nr != 1 {
		t.Fatalf("non-resident base must stay findable and be counted: %+v notResident %d", got, nr)
	}
}

// Checks that pool eviction drops plaintext but keeps the similarity-index entry,
// so a later read-back can still use the base. With a 300 KiB pool and four
// 128 KiB bases, all four stay in the index. The oldest base is returned as a
// non-resident candidate, and the newest as a resident one.
func TestEnginePoolEvictionCascadesToIndex(t *testing.T) {
	e := newTestEngine(t, func(c *Config) { c.BasePool.MaxBytes = "300KiB" })
	d := e.Domain(key(1))
	var bases [][]byte
	for i := 0; i < 4; i++ {
		b := randBytes(int64(i), 128<<10)
		bases = append(bases, b)
		d.RegisterBase(BaseInfo{ObjectKey: key(byte(i)), Size: uint32(len(b))}, append([]byte(nil), b...), e.Sketcher().Sketch(b))
	}
	if got := d.sim.Len(); got != 4 {
		t.Fatalf("similarity index has %d bases; eviction must not forget them", got)
	}
	if got, nr := d.Candidates(e.Sketcher().Sketch(bases[0])); len(got) != 1 || nr != 1 {
		t.Fatalf("evicted base should be a non-resident candidate: %+v nr=%d", got, nr)
	}
	if got, nr := d.Candidates(e.Sketcher().Sketch(bases[3])); len(got) != 1 || nr != 0 {
		t.Fatalf("resident base not found: %+v nr=%d", got, nr)
	}
}

// Checks that when the similarity index is full, a displaced base is also removed
// from the pool. With MaxEntries set to 2, registering a third base displaces the
// first. Its plaintext must leave the pool, and two pool entries must remain.
func TestEngineMaxEntriesDisplacementRemovesFromPool(t *testing.T) {
	e := newTestEngine(t, func(c *Config) { c.Index.MaxEntries = 2 })
	d := e.Domain(key(1))
	for i := 0; i < 3; i++ {
		b := randBytes(int64(i), 64<<10)
		d.RegisterBase(BaseInfo{ObjectKey: key(byte(i)), Size: uint32(len(b))}, b, e.Sketcher().Sketch(b))
	}
	if e.pool.Has(key(0)) {
		t.Fatal("displaced base still holds pool memory")
	}
	if n, _ := e.pool.Stats(); n != 2 {
		t.Fatalf("pool entries %d, want 2", n)
	}
}

// Checks that dedup domains do not share state. A known-independent mark and a
// representation entry written in one domain are invisible in another. A base
// registered in one domain is never offered as a candidate in another.
func TestEngineDomainsAreIsolated(t *testing.T) {
	e := newTestEngine(t, nil)
	a, b := e.Domain(key(1)), e.Domain(key(2))
	if a == b || e.Domain(key(1)) != a {
		t.Fatal("domain identity")
	}
	h := key(50)
	a.MarkKnown(h)
	a.RecordRep(h, RepEntry{DeltaObjectKey: key(60), ObjectSize: 7})
	if b.Known(h) {
		t.Fatal("known leaked across domains")
	}
	if _, ok := b.LookupRep(h); ok {
		t.Fatal("representation leaked across domains")
	}
	base := randBytes(1, 64<<10)
	a.RegisterBase(BaseInfo{ObjectKey: key(70)}, base, e.Sketcher().Sketch(base))
	if got, _ := b.Candidates(e.Sketcher().Sketch(mutate(base, 1, 1, 10))); len(got) != 0 {
		t.Fatal("a base from one domain was offered to another")
	}
}

// Checks least-recently-used eviction of domains with MaxDomains set to 2. Touching
// a domain marks it recently used, so creating a new domain evicts the least
// recently used one. When domain 1 is evicted, its pool base is released, and its
// handle refuses new bases.
func TestEngineEvictsLeastRecentlyUsedDomain(t *testing.T) {
	e := newTestEngine(t, func(c *Config) { c.Index.MaxDomains = 2 })
	d1 := e.Domain(key(1))
	base := randBytes(1, 64<<10)
	d1.RegisterBase(BaseInfo{ObjectKey: key(70)}, base, e.Sketcher().Sketch(base))
	e.Domain(key(2))
	e.Domain(key(1)) // touch 1 => 2 is least recently used
	e.Domain(key(3))
	if st := e.Stats(); st.Domains != 2 {
		t.Fatalf("domains %d", st.Domains)
	}
	if e.Domain(key(1)) != d1 {
		t.Fatal("recently used domain was evicted")
	}
	// Now evict d1 itself and check its pool memory is released and it refuses new bases.
	e.Domain(key(4))
	e.Domain(key(5))
	if e.pool.Has(key(70)) {
		t.Fatal("evicted domain's bases still hold pool memory")
	}
	if d1.RegisterBase(BaseInfo{ObjectKey: key(71)}, randBytes(2, 64<<10), e.Sketcher().Sketch(base)) {
		t.Fatal("an evicted domain must not accept new bases")
	}
}

// Checks that a base larger than the whole pool is rejected. A 128 KiB base offered
// to a 64 KiB pool must fail to register and must not appear in the similarity index.
func TestEngineRegisterRejectsOversizeBase(t *testing.T) {
	e := newTestEngine(t, func(c *Config) { c.BasePool.MaxBytes = "64KiB" })
	d := e.Domain(key(1))
	big := randBytes(1, 128<<10)
	if d.RegisterBase(BaseInfo{ObjectKey: key(1)}, big, e.Sketcher().Sketch(big)) {
		t.Fatal("a base larger than the pool must be rejected")
	}
	if d.sim.Len() != 0 {
		t.Fatal("rejected base leaked into the index")
	}
}

// Checks that ForgetRep removes a representation entry, so LookupRep misses afterwards.
func TestEngineForgetRep(t *testing.T) {
	e := newTestEngine(t, nil)
	d := e.Domain(key(1))
	d.RecordRep(key(5), RepEntry{ObjectSize: 1})
	d.ForgetRep(key(5))
	if _, ok := d.LookupRep(key(5)); ok {
		t.Fatal("rep survived Forget")
	}
}
