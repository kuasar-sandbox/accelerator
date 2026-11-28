package delta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func snapEngine(t *testing.T, path string, mut func(*Config)) *Engine {
	t.Helper()
	return newTestEngine(t, func(c *Config) {
		c.Index.SnapshotPath = path
		if mut != nil {
			mut(c)
		}
	})
}

func populate(e *Engine) {
	d := e.Domain(key(1))
	for i := byte(0); i < 50; i++ {
		d.MarkKnown(key(i))
		d.RecordRep(key(i+100), RepEntry{DeltaObjectKey: key(i + 1), BaseObjectKey: key(i + 2), ObjectSize: uint32(i) + 1000, BaseSize: uint32(i) + 5000})
	}
	e.Domain(key(2)).MarkKnown(key(77))
}

// Checks that a reload restores every known mark and representation entry, keeps
// the two domains separate, and reports two domains and 50 representations.
func TestSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "snap")
	e1 := snapEngine(t, path, nil)
	populate(e1)
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}

	e2 := snapEngine(t, path, nil)
	if err := e2.LoadErr(); err != nil {
		t.Fatal(err)
	}
	d := e2.Domain(key(1))
	for i := byte(0); i < 50; i++ {
		if !d.Known(key(i)) {
			t.Fatalf("known %d lost", i)
		}
		got, ok := d.LookupRep(key(i + 100))
		want := RepEntry{DeltaObjectKey: key(i + 1), BaseObjectKey: key(i + 2), ObjectSize: uint32(i) + 1000, BaseSize: uint32(i) + 5000}
		if !ok || got != want {
			t.Fatalf("rep %d: ok=%v got %+v", i, ok, got)
		}
	}
	if !e2.Domain(key(2)).Known(key(77)) {
		t.Fatal("second domain lost")
	}
	if e2.Domain(key(2)).Known(key(0)) {
		t.Fatal("domains merged on reload")
	}
	if st := e2.Stats(); st.Domains != 2 || st.Representations != 50 {
		t.Fatalf("stats %+v", st)
	}
}

// Checks that a missing snapshot file is a clean start, not an error.
func TestSnapshotMissingFileIsNotAnError(t *testing.T) {
	e := snapEngine(t, filepath.Join(t.TempDir(), "absent"), nil)
	if e.LoadErr() != nil {
		t.Fatal(e.LoadErr())
	}
}

// Checks that a snapshot written under different slots, group size, feature size,
// or known capacity is rejected with ErrSnapshotMismatch, and the engine starts empty.
func TestSnapshotRejectedWhenParametersDiffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	e1 := snapEngine(t, path, nil)
	populate(e1)
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*Config){
		"slots":    func(c *Config) { c.Sketch.Slots = 32 },
		"group":    func(c *Config) { c.Sketch.GroupSize = 4 },
		"features": func(c *Config) { c.FeatureChunker.Avg = "2KiB" },
		"capacity": func(c *Config) { c.Index.KnownCapacity = 1 << 15 },
	} {
		e := snapEngine(t, path, mut)
		if !errors.Is(e.LoadErr(), ErrSnapshotMismatch) {
			t.Errorf("%s: want ErrSnapshotMismatch, got %v", name, e.LoadErr())
		}
		if e.Stats().Domains != 0 || e.Domain(key(1)).Known(key(1)) {
			t.Errorf("%s: a rejected snapshot must leave the engine empty", name)
		}
	}
}

// Checks that nine tampered variants of a snapshot (flipped bytes, truncation,
// trailing bytes, bad magic or version, empty file) each fail with
// ErrSnapshotCorrupt, and that none of them partly loads.
func TestSnapshotRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	e1 := snapEngine(t, path, nil)
	populate(e1)
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	orig, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	corrupt := map[string]func([]byte) []byte{
		"flipped byte in reps":  func(b []byte) []byte { b[len(b)-60] ^= 1; return b },
		"flipped byte in bloom": func(b []byte) []byte { b[100] ^= 0x80; return b },
		"flipped checksum":      func(b []byte) []byte { b[len(b)-1] ^= 1; return b },
		"truncated mid-record":  func(b []byte) []byte { return b[:len(b)-70] },
		"truncated checksum":    func(b []byte) []byte { return b[:len(b)-5] },
		"trailing garbage":      func(b []byte) []byte { return append(b, 0) },
		"bad magic":             func(b []byte) []byte { b[0] = 'X'; return b },
		"bad version":           func(b []byte) []byte { b[8] = 9; return b },
		"empty":                 func(b []byte) []byte { return nil },
	}
	for name, fn := range corrupt {
		bad := fn(append([]byte(nil), orig...))
		if err := os.WriteFile(path, bad, 0o600); err != nil {
			t.Fatal(err)
		}
		e := snapEngine(t, path, nil)
		if !errors.Is(e.LoadErr(), ErrSnapshotCorrupt) {
			t.Errorf("%s: want ErrSnapshotCorrupt, got %v", name, e.LoadErr())
		}
		if e.Stats().Domains != 0 {
			t.Errorf("%s: corrupt snapshot partially loaded", name)
		}
	}
}

// Checks that recency survives a reload: after a restore, the next insert evicts
// the same least recently used entry it would have before the snapshot.
func TestSnapshotPreservesRecencyOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	e1 := snapEngine(t, path, func(c *Config) { c.Index.MaxRepresentations = 3 })
	d := e1.Domain(key(1))
	for i := byte(1); i <= 3; i++ {
		d.RecordRep(key(i), RepEntry{ObjectSize: uint32(i)})
	}
	d.LookupRep(key(1)) // order oldest->newest: 2,3,1
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	e2 := snapEngine(t, path, func(c *Config) { c.Index.MaxRepresentations = 3 })
	d2 := e2.Domain(key(1))
	d2.RecordRep(key(9), RepEntry{ObjectSize: 9}) // must evict 2, the oldest
	if _, ok := d2.LookupRep(key(2)); ok {
		t.Fatal("recency order not preserved across snapshot")
	}
	for _, k := range []byte{1, 3, 9} {
		if _, ok := d2.LookupRep(key(k)); !ok {
			t.Fatalf("%d missing", k)
		}
	}
}

// Checks the write policy. A clean engine writes nothing. The first snapshot is
// written at once. Within the interval a request is skipped and the engine stays
// dirty. Close always writes. After the interval, MaybeSnapshot writes again.
func TestMaybeSnapshotPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	e := snapEngine(t, path, func(c *Config) { c.Index.SnapshotInterval = "1h" })
	now := time.Unix(1000, 0)
	e.now = func() time.Time { return now }

	if err := e.MaybeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("a clean engine must not write")
	}

	e.Domain(key(1)).MarkKnown(key(1))
	if err := e.MaybeSnapshot(); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("the first snapshot of a process must be written immediately:", err)
	}

	e.Domain(key(1)).MarkKnown(key(2))
	now = now.Add(time.Minute)
	if err := e.MaybeSnapshot(); err != nil {
		t.Fatal(err)
	}
	if again, _ := os.ReadFile(path); !bytes.Equal(again, first) {
		t.Fatal("snapshot rewritten inside the interval")
	}
	if !e.dirty.Load() {
		t.Fatal("a skipped snapshot must stay dirty")
	}

	// Close flushes regardless of the interval.
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if e.dirty.Load() {
		t.Fatal("Close must clear dirty")
	}
	e2 := snapEngine(t, path, nil)
	if !e2.Domain(key(1)).Known(key(2)) {
		t.Fatal("Close did not persist the latest state")
	}

	// After the interval, MaybeSnapshot writes again.
	e.Domain(key(1)).MarkKnown(key(3))
	now = now.Add(2 * time.Hour)
	if err := e.MaybeSnapshot(); err != nil || e.dirty.Load() {
		t.Fatalf("interval elapsed but not written: %v dirty=%v", err, e.dirty.Load())
	}
}

// Checks that with no snapshot path, Snapshot, MaybeSnapshot, and Close all do nothing.
func TestSnapshotNoPathIsNoOp(t *testing.T) {
	e := newTestEngine(t, nil)
	populate(e)
	if e.Snapshot() != nil || e.MaybeSnapshot() != nil || e.Close() != nil {
		t.Fatal("no-path snapshot must be a no-op")
	}
}

// Checks that repeated snapshots leave only the snapshot file in its directory,
// with no temporary files left behind.
func TestSnapshotAtomicLeavesNoTempFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap")
	e := snapEngine(t, path, nil)
	populate(e)
	for i := 0; i < 3; i++ {
		if err := e.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected only the snapshot, found %d files", len(entries))
	}
}

// Checks that a failed snapshot returns an error and leaves the engine dirty, so
// the write is retried later.
func TestSnapshotUnwritableDirKeepsDirty(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "file")
	os.WriteFile(blocker, nil, 0o600)
	e := snapEngine(t, filepath.Join(blocker, "snap"), nil) // parent is a file
	populate(e)
	if err := e.Snapshot(); err == nil {
		t.Fatal("expected an error")
	}
	if !e.dirty.Load() {
		t.Fatal("a failed snapshot must stay dirty so it is retried")
	}
}

// Checks that a base registered before a snapshot is a candidate after reload, and
// that it counts as non-resident, since the plaintext pool is not restored.
func TestSnapshotRestoresSimilarityIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	e1 := snapEngine(t, path, nil)
	d := e1.Domain(key(1))
	base := randBytes(1, 64<<10)
	info := BaseInfo{ObjectKey: key(9), BaseKey: key(8), Size: uint32(len(base))}
	if !d.RegisterBase(info, append([]byte(nil), base...), e1.Sketcher().Sketch(base)) {
		t.Fatal("register")
	}
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}

	e2 := snapEngine(t, path, nil)
	if err := e2.LoadErr(); err != nil {
		t.Fatal(err)
	}
	d2 := e2.Domain(key(1))
	if d2.sim.Len() != 1 {
		t.Fatalf("restored %d bases, want 1", d2.sim.Len())
	}
	got, nr := d2.Candidates(e2.Sketcher().Sketch(mutate(base, 2, 2, 40)))
	if len(got) != 1 || got[0].ObjectKey != key(9) || got[0].BaseKey != key(8) || nr != 1 {
		t.Fatalf("candidates %+v notResident %d", got, nr)
	}
	if e2.pool.Has(key(9)) {
		t.Fatal("the plaintext pool must not be restored from the snapshot")
	}
}

// Checks that the snapshot file holds no base key, base address, representation
// key, salt, or parameter fingerprint in the clear. Only the magic, version, and
// file ID are readable. The file must still round-trip, bases included.
func TestSnapshotIsSealed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	e1 := snapEngine(t, path, nil)
	populate(e1)
	d := e1.Domain(key(1))
	base := randBytes(3, 64<<10)
	baseKey := key(0xAB)
	if !d.RegisterBase(BaseInfo{ObjectKey: key(0xCD), BaseKey: baseKey, Size: uint32(len(base))}, append([]byte(nil), base...), e1.Sketcher().Sketch(base)) {
		t.Fatal("register")
	}
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[:len(snapMagic)]) != snapMagic || binary.LittleEndian.Uint32(raw[len(snapMagic):]) != snapVersion {
		t.Fatalf("header %x", raw[:12])
	}
	for name, secret := range map[string][]byte{
		"base key":     baseKey[:],
		"base address": func() []byte { k := key(0xCD); return k[:] }(),
		"salt":         func() []byte { k := key(1); return k[:] }(),
		"rep key":      func() []byte { k := key(101); return k[:] }(),
		"fingerprint":  func() []byte { f := e1.Params().Fingerprint(); return f[:] }(),
	} {
		if bytes.Contains(raw, secret) {
			t.Errorf("%s appears in the snapshot file in the clear", name)
		}
	}
	// And it still round-trips, bases included.
	e2 := snapEngine(t, path, nil)
	if err := e2.LoadErr(); err != nil {
		t.Fatal(err)
	}
	if got := e2.Domain(key(1)).sim.Len(); got != 1 {
		t.Fatalf("restored %d bases, want 1", got)
	}
}

// Checks that a snapshot sealed under one customer key is rejected under another
// with ErrSnapshotCorrupt, and the engine starts empty.
func TestSnapshotRejectedUnderAnotherCustomerKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	e1 := snapEngine(t, path, nil)
	populate(e1)
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	other := func() ([32]byte, error) { return key(0x5C), nil }
	e2, err := NewEngine(testParams(t, func(c *Config) { c.Index.SnapshotPath = path }), other)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(e2.LoadErr(), ErrSnapshotCorrupt) {
		t.Fatalf("want ErrSnapshotCorrupt, got %v", e2.LoadErr())
	}
	if e2.Stats().Domains != 0 {
		t.Fatal("a snapshot under another key must not load")
	}
}

// Checks that a snapshot larger than one frame seals and reloads, and that
// truncating inside a later frame is detected.
func TestSnapshotMultiFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	big := func(c *Config) { c.Index.KnownCapacity = 1 << 21 } // ~2.6 MiB of Bloom words per domain
	e1 := snapEngine(t, path, big)
	populate(e1)
	if err := e1.Snapshot(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() < 2*sealFrameSize {
		t.Fatalf("snapshot is %d bytes; the test needs more than two frames", st.Size())
	}
	e2 := snapEngine(t, path, big)
	if err := e2.LoadErr(); err != nil {
		t.Fatal(err)
	}
	if st := e2.Stats(); st.Domains != 2 || st.Representations != 50 {
		t.Fatalf("stats %+v", st)
	}
	// Truncating inside a later frame is detected.
	raw, _ := os.ReadFile(path)
	if err := os.WriteFile(path, raw[:len(raw)-100], 0o600); err != nil {
		t.Fatal(err)
	}
	if e3 := snapEngine(t, path, big); !errors.Is(e3.LoadErr(), ErrSnapshotCorrupt) {
		t.Fatalf("want ErrSnapshotCorrupt, got %v", e3.LoadErr())
	}
}

// Checks that a snapshot whose version field is not snapVersion is refused
// before its body is read, and that the engine starts empty.
func TestSnapshotRejectsOtherVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap")
	var buf bytes.Buffer
	buf.WriteString(snapMagic)
	binary.Write(&buf, binary.LittleEndian, snapVersion+1)
	buf.Write(make([]byte, 64))
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	e := snapEngine(t, path, nil)
	if !errors.Is(e.LoadErr(), ErrSnapshotCorrupt) {
		t.Fatalf("want ErrSnapshotCorrupt, got %v", e.LoadErr())
	}
	if e.Stats().Domains != 0 || e.Stats().Representations != 0 {
		t.Fatalf("a snapshot of another version must not load: %+v", e.Stats())
	}
}

// Checks that a snapshot path without a customer key is refused, that a failing key
// function is reported, and that no snapshot path needs no key.
func TestNewEngineSnapshotPathNeedsKey(t *testing.T) {
	p := testParams(t, func(c *Config) { c.Index.SnapshotPath = filepath.Join(t.TempDir(), "snap") })
	if _, err := NewEngine(p, nil); err == nil {
		t.Fatal("a snapshot path without a customer key must be refused")
	}
	if _, err := NewEngine(p, func() ([32]byte, error) { return [32]byte{}, errors.New("no key") }); err == nil {
		t.Fatal("a failing key function must be reported")
	}
	if _, err := NewEngine(testParams(t, nil), nil); err != nil {
		t.Fatalf("no snapshot path, no key needed: %v", err)
	}
}
