package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// deltaManifest is a Version2 manifest of n chunks where every entry with
// index%3==1 is a delta of a base object outside the manifest.
func deltaManifest(n int) *Manifest {
	m := repeatedManifest(n)
	m.Version = Version2
	for i := range m.Entries {
		m.Entries[i].CiphertextHash = [32]byte{byte(i), 0x42}
		if i%3 == 1 {
			m.Entries[i].IsDelta = true
			m.Deltas = append(m.Deltas, DeltaRef{Entry: uint32(i), BaseSize: 4096, Base: [32]byte{0xB0, byte(i)}})
		}
	}
	return m
}

// TestVersion2DeltaTableRoundTrip checks that a Version2 manifest survives
// Marshal and Unmarshal with its sealed key table, delta refs and IsDelta flags
// intact, and that DeltaFor agrees with the flags. On the raw encoding it also
// checks the layout: the delta count sits at header bytes 52:56, and the key
// table starts right after the delta refs.
func TestVersion2DeltaTableRoundTrip(t *testing.T) {
	t.Parallel()
	m := deltaManifest(10)
	sealed := bytes.Repeat([]byte{0x11}, 10*40)
	phys, err := Marshal(m, sealed)
	if err != nil {
		t.Fatal(err)
	}
	got, gotSealed, err := Unmarshal(phys)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != Version2 {
		t.Fatalf("version = %d", got.Version)
	}
	if !bytes.Equal(gotSealed, sealed) {
		t.Fatal("sealed key table mismatch")
	}
	if len(got.Deltas) != len(m.Deltas) {
		t.Fatalf("deltas = %d, want %d", len(got.Deltas), len(m.Deltas))
	}
	for i := range m.Deltas {
		if got.Deltas[i] != m.Deltas[i] {
			t.Fatalf("delta %d = %+v, want %+v", i, got.Deltas[i], m.Deltas[i])
		}
	}
	for i, e := range got.Entries {
		if e.IsDelta != m.Entries[i].IsDelta {
			t.Fatalf("entry %d IsDelta = %v", i, e.IsDelta)
		}
		d, ok := got.DeltaFor(i)
		if ok != e.IsDelta {
			t.Fatalf("DeltaFor(%d) ok = %v, IsDelta = %v", i, ok, e.IsDelta)
		}
		if ok && d.Entry != uint32(i) {
			t.Fatalf("DeltaFor(%d) = entry %d", i, d.Entry)
		}
	}
	// Geometry: the table sits between holes and the key table.
	if phys[4] != ManifestEncodingRaw {
		t.Skip("snappy envelope; layout checked on raw path")
	}
	le := binary.LittleEndian
	payload := phys[5:]
	if got := le.Uint32(payload[52:56]); got != uint32(len(m.Deltas)) {
		t.Fatalf("header delta count = %d", got)
	}
	keyOff := le.Uint64(payload[24:32])
	want := uint64(HeaderSize + 10*EntrySize + len(m.Deltas)*DeltaRefSize)
	if keyOff != want {
		t.Fatalf("key table offset = %d, want %d", keyOff, want)
	}
}

// TestVersion1BytesAndAADUnchanged checks that Version1 is byte-compatible with
// the format from before Version2: reserved header bytes stay zero, the AAD is
// only the header plus entries, and a delta table or delta flag on a Version1
// manifest is rejected by Marshal with ErrBadDeltas.
func TestVersion1BytesAndAADUnchanged(t *testing.T) {
	t.Parallel()
	// A Version1 manifest must marshal and authenticate exactly as before
	// Version2 existed: no delta count in the header, no table, same AAD.
	m := repeatedManifest(5)
	phys, err := Marshal(m, []byte("kt"))
	if err != nil {
		t.Fatal(err)
	}
	payload := phys[5:]
	if !bytes.Equal(payload[52:60], make([]byte, 8)) {
		t.Fatalf("Version1 reserved bytes not zero: %x", payload[52:60])
	}
	aad := BuildAAD(m)
	if len(aad) != HeaderSize+5*EntrySize {
		t.Fatalf("Version1 AAD length = %d", len(aad))
	}
	// Deltas on a Version1 manifest are ignored by AAD (and rejected by Marshal).
	m.Deltas = []DeltaRef{{Entry: 0, BaseSize: 1}}
	if len(BuildAAD(m)) != len(aad) {
		t.Fatal("Version1 AAD must not include delta refs")
	}
	if _, err := Marshal(m, nil); !errors.Is(err, ErrBadDeltas) {
		t.Fatalf("Marshal(Version1 with deltas) = %v, want ErrBadDeltas", err)
	}
	m.Deltas = nil
	m.Entries[0].IsDelta = true
	if _, err := Marshal(m, nil); !errors.Is(err, ErrBadDeltas) {
		t.Fatalf("Marshal(Version1 with delta flag) = %v, want ErrBadDeltas", err)
	}
}

// TestVersion2AADCoversDeltaTable checks that a Version2 manifest's AAD includes
// the delta table. Changing a base key or a base size must change the AAD bytes,
// so the sealed key table cannot be unsealed against a redirected base.
func TestVersion2AADCoversDeltaTable(t *testing.T) {
	t.Parallel()
	m := deltaManifest(6)
	a := BuildAAD(m)
	if len(a) != HeaderSize+6*EntrySize+len(m.Deltas)*DeltaRefSize {
		t.Fatalf("AAD length = %d", len(a))
	}
	m.Deltas[0].Base[0] ^= 1
	if bytes.Equal(a, BuildAAD(m)) {
		t.Fatal("redirecting a base must change the AAD")
	}
	m.Deltas[0].Base[0] ^= 1
	m.Deltas[0].BaseSize++
	if bytes.Equal(a, BuildAAD(m)) {
		t.Fatal("changing a base size must change the AAD")
	}
}

// TestVersion2RejectsInconsistentDeltaTable checks that flags and delta refs
// that disagree are rejected with ErrBadDeltas. The cases are a flag without a
// ref, a ref without a flag, zero plus delta, an out-of-range entry, an unsorted
// table, a zero base size, and a base equal to the entry's own object. The same
// rules are then checked on the wire by patching a valid encoding. A Version1
// header carrying Version2 flags and table must also fail to unmarshal.
func TestVersion2RejectsInconsistentDeltaTable(t *testing.T) {
	t.Parallel()
	mut := func(f func(m *Manifest)) *Manifest {
		m := deltaManifest(6)
		f(m)
		return m
	}
	for _, tc := range []struct {
		name string
		m    *Manifest
	}{
		{"flag without ref", mut(func(m *Manifest) { m.Entries[0].IsDelta = true })},
		{"ref without flag", mut(func(m *Manifest) { m.Entries[1].IsDelta = false })},
		{"zero and delta", mut(func(m *Manifest) { m.Entries[1].IsZero = true })},
		{"out of range", mut(func(m *Manifest) { m.Deltas[0].Entry = 99 })},
		{"unsorted", mut(func(m *Manifest) { m.Deltas[0], m.Deltas[1] = m.Deltas[1], m.Deltas[0] })},
		{"zero base size", mut(func(m *Manifest) { m.Deltas[0].BaseSize = 0 })},
		{"self base", mut(func(m *Manifest) { m.Deltas[0].Base = m.Entries[1].CiphertextHash })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Marshal(tc.m, nil); !errors.Is(err, ErrBadDeltas) {
				t.Fatalf("Marshal = %v, want ErrBadDeltas", err)
			}
		})
	}

	// The same checks must hold on the wire: patch a valid encoding.
	good := deltaManifest(6)
	phys, err := Marshal(good, []byte("kt"))
	if err != nil {
		t.Fatal(err)
	}
	if phys[4] != ManifestEncodingRaw {
		t.Fatal("expected raw envelope for this small manifest")
	}
	patched := append([]byte(nil), phys...)
	// Clear the delta flag of entry 1 (payload offset: 5 + 60 + 1*56 + 12).
	flagsAt := 5 + (HeaderSize - 4) + 1*EntrySize + 12
	binary.LittleEndian.PutUint32(patched[flagsAt:], 0)
	if _, _, err := Unmarshal(patched); !errors.Is(err, ErrBadDeltas) {
		t.Fatalf("Unmarshal(flag cleared) = %v, want ErrBadDeltas", err)
	}
	// Version1 header byte with Version2 flags set: the flag is rejected.
	patched = append([]byte(nil), phys...)
	patched[5] = Version1
	if _, _, err := Unmarshal(patched); err == nil {
		t.Fatal("Unmarshal(Version1 with delta flags/table) must fail")
	}
}

// TestVersion2WithoutDeltasRoundTrips checks that Version2 with an empty delta
// table is still a valid encoding and round-trips with no deltas.
func TestVersion2WithoutDeltasRoundTrips(t *testing.T) {
	t.Parallel()
	m := repeatedManifest(3)
	m.Version = Version2
	phys, err := Marshal(m, []byte("kt"))
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := Unmarshal(phys)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != Version2 || len(got.Deltas) != 0 {
		t.Fatalf("got version %d, %d deltas", got.Version, len(got.Deltas))
	}
}
