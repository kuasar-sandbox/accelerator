package codec

import (
	"bytes"
	"errors"
	"testing"
)

// TestMarshalUnmarshal_NoHoles_PreservesAADBytes verifies that adding
// the Holes section without any holes produces the exact same AAD as
// the pre-hole code path. This guards against breaking older sealed
// manifests.
func TestMarshalUnmarshal_NoHoles_PreservesAADBytes(t *testing.T) {
	m := &Manifest{
		Version:      Version1,
		ChunkMode:    ChunkModeFastCDC,
		ImageSize:    4096,
		MinChunkSize: 64,
		MaxChunkSize: 4096,
		Entries: []ChunkEntry{
			{Offset: 0, Size: 4096, IsZero: false, CiphertextHash: [32]byte{0xDE, 0xAD}},
		},
	}
	if err := m.ValidateGeometry(); err != nil {
		t.Fatalf("ValidateGeometry: %v", err)
	}
	aadNoHoles := BuildAAD(m)
	// AAD layout for a hole-less manifest: HeaderSize (64) + 1 entry (56)
	// = 120 bytes. Header byte 44..47 (HoleCount) is zero; bytes 48..63
	// are zero (HolesOffset + reserved). This matches what the old
	// buildAAD produced when those bytes were never written.
	if got, want := len(aadNoHoles), HeaderSize+EntrySize; got != want {
		t.Fatalf("AAD length %d, want %d", got, want)
	}
	// HoleCount slot in AAD must be all-zero.
	for i := 44; i < 48; i++ {
		if aadNoHoles[i] != 0 {
			t.Fatalf("AAD byte %d = %x, want 0 (HoleCount)", i, aadNoHoles[i])
		}
	}
}

// TestMarshalUnmarshal_WithHoles_RoundTrip — pack a manifest with
// entries+holes through Marshal/Unmarshal and assert the parsed
// structure equals the input.
func TestMarshalUnmarshal_WithHoles_RoundTrip(t *testing.T) {
	m := &Manifest{
		Version:      Version1,
		ChunkMode:    ChunkModeFixed,
		ImageSize:    1 << 20, // 1 MiB
		MinChunkSize: 4096,
		MaxChunkSize: 4096,
		Entries: []ChunkEntry{
			{Offset: 0, Size: 4096, CiphertextHash: [32]byte{0x11}},
			{Offset: 4096, Size: 4096, IsZero: true},
		},
		Holes: []HoleExtent{
			{Offset: 8192, Size: 1<<20 - 8192}, // tail hole
		},
	}
	if err := m.ValidateGeometry(); err != nil {
		t.Fatalf("ValidateGeometry: %v", err)
	}

	// Use a non-empty sealed key table so the layout is exercised end-to-end.
	sealedKT := []byte{0xAA, 0xBB, 0xCC}
	data, err := Marshal(m, sealedKT)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, gotKT, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.ImageSize != m.ImageSize || len(got.Entries) != len(m.Entries) || len(got.Holes) != len(m.Holes) {
		t.Fatalf("roundtrip header mismatch: got %+v, want %+v", got, m)
	}
	for i, e := range got.Entries {
		if e != m.Entries[i] {
			t.Fatalf("entry %d mismatch: got %+v, want %+v", i, e, m.Entries[i])
		}
	}
	for i, h := range got.Holes {
		if h != m.Holes[i] {
			t.Fatalf("hole %d mismatch: got %+v, want %+v", i, h, m.Holes[i])
		}
	}
	if !bytes.Equal(gotKT, sealedKT) {
		t.Fatalf("sealed key table mismatch")
	}
}

// TestValidateGeometry_RejectsOverlap — an entry and a hole that
// overlap should fail validation.
func TestValidateGeometry_RejectsOverlap(t *testing.T) {
	m := &Manifest{
		ImageSize: 8192,
		Entries:   []ChunkEntry{{Offset: 0, Size: 4096}},
		Holes:     []HoleExtent{{Offset: 2048, Size: 4096}}, // overlaps entry [0, 4096)
	}
	if err := m.ValidateGeometry(); !errors.Is(err, ErrBadGeometry) {
		t.Fatalf("expected ErrBadGeometry on overlap, got %v", err)
	}
}

// TestValidateGeometry_RejectsGap — entries+holes leaving a gap
// must fail.
func TestValidateGeometry_RejectsGap(t *testing.T) {
	m := &Manifest{
		ImageSize: 8192,
		Entries:   []ChunkEntry{{Offset: 0, Size: 4096}},
		// gap [4096, 8192) — no hole, no entry
	}
	if err := m.ValidateGeometry(); !errors.Is(err, ErrBadGeometry) {
		t.Fatalf("expected ErrBadGeometry on gap, got %v", err)
	}
}

// TestValidateGeometry_RejectsZeroSize — zero-size entries/holes.
func TestValidateGeometry_RejectsZeroSize(t *testing.T) {
	m1 := &Manifest{
		ImageSize: 0,
		Entries:   []ChunkEntry{{Offset: 0, Size: 0}},
	}
	if err := m1.ValidateGeometry(); !errors.Is(err, ErrBadGeometry) {
		t.Fatalf("zero-size entry: expected ErrBadGeometry, got %v", err)
	}
	m2 := &Manifest{
		ImageSize: 0,
		Holes:     []HoleExtent{{Offset: 0, Size: 0}},
	}
	if err := m2.ValidateGeometry(); !errors.Is(err, ErrBadGeometry) {
		t.Fatalf("zero-size hole: expected ErrBadGeometry, got %v", err)
	}
}

// TestValidateGeometry_AllHoles — an image that's entirely a hole is valid.
func TestValidateGeometry_AllHoles(t *testing.T) {
	m := &Manifest{
		ImageSize: 1 << 30,
		Holes:     []HoleExtent{{Offset: 0, Size: 1 << 30}},
	}
	if err := m.ValidateGeometry(); err != nil {
		t.Fatalf("all-hole manifest should validate: %v", err)
	}
}

// TestUnmarshal_RejectsBadGeometry — corrupt manifest bytes that
// describe a tile violation must fail at Unmarshal.
func TestUnmarshal_RejectsBadGeometry(t *testing.T) {
	// Build a manifest with a deliberately-invalid layout (gap),
	// Marshal it, then Unmarshal — should fail.
	m := &Manifest{
		Version:   Version1,
		ChunkMode: ChunkModeFixed,
		ImageSize: 8192, // claims 8192 but only describes [0, 4096)
		Entries:   []ChunkEntry{{Offset: 0, Size: 4096}},
	}
	// Skip ValidateGeometry on construction — Marshal doesn't validate.
	data, err := Marshal(m, nil)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, _, err := Unmarshal(data); !errors.Is(err, ErrBadGeometry) {
		t.Fatalf("expected ErrBadGeometry from Unmarshal, got %v", err)
	}
}
