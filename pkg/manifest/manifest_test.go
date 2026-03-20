package manifest

import (
	"crypto/rand"
	"testing"

	"github.com/fullof-work/container-accelerator-research/pkg/chunker"
)

func TestMarshalUnmarshalRoundtrip(t *testing.T) {
	var customerKey [32]byte
	rand.Read(customerKey[:])

	// Create a manifest with variable-size chunks.
	m := &Manifest{
		ImageSize:    10 * 1024 * 1024, // 10 MiB
		ChunkMode:    ChunkModeFastCDC,
		MinChunkSize: chunker.MinChunkSize,
		MaxChunkSize: chunker.MaxChunkSize,
	}

	// Simulate variable-size chunks.
	chunkSizes := []uint32{500000, 600000, 400000, 700000, 550000, 800000, 450000}
	offset := uint64(0)
	for i, size := range chunkSizes {
		entry := ChunkEntry{
			Offset: offset,
			Size:   size,
			IsZero: i == 3, // mark one as zero
		}
		if !entry.IsZero {
			rand.Read(entry.CiphertextHash[:])
		}
		m.Entries = append(m.Entries, entry)

		var key [32]byte
		if !entry.IsZero {
			rand.Read(key[:])
		}
		m.Keys = append(m.Keys, key)

		offset += uint64(size)
	}

	data, err := m.Marshal(customerKey)
	if err != nil {
		t.Fatal(err)
	}

	m2, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	if m2.ImageSize != m.ImageSize {
		t.Errorf("ImageSize: got %d, want %d", m2.ImageSize, m.ImageSize)
	}
	if m2.ChunkMode != m.ChunkMode {
		t.Errorf("ChunkMode: got %d, want %d", m2.ChunkMode, m.ChunkMode)
	}
	if m2.MinChunkSize != m.MinChunkSize {
		t.Errorf("MinChunkSize: got %d, want %d", m2.MinChunkSize, m.MinChunkSize)
	}
	if m2.MaxChunkSize != m.MaxChunkSize {
		t.Errorf("MaxChunkSize: got %d, want %d", m2.MaxChunkSize, m.MaxChunkSize)
	}
	if len(m2.Entries) != len(m.Entries) {
		t.Fatalf("entry count: got %d, want %d", len(m2.Entries), len(m.Entries))
	}

	for i := range m.Entries {
		if m2.Entries[i].Offset != m.Entries[i].Offset {
			t.Errorf("entry %d offset mismatch: got %d, want %d", i, m2.Entries[i].Offset, m.Entries[i].Offset)
		}
		if m2.Entries[i].Size != m.Entries[i].Size {
			t.Errorf("entry %d size mismatch: got %d, want %d", i, m2.Entries[i].Size, m.Entries[i].Size)
		}
		if m2.Entries[i].IsZero != m.Entries[i].IsZero {
			t.Errorf("entry %d IsZero mismatch", i)
		}
		if m2.Entries[i].CiphertextHash != m.Entries[i].CiphertextHash {
			t.Errorf("entry %d hash mismatch", i)
		}
	}

	// Keys should not be available yet.
	if !m2.IsSealed() {
		t.Error("manifest should be sealed before Unseal()")
	}

	// Unseal.
	if err := m2.Unseal(customerKey); err != nil {
		t.Fatal(err)
	}

	if m2.IsSealed() {
		t.Error("manifest should be unsealed after Unseal()")
	}

	for i := range m.Keys {
		if m2.Keys[i] != m.Keys[i] {
			t.Errorf("key %d mismatch", i)
		}
	}
}

func TestUnsealWrongKey(t *testing.T) {
	var customerKey [32]byte
	rand.Read(customerKey[:])

	m := &Manifest{
		ImageSize:    chunker.MinChunkSize,
		ChunkMode:    ChunkModeFastCDC,
		MinChunkSize: chunker.MinChunkSize,
		MaxChunkSize: chunker.MaxChunkSize,
		Entries:      []ChunkEntry{{Offset: 0, Size: chunker.MinChunkSize}},
		Keys:         [][32]byte{{}},
	}
	rand.Read(m.Entries[0].CiphertextHash[:])
	rand.Read(m.Keys[0][:])

	data, err := m.Marshal(customerKey)
	if err != nil {
		t.Fatal(err)
	}

	m2, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	var wrongKey [32]byte
	rand.Read(wrongKey[:])
	if err := m2.Unseal(wrongKey); err == nil {
		t.Error("expected error when unsealing with wrong key")
	}
}

func TestChunkIndexForOffset(t *testing.T) {
	// Create a manifest with variable-size chunks.
	m := &Manifest{
		Entries: []ChunkEntry{
			{Offset: 0, Size: 100000},        // 0-99999
			{Offset: 100000, Size: 150000},   // 100000-249999
			{Offset: 250000, Size: 200000},   // 250000-449999
			{Offset: 450000, Size: 80000},    // 450000-529999
		},
	}

	tests := []struct {
		offset   uint64
		expected uint32
	}{
		{0, 0},
		{50000, 0},
		{99999, 0},
		{100000, 1},
		{200000, 1},
		{249999, 1},
		{250000, 2},
		{449999, 2},
		{450000, 3},
		{500000, 3},
		{529999, 3},
	}

	for _, tt := range tests {
		got := m.ChunkIndexForOffset(tt.offset)
		if got != tt.expected {
			t.Errorf("ChunkIndexForOffset(%d) = %d, want %d", tt.offset, got, tt.expected)
		}
	}
}

func TestChunkIndexForOffsetSingleChunk(t *testing.T) {
	m := &Manifest{
		Entries: []ChunkEntry{
			{Offset: 0, Size: 1000000},
		},
	}

	tests := []struct {
		offset   uint64
		expected uint32
	}{
		{0, 0},
		{500000, 0},
		{999999, 0},
	}

	for _, tt := range tests {
		got := m.ChunkIndexForOffset(tt.offset)
		if got != tt.expected {
			t.Errorf("ChunkIndexForOffset(%d) = %d, want %d", tt.offset, got, tt.expected)
		}
	}
}

func TestChunkIndexForOffsetEmpty(t *testing.T) {
	m := &Manifest{
		Entries: []ChunkEntry{},
	}

	got := m.ChunkIndexForOffset(0)
	if got != 0 {
		t.Errorf("ChunkIndexForOffset(0) on empty manifest = %d, want 0", got)
	}
}

func TestBadMagic(t *testing.T) {
	data := make([]byte, HeaderSize)
	_, err := Unmarshal(data)
	if err != ErrBadMagic {
		t.Errorf("expected ErrBadMagic, got %v", err)
	}
}

func TestTruncated(t *testing.T) {
	_, err := Unmarshal([]byte{1, 2, 3})
	if err != ErrTruncated {
		t.Errorf("expected ErrTruncated, got %v", err)
	}
}

func TestManifestOverhead(t *testing.T) {
	// Verify overhead claim: < 0.03% for 10 GiB image with FastCDC.
	imageSize := uint64(10 * 1024 * 1024 * 1024)
	// Estimate chunk count with avg 512K chunks.
	avgChunkSize := uint64(chunker.AvgChunkSize)
	chunkCount := int((imageSize + avgChunkSize - 1) / avgChunkSize)

	headerBytes := HeaderSize
	entryBytes := chunkCount * EntrySize
	keyTableBytes := 12 + chunkCount*32 + 16
	total := headerBytes + entryBytes + keyTableBytes

	ratio := float64(total) / float64(imageSize) * 100
	t.Logf("10 GiB manifest (FastCDC): %d bytes (%.4f%%)", total, ratio)
	if ratio >= 0.03 {
		t.Errorf("manifest overhead %.4f%% exceeds 0.03%%", ratio)
	}
}

func TestVersion(t *testing.T) {
	if Version != 2 {
		t.Errorf("Version = %d, want 2", Version)
	}
}

func TestEntrySize(t *testing.T) {
	if EntrySize != 56 {
		t.Errorf("EntrySize = %d, want 56", EntrySize)
	}
}
