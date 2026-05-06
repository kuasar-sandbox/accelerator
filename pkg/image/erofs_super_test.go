package flatten

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// TestReadEROFSSize_OK — synthesise a minimal valid superblock and
// confirm the size computation.
func TestReadEROFSSize_OK(t *testing.T) {
	// 8 MiB image: blocks=2048, blkszbits=12 (4 KiB) → 2048 * 4096 = 8 MiB.
	const wantBlocks = 2048
	const wantBlkBits = 12
	const wantSize = uint64(wantBlocks) << wantBlkBits

	buf := make([]byte, erofsSuperOffset+erofsSuperSize)
	binary.LittleEndian.PutUint32(buf[erofsSuperOffset+erofsOffMagic:], erofsMagic)
	buf[erofsSuperOffset+erofsOffBlkBits] = wantBlkBits
	binary.LittleEndian.PutUint32(buf[erofsSuperOffset+erofsOffBlocks:], wantBlocks)

	got, err := ReadEROFSSize(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("ReadEROFSSize: %v", err)
	}
	if got != wantSize {
		t.Errorf("size: got %d, want %d", got, wantSize)
	}
}

// TestReadEROFSSize_BadMagic — wrong magic in the superblock yields
// a clear error rather than a silently-bogus size.
func TestReadEROFSSize_BadMagic(t *testing.T) {
	buf := make([]byte, erofsSuperOffset+erofsSuperSize)
	binary.LittleEndian.PutUint32(buf[erofsSuperOffset:], 0xDEADBEEF)
	if _, err := ReadEROFSSize(bytes.NewReader(buf)); err == nil ||
		!strings.Contains(err.Error(), "bad magic") {
		t.Fatalf("expected bad-magic error, got %v", err)
	}
}

// TestReadEROFSSize_ImplausibleBlkBits — blkszbits 0 is corrupt;
// >30 would overflow. Both should error.
func TestReadEROFSSize_ImplausibleBlkBits(t *testing.T) {
	for _, bits := range []byte{0, 31, 64} {
		buf := make([]byte, erofsSuperOffset+erofsSuperSize)
		binary.LittleEndian.PutUint32(buf[erofsSuperOffset:], erofsMagic)
		buf[erofsSuperOffset+erofsOffBlkBits] = bits
		binary.LittleEndian.PutUint32(buf[erofsSuperOffset+erofsOffBlocks:], 1)
		if _, err := ReadEROFSSize(bytes.NewReader(buf)); err == nil {
			t.Errorf("blkszbits=%d: expected error, got nil", bits)
		}
	}
}
