// Package manifest implements the binary manifest format for the container accelerator.
//
// A manifest describes a chunked image or snapshot: header, per-chunk entries,
// optional hole extents, and a sealed key table. The on-disk layout is:
//
//	[Header 64B]
//	[ChunkEntry × N, 56B each]
//	[hole extent × M, 16B each]   -- entries describe data, holes describe absence
//	[SealedKeyTable variable]
//
// Entries cover the data ranges of the original image; holes cover the
// "no data here" ranges (filesystem holes, qcow2 unallocated, TRIM, ...).
// Together they tile [0, ImageSize) with no overlap and no gap.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/sparse"
)

// Binary format constants.
const (
	Magic       uint32 = 0x4D414E49 // "MANI"
	Version1    uint8  = 1
	HeaderSize         = 64
	EntrySize          = 56
	HoleSize           = 16 // 8 B Offset + 8 B Size; no flags, no reserved
)

// ChunkEntry flag bits (the 4-byte flags field at offset 12 of each entry).
const (
	// EntryFlagZero marks a chunk whose plaintext is all zero bytes.
	// Zero chunks are not encrypted, not stored in the content store,
	// and not present in the sealed key table — load synthesises the
	// zero bytes locally. CiphertextHash is unused for zero entries.
	EntryFlagZero uint32 = 1 << 0
)

// ChunkMode identifies the chunking algorithm used to produce the manifest.
type ChunkMode uint8

const (
	ChunkModeFastCDC ChunkMode = 1
	ChunkModeFixed   ChunkMode = 2
)

// Manifest holds the decoded manifest data.
type Manifest struct {
	Version      uint8
	ChunkMode    ChunkMode
	ImageSize    uint64
	MinChunkSize uint32
	MaxChunkSize uint32
	Entries      []ChunkEntry
	// Holes are the "no data here" regions of the original image,
	// sorted by Offset and disjoint from Entries. Holes are externally
	// declared (filesystem hole detection, qcow2 unallocated, TRIM
	// ranges, ...); they are NEVER derived from chunk content — a
	// chunk of all zeros uses IsZero instead. Read semantics are
	// caller-defined: the fetch layer zero-fills holes, or falls
	// through to a lower layer when overlaid.
	Holes []sparse.Extent
	Keys  [][32]byte // per-chunk convergent keys (plaintext, before sealing)
}

// ChunkEntry describes one chunk in the original image.
type ChunkEntry struct {
	Offset         uint64
	Size           uint32
	IsZero         bool
	CiphertextHash [32]byte
}

// ChunkCount returns the number of chunk entries.
func (m *Manifest) ChunkCount() uint32 {
	return uint32(len(m.Entries))
}

// Errors returned by Marshal and Unmarshal.
var (
	ErrNilManifest   = errors.New("manifest: nil manifest")
	ErrTooShort      = errors.New("manifest: data too short")
	ErrBadMagic      = errors.New("manifest: invalid magic bytes")
	ErrBadVersion    = errors.New("manifest: unsupported version")
	ErrBadChunkCount = errors.New("manifest: chunk count mismatch")
	ErrBadGeometry   = errors.New("manifest: entries and holes do not tile [0, ImageSize)")
)

// Marshal serializes a Manifest and its sealed key table into the binary format.
//
// The caller is responsible for sealing the key table (via crypto.KeyTableEncryptor)
// before passing it here.
func Marshal(m *Manifest, sealedKeyTable []byte) ([]byte, error) {
	if m == nil {
		return nil, ErrNilManifest
	}

	count := m.ChunkCount()
	holeCount := uint32(len(m.Holes))
	holesOffset := uint64(HeaderSize) + uint64(count)*uint64(EntrySize)
	keyTableOffset := holesOffset + uint64(holeCount)*uint64(HoleSize)
	totalSize := keyTableOffset + uint64(len(sealedKeyTable))

	buf := make([]byte, totalSize)
	le := binary.LittleEndian

	// --- Header (64 bytes) ---
	le.PutUint32(buf[0:4], Magic)
	buf[4] = m.Version
	buf[5] = uint8(m.ChunkMode)
	// buf[6:8] reserved, zero
	le.PutUint64(buf[8:16], m.ImageSize)
	le.PutUint32(buf[16:20], count)
	le.PutUint32(buf[20:24], m.MinChunkSize)
	le.PutUint32(buf[24:28], m.MaxChunkSize)
	le.PutUint64(buf[28:36], keyTableOffset)
	le.PutUint64(buf[36:44], uint64(len(sealedKeyTable)))
	le.PutUint32(buf[44:48], holeCount)
	le.PutUint64(buf[48:56], holesOffset)
	// buf[56:64] reserved, zero

	// --- Chunk Entries (56 bytes each) ---
	for i, e := range m.Entries {
		base := HeaderSize + i*EntrySize
		le.PutUint64(buf[base:base+8], e.Offset)
		le.PutUint32(buf[base+8:base+12], e.Size)
		var flags uint32
		if e.IsZero {
			flags |= EntryFlagZero
		}
		le.PutUint32(buf[base+12:base+16], flags)
		copy(buf[base+16:base+48], e.CiphertextHash[:])
		// buf[base+48:base+56] reserved, zero
	}

	// --- Hole Extents (16 bytes each) ---
	for i, h := range m.Holes {
		base := int(holesOffset) + i*HoleSize
		le.PutUint64(buf[base:base+8], h.Offset)
		le.PutUint64(buf[base+8:base+16], h.Size)
	}

	// --- Sealed Key Table ---
	copy(buf[keyTableOffset:], sealedKeyTable)

	return buf, nil
}

// Unmarshal deserializes the binary format into a Manifest and the raw sealed key table.
//
// The returned Manifest has an empty Keys slice; the caller should unseal the key table
// (via crypto.KeyTableEncryptor) and populate Keys separately.
func Unmarshal(data []byte) (*Manifest, []byte, error) {
	if len(data) < HeaderSize {
		return nil, nil, ErrTooShort
	}

	le := binary.LittleEndian

	// --- Header ---
	magic := le.Uint32(data[0:4])
	if magic != Magic {
		return nil, nil, fmt.Errorf("%w: got 0x%08X", ErrBadMagic, magic)
	}

	version := data[4]
	if version != Version1 {
		return nil, nil, fmt.Errorf("%w: got %d", ErrBadVersion, version)
	}

	chunkMode := ChunkMode(data[5])
	imageSize := le.Uint64(data[8:16])
	chunkCount := le.Uint32(data[16:20])
	minChunkSize := le.Uint32(data[20:24])
	maxChunkSize := le.Uint32(data[24:28])
	keyTableOffset := le.Uint64(data[28:36])
	keyTableSize := le.Uint64(data[36:44])
	holeCount := le.Uint32(data[44:48])
	holesOffset := le.Uint64(data[48:56])

	// Validate that the data is large enough for all entries.
	entriesEnd := uint64(HeaderSize) + uint64(chunkCount)*uint64(EntrySize)
	if uint64(len(data)) < entriesEnd {
		return nil, nil, fmt.Errorf("%w: need %d bytes for entries, have %d", ErrTooShort, entriesEnd, len(data))
	}

	// Validate hole table bounds.
	holesEnd := holesOffset + uint64(holeCount)*uint64(HoleSize)
	if holeCount > 0 && uint64(len(data)) < holesEnd {
		return nil, nil, fmt.Errorf("%w: need %d bytes for holes, have %d", ErrTooShort, holesEnd, len(data))
	}

	// Validate key table bounds.
	if uint64(len(data)) < keyTableOffset+keyTableSize {
		return nil, nil, fmt.Errorf("%w: need %d bytes for key table, have %d", ErrTooShort, keyTableOffset+keyTableSize, len(data))
	}

	// --- Chunk Entries ---
	entries := make([]ChunkEntry, chunkCount)
	for i := range entries {
		base := uint64(HeaderSize) + uint64(i)*uint64(EntrySize)
		entries[i].Offset = le.Uint64(data[base : base+8])
		entries[i].Size = le.Uint32(data[base+8 : base+12])
		flags := le.Uint32(data[base+12 : base+16])
		entries[i].IsZero = (flags & EntryFlagZero) != 0
		copy(entries[i].CiphertextHash[:], data[base+16:base+48])
	}

	// --- Hole Extents ---
	var holes []sparse.Extent
	if holeCount > 0 {
		holes = make([]sparse.Extent, holeCount)
		for i := range holes {
			base := holesOffset + uint64(i)*uint64(HoleSize)
			holes[i].Offset = le.Uint64(data[base : base+8])
			holes[i].Size = le.Uint64(data[base+8 : base+16])
		}
	}

	// --- Sealed Key Table ---
	sealedKeyTable := make([]byte, keyTableSize)
	copy(sealedKeyTable, data[keyTableOffset:keyTableOffset+keyTableSize])

	m := &Manifest{
		Version:      version,
		ChunkMode:    chunkMode,
		ImageSize:    imageSize,
		MinChunkSize: minChunkSize,
		MaxChunkSize: maxChunkSize,
		Entries:      entries,
		Holes:        holes,
	}

	if err := m.ValidateGeometry(); err != nil {
		return nil, nil, err
	}

	return m, sealedKeyTable, nil
}

// ValidateGeometry checks that Entries and Holes together tile
// [0, ImageSize) with no gaps and no overlaps. Called from Unmarshal;
// callers constructing a Manifest in-memory should run this before
// Marshal.
//
// An empty manifest (ImageSize=0, no entries, no holes) is valid.
// Entries / Holes with Size=0 are rejected (they would collapse the tile).
func (m *Manifest) ValidateGeometry() error {
	if m.ImageSize == 0 && len(m.Entries) == 0 && len(m.Holes) == 0 {
		return nil
	}
	type segment struct {
		offset uint64
		end    uint64
	}
	segs := make([]segment, 0, len(m.Entries)+len(m.Holes))
	for _, e := range m.Entries {
		if e.Size == 0 {
			return fmt.Errorf("%w: entry at offset %d has zero size", ErrBadGeometry, e.Offset)
		}
		segs = append(segs, segment{e.Offset, e.Offset + uint64(e.Size)})
	}
	for _, h := range m.Holes {
		if h.Size == 0 {
			return fmt.Errorf("%w: hole at offset %d has zero size", ErrBadGeometry, h.Offset)
		}
		segs = append(segs, segment{h.Offset, h.Offset + h.Size})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].offset < segs[j].offset })
	cursor := uint64(0)
	for _, s := range segs {
		if s.offset != cursor {
			return fmt.Errorf("%w: gap or overlap at offset %d (expected %d)", ErrBadGeometry, s.offset, cursor)
		}
		cursor = s.end
	}
	if cursor != m.ImageSize {
		return fmt.Errorf("%w: tiled coverage ends at %d, ImageSize is %d", ErrBadGeometry, cursor, m.ImageSize)
	}
	return nil
}

// ChunkIndexForOffset returns the index of the chunk entry that contains the given
// byte offset within the original image. Returns -1 if the offset is not covered
// by any chunk (e.g., beyond the last chunk).
//
// Entries must be sorted by Offset in ascending order (as produced by ingest).
// Uses binary search for O(log n) lookup.
func ChunkIndexForOffset(entries []ChunkEntry, offset uint64) int {
	n := len(entries)
	if n == 0 {
		return -1
	}

	// Binary search: find the last entry whose Offset <= offset.
	lo, hi := 0, n-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if entries[mid].Offset <= offset {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	// hi is now the index of the last entry with Offset <= offset, or -1.
	if hi < 0 {
		return -1
	}

	// Check that offset falls within this chunk's range.
	e := entries[hi]
	if offset < e.Offset+uint64(e.Size) {
		return hi
	}
	return -1
}
