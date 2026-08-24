// Package manifest implements the binary manifest format for the container accelerator.
//
// A manifest describes a chunked image or snapshot. Its physical Version1
// envelope is ["MANI"][encoding][payload], where payload is the logical bytes
// after magic either RAW or canonical Go Snappy block encoded. The logical layout is:
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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/golang/snappy"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/internal/objectformat"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// Binary format constants.
const (
	// Magic is retained byte-for-byte in BuildAAD. The physical envelope uses
	// manifestMagic explicitly so its on-wire bytes are the required "MANI".
	Magic      uint32 = 0x4D414E49
	Version1   uint8  = 1
	HeaderSize        = 64
	EntrySize         = 56
	HoleSize          = 16 // 8 B Offset + 8 B Size; no flags, no reserved

	ManifestEncodingRaw    byte = 0x00
	ManifestEncodingSnappy byte = 0x01

	// MaxManifestDecodedSize bounds the complete logical manifest, including
	// its four-byte magic. It is fixed canonical reader policy, not config.
	MaxManifestDecodedSize = objectformat.MaxManifestDecodedSize
	MaxChunkDecodedSize    = objectformat.MaxChunkDecodedSize
)

var manifestMagic = [4]byte{'M', 'A', 'N', 'I'}

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
	ErrNilManifest      = errors.New("manifest: nil manifest")
	ErrTooShort         = errors.New("manifest: data too short")
	ErrBadMagic         = errors.New("manifest: invalid magic bytes")
	ErrBadVersion       = errors.New("manifest: unsupported version")
	ErrBadChunkCount    = errors.New("manifest: chunk count mismatch")
	ErrBadGeometry      = errors.New("manifest: entries and holes do not tile [0, ImageSize)")
	ErrBadEncoding      = errors.New("manifest: unsupported physical encoding")
	ErrBadChunkSize     = errors.New("manifest: chunk size exceeds declared or implementation limit")
	ErrManifestTooLarge = errors.New("manifest: decoded size exceeds hard limit")
)

// Marshal serializes a Manifest and its sealed key table into the binary format.
//
// The caller is responsible for sealing the key table (via crypto.KeyTableEncryptor)
// before passing it here.
func Marshal(m *Manifest, sealedKeyTable []byte) ([]byte, error) {
	if m == nil {
		return nil, ErrNilManifest
	}
	if m.Version != Version1 {
		return nil, fmt.Errorf("%w: got %d", ErrBadVersion, m.Version)
	}
	if uint64(len(m.Entries)) > math.MaxUint32 || uint64(len(m.Holes)) > math.MaxUint32 {
		return nil, ErrManifestTooLarge
	}
	if err := m.ValidateGeometry(); err != nil {
		return nil, err
	}
	if err := validateChunkSizes(m); err != nil {
		return nil, err
	}

	count := uint32(len(m.Entries))
	holeCount := uint32(len(m.Holes))
	holesOffset, ok := checkedAdd(uint64(HeaderSize), uint64(count)*uint64(EntrySize))
	if !ok {
		return nil, ErrManifestTooLarge
	}
	keyTableOffset, ok := checkedAdd(holesOffset, uint64(holeCount)*uint64(HoleSize))
	if !ok {
		return nil, ErrManifestTooLarge
	}
	totalSize, ok := checkedAdd(keyTableOffset, uint64(len(sealedKeyTable)))
	if !ok || totalSize > MaxManifestDecodedSize || totalSize > uint64(maxInt()) {
		return nil, fmt.Errorf("%w: %d > %d", ErrManifestTooLarge, totalSize, MaxManifestDecodedSize)
	}

	// The physical envelope owns the magic. Build only the logical bytes after
	// it so RAW Unmarshal can parse the physical payload without reconstructing
	// or copying the old layout.
	payload := make([]byte, int(totalSize)-len(manifestMagic))
	le := binary.LittleEndian

	// --- Logical header bytes after magic (60 bytes) ---
	payload[0] = m.Version
	payload[1] = uint8(m.ChunkMode)
	// payload[2:4] reserved, zero
	le.PutUint64(payload[4:12], m.ImageSize)
	le.PutUint32(payload[12:16], count)
	le.PutUint32(payload[16:20], m.MinChunkSize)
	le.PutUint32(payload[20:24], m.MaxChunkSize)
	le.PutUint64(payload[24:32], keyTableOffset)
	le.PutUint64(payload[32:40], uint64(len(sealedKeyTable)))
	le.PutUint32(payload[40:44], holeCount)
	le.PutUint64(payload[44:52], holesOffset)
	// payload[52:60] reserved, zero

	// --- Chunk Entries (56 bytes each) ---
	for i, e := range m.Entries {
		base := HeaderSize - len(manifestMagic) + i*EntrySize
		le.PutUint64(payload[base:base+8], e.Offset)
		le.PutUint32(payload[base+8:base+12], e.Size)
		var flags uint32
		if e.IsZero {
			flags |= EntryFlagZero
		}
		le.PutUint32(payload[base+12:base+16], flags)
		copy(payload[base+16:base+48], e.CiphertextHash[:])
		// payload[base+48:base+56] reserved, zero
	}

	// --- Hole Extents (16 bytes each) ---
	for i, h := range m.Holes {
		base := int(holesOffset) - len(manifestMagic) + i*HoleSize
		le.PutUint64(payload[base:base+8], h.Offset)
		le.PutUint64(payload[base+8:base+16], h.Size)
	}

	// --- Sealed Key Table ---
	copy(payload[int(keyTableOffset)-len(manifestMagic):], sealedKeyTable)

	encoded, err := encodeManifestPayload(payload)
	if err != nil {
		return nil, err
	}
	decodedLen, err := snappy.DecodedLen(encoded)
	if err != nil {
		return nil, fmt.Errorf("manifest: invalid Snappy encoder output length: %w", err)
	}
	if decodedLen != len(payload) {
		return nil, fmt.Errorf("manifest: invalid Snappy encoder output: encoded=%d decoded=%d, want %d", len(encoded), decodedLen, len(payload))
	}
	encoding := ManifestEncodingRaw
	physicalPayload := payload
	if objectformat.CompressionBeneficial(uint64(len(payload)), uint64(len(encoded))) {
		encoding = ManifestEncodingSnappy
		physicalPayload = encoded
	}
	physical := make([]byte, 5+len(physicalPayload))
	copy(physical[:4], manifestMagic[:])
	physical[4] = encoding
	copy(physical[5:], physicalPayload)
	return physical, nil
}

// Unmarshal deserializes the binary format into a Manifest and the raw sealed key table.
//
// The returned Manifest has an empty Keys slice; the caller should unseal the key table
// (via crypto.KeyTableEncryptor) and populate Keys separately.
func Unmarshal(data []byte) (*Manifest, []byte, error) {
	if len(data) < 5 {
		return nil, nil, ErrTooShort
	}
	if !bytes.Equal(data[:4], manifestMagic[:]) {
		return nil, nil, fmt.Errorf("%w: got %x", ErrBadMagic, data[:4])
	}

	var payload []byte
	switch data[4] {
	case ManifestEncodingRaw:
		payload = data[5:]
		if uint64(len(payload))+uint64(len(manifestMagic)) > MaxManifestDecodedSize {
			return nil, nil, fmt.Errorf("%w: %d > %d", ErrManifestTooLarge, len(payload)+len(manifestMagic), MaxManifestDecodedSize)
		}
	case ManifestEncodingSnappy:
		decodedLen, err := snappy.DecodedLen(data[5:])
		if err != nil {
			return nil, nil, fmt.Errorf("manifest: Snappy decoded length: %w", err)
		}
		if decodedLen < HeaderSize-len(manifestMagic) {
			return nil, nil, fmt.Errorf("%w: Snappy logical payload has %d bytes, need %d", ErrTooShort, decodedLen, HeaderSize-len(manifestMagic))
		}
		if decodedLen > MaxManifestDecodedSize-len(manifestMagic) {
			return nil, nil, fmt.Errorf("%w: %d > %d", ErrManifestTooLarge, decodedLen+len(manifestMagic), MaxManifestDecodedSize)
		}
		maxEncoded := snappy.MaxEncodedLen(decodedLen)
		if maxEncoded < 0 || len(data)-5 > maxEncoded {
			return nil, nil, fmt.Errorf("manifest: Snappy payload length %d exceeds maximum %d for %d decoded bytes", len(data)-5, maxEncoded, decodedLen)
		}
		if !objectformat.CompressionBeneficial(uint64(decodedLen), uint64(len(data)-5)) {
			return nil, nil, fmt.Errorf("%w: non-canonical Snappy payload length %d for %d decoded bytes", ErrBadEncoding, len(data)-5, decodedLen)
		}
		payload, err = snappy.Decode(nil, data[5:])
		if err != nil {
			return nil, nil, fmt.Errorf("manifest: Snappy decode: %w", err)
		}
		if len(payload) != decodedLen {
			return nil, nil, fmt.Errorf("manifest: Snappy decoded %d bytes, want %d", len(payload), decodedLen)
		}
	default:
		return nil, nil, fmt.Errorf("%w: 0x%02x", ErrBadEncoding, data[4])
	}
	return unmarshalLogicalPayload(payload)
}

func unmarshalLogicalPayload(payload []byte) (*Manifest, []byte, error) {
	if len(payload) < HeaderSize-len(manifestMagic) {
		return nil, nil, ErrTooShort
	}
	le := binary.LittleEndian
	version := payload[0]
	if version != Version1 {
		return nil, nil, fmt.Errorf("%w: got %d", ErrBadVersion, version)
	}

	chunkMode := ChunkMode(payload[1])
	imageSize := le.Uint64(payload[4:12])
	chunkCount := le.Uint32(payload[12:16])
	minChunkSize := le.Uint32(payload[16:20])
	maxChunkSize := le.Uint32(payload[20:24])
	keyTableOffset := le.Uint64(payload[24:32])
	keyTableSize := le.Uint64(payload[32:40])
	holeCount := le.Uint32(payload[40:44])
	holesOffset := le.Uint64(payload[44:52])
	logicalSize := uint64(len(payload) + len(manifestMagic))

	entriesEnd, ok := checkedAdd(uint64(HeaderSize), uint64(chunkCount)*uint64(EntrySize))
	if !ok || entriesEnd > logicalSize {
		return nil, nil, fmt.Errorf("%w: need %d bytes for entries, have %d", ErrTooShort, entriesEnd, logicalSize)
	}
	if holesOffset != entriesEnd {
		return nil, nil, fmt.Errorf("manifest: non-canonical holes offset %d, want %d", holesOffset, entriesEnd)
	}
	holesEnd, ok := checkedAdd(holesOffset, uint64(holeCount)*uint64(HoleSize))
	if !ok || holesEnd > logicalSize {
		return nil, nil, fmt.Errorf("%w: need %d bytes for holes, have %d", ErrTooShort, holesEnd, logicalSize)
	}
	if keyTableOffset != holesEnd {
		return nil, nil, fmt.Errorf("manifest: non-canonical key-table offset %d, want %d", keyTableOffset, holesEnd)
	}
	keyTableEnd, ok := checkedAdd(keyTableOffset, keyTableSize)
	if !ok || keyTableEnd > logicalSize {
		return nil, nil, fmt.Errorf("%w: need %d bytes for key table, have %d", ErrTooShort, keyTableEnd, logicalSize)
	}
	if keyTableEnd != logicalSize {
		return nil, nil, fmt.Errorf("manifest: trailing data: logical size %d, tables end at %d", logicalSize, keyTableEnd)
	}

	// --- Chunk Entries ---
	entries := make([]ChunkEntry, chunkCount)
	for i := range entries {
		base := uint64(HeaderSize-len(manifestMagic)) + uint64(i)*uint64(EntrySize)
		entries[i].Offset = le.Uint64(payload[base : base+8])
		entries[i].Size = le.Uint32(payload[base+8 : base+12])
		flags := le.Uint32(payload[base+12 : base+16])
		entries[i].IsZero = (flags & EntryFlagZero) != 0
		copy(entries[i].CiphertextHash[:], payload[base+16:base+48])
	}

	// --- Hole Extents ---
	var holes []sparse.Extent
	if holeCount > 0 {
		holes = make([]sparse.Extent, holeCount)
		for i := range holes {
			base := holesOffset - uint64(len(manifestMagic)) + uint64(i)*uint64(HoleSize)
			holes[i].Offset = le.Uint64(payload[base : base+8])
			holes[i].Size = le.Uint64(payload[base+8 : base+16])
		}
	}

	// --- Sealed Key Table ---
	sealedKeyTable := make([]byte, keyTableSize)
	keyStart := keyTableOffset - uint64(len(manifestMagic))
	copy(sealedKeyTable, payload[keyStart:keyStart+keyTableSize])

	m := &Manifest{
		Version:      version,
		ChunkMode:    chunkMode,
		ImageSize:    imageSize,
		MinChunkSize: minChunkSize,
		MaxChunkSize: maxChunkSize,
		Entries:      entries,
		Holes:        holes,
	}

	if err := validateChunkSizes(m); err != nil {
		return nil, nil, err
	}
	if err := m.ValidateGeometry(); err != nil {
		return nil, nil, err
	}

	return m, sealedKeyTable, nil
}

func encodeManifestPayload(payload []byte) (encoded []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			encoded = nil
			err = fmt.Errorf("manifest: Snappy encode failed: %v", recovered)
		}
	}()
	return snappy.Encode(nil, payload), nil
}

func validateChunkSizes(m *Manifest) error {
	for i, entry := range m.Entries {
		if entry.Size > MaxChunkDecodedSize || entry.Size > m.MaxChunkSize {
			return fmt.Errorf("%w: entry %d size %d, declared max %d, hard max %d", ErrBadChunkSize, i, entry.Size, m.MaxChunkSize, MaxChunkDecodedSize)
		}
	}
	return nil
}

func checkedAdd(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}

func maxInt() int {
	return int(^uint(0) >> 1)
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
		end, ok := checkedAdd(e.Offset, uint64(e.Size))
		if !ok {
			return fmt.Errorf("%w: entry at offset %d overflows", ErrBadGeometry, e.Offset)
		}
		segs = append(segs, segment{e.Offset, end})
	}
	for _, h := range m.Holes {
		if h.Size == 0 {
			return fmt.Errorf("%w: hole at offset %d has zero size", ErrBadGeometry, h.Offset)
		}
		end, ok := checkedAdd(h.Offset, h.Size)
		if !ok {
			return fmt.Errorf("%w: hole at offset %d overflows", ErrBadGeometry, h.Offset)
		}
		segs = append(segs, segment{h.Offset, end})
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
