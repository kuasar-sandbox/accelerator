package codec

import "encoding/binary"

// BuildAAD reconstructs the authenticated additional data used to
// seal/unseal the per-chunk key table. AAD covers the manifest's
// geometry — header subset (excluding key-table layout metadata),
// entries, and holes — so any tampering with the image's layout is
// detected at unseal time.
//
// Two callers historically had their own copies of this layout
// (pkg/ingest and cmd/manifest-ctl); they are now both routed
// through this helper to prevent drift.
//
// AAD layout:
//
//	[Header subset: 48 B]
//	[Entries N × 56 B]
//	[Holes M × 16 B]
//
// The header subset uses the same byte positions as the on-disk
// header up to and including HoleCount at offset 44. KeyTableOffset,
// KeyTableSize, HolesOffset, and the 8 reserved bytes are NOT in AAD
// (they are layout metadata, not geometry).
func BuildAAD(m *Manifest) []byte {
	count := m.ChunkCount()
	holeCount := uint32(len(m.Holes))
	aadLen := HeaderSize + int(count)*EntrySize + int(holeCount)*HoleSize
	buf := make([]byte, aadLen)
	le := binary.LittleEndian

	// Header subset.
	le.PutUint32(buf[0:4], Magic)
	buf[4] = m.Version
	buf[5] = uint8(m.ChunkMode)
	le.PutUint64(buf[8:16], m.ImageSize)
	le.PutUint32(buf[16:20], count)
	le.PutUint32(buf[20:24], m.MinChunkSize)
	le.PutUint32(buf[24:28], m.MaxChunkSize)
	// buf[28:36] keyTableOffset — NOT in AAD
	// buf[36:44] keyTableSize   — NOT in AAD
	le.PutUint32(buf[44:48], holeCount)
	// buf[48:56] holesOffset    — NOT in AAD
	// buf[56:64] reserved        — NOT in AAD; AAD ends at byte 48 of header

	// Entries.
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
	}

	// Holes.
	for i, h := range m.Holes {
		base := HeaderSize + int(count)*EntrySize + i*HoleSize
		le.PutUint64(buf[base:base+8], h.Offset)
		le.PutUint64(buf[base+8:base+16], h.Size)
	}

	return buf
}
