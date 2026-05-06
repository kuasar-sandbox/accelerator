package flatten

import (
	"encoding/binary"
	"fmt"
	"io"
)

// EROFS superblock layout — only the fields we read are documented
// here. Full layout is in the Linux kernel
// fs/erofs/erofs_fs.h: erofs_super_block.
//
//	offset 0   magic            4 B
//	offset 4   checksum         4 B
//	offset 8   feature_compat   4 B
//	offset 12  blkszbits        1 B  (log2 block size; usually 12 → 4 KiB)
//	offset 13  sb_extslots      1 B
//	offset 14  root_nid         2 B
//	offset 16  inos             8 B
//	offset 24  build_time       8 B
//	offset 32  build_time_nsec  4 B
//	offset 36  blocks           4 B  (total blocks in image)
//	...
const (
	erofsSuperOffset = 1024
	erofsSuperSize   = 128
	erofsMagic       = 0xE0F5E1E2

	erofsOffMagic   = 0  // 4 B
	erofsOffBlkBits = 12 // 1 B
	erofsOffBlocks  = 36 // 4 B
)

// ReadEROFSSize parses the EROFS superblock at offset 1024 of r and
// returns the image size in bytes (= blocks << blkszbits).
//
// Returns a clear error when the magic doesn't match — typical when
// callers point this at a non-EROFS file or a buffer that doesn't
// start at the EROFS image's offset 0.
func ReadEROFSSize(r io.ReaderAt) (uint64, error) {
	var sb [erofsSuperSize]byte
	if _, err := r.ReadAt(sb[:], erofsSuperOffset); err != nil {
		return 0, fmt.Errorf("erofs: read superblock: %w", err)
	}
	magic := binary.LittleEndian.Uint32(sb[erofsOffMagic : erofsOffMagic+4])
	if magic != erofsMagic {
		return 0, fmt.Errorf("erofs: bad magic 0x%08X at offset %d (expected 0x%08X)", magic, erofsSuperOffset, erofsMagic)
	}
	blkszbits := sb[erofsOffBlkBits]
	if blkszbits == 0 || blkszbits > 30 {
		// Sanity: blkszbits 0 means 1-byte blocks (corrupt/bogus);
		// >30 would overflow uint64 multiplication for any sane
		// blocks count.
		return 0, fmt.Errorf("erofs: implausible blkszbits %d", blkszbits)
	}
	blocks := binary.LittleEndian.Uint32(sb[erofsOffBlocks : erofsOffBlocks+4])
	return uint64(blocks) << blkszbits, nil
}
