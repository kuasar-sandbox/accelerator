package tar

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// zeroBlockSize is the hole-detection granularity of CopySparse,
// matching the common filesystem block size.
const zeroBlockSize = 4096

var zeroBlock [zeroBlockSize]byte

// CopySparse copies size logical bytes from r into dst, seeking over
// zero runs (4 KiB granularity) so they become holes, and truncates
// dst to size at the end (which also materializes a trailing hole).
// It works on the decoded byte stream, so holes come back whether the
// archive encoded them sparsely or stored dense zeros. dst must be a
// fresh file.
func CopySparse(dst *os.File, r io.Reader, size int64) error {
	buf := make([]byte, 32*zeroBlockSize)
	var off int64
	for off < size {
		want := min(int64(len(buf)), size-off)
		n, err := io.ReadFull(r, buf[:want])
		if n > 0 {
			if werr := writePunched(dst, buf[:n], off); werr != nil {
				return werr
			}
			off += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if off != size {
				return fmt.Errorf("short entry data: %d of %d bytes", off, size)
			}
			break
		}
		if err != nil {
			return err
		}
	}
	return dst.Truncate(size)
}

// writePunched writes chunk at file offset base, skipping (seeking
// over) the zero blocks inside it.
func writePunched(dst *os.File, chunk []byte, base int64) error {
	for len(chunk) > 0 {
		blk := min(zeroBlockSize, len(chunk))
		if bytes.Equal(chunk[:blk], zeroBlock[:blk]) {
			base += int64(blk)
			chunk = chunk[blk:]
			continue
		}
		// Coalesce consecutive non-zero blocks into one write.
		end := blk
		for end < len(chunk) {
			next := min(zeroBlockSize, len(chunk)-end)
			if bytes.Equal(chunk[end:end+next], zeroBlock[:next]) {
				break
			}
			end += next
		}
		if _, err := dst.WriteAt(chunk[:end], base); err != nil {
			return err
		}
		base += int64(end)
		chunk = chunk[end:]
	}
	return nil
}
