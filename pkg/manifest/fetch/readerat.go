package fetch

import (
	"context"
	"io"
)

// readerAt adapts a Stream to a standard io.ReaderAt so callers such as
// archive/zip (trailing-ZIP EOCD scan + per-entry reads) and
// flatten.ReadConfig / flatten.ReadEROFSSize can read image metadata
// directly over the chunk-granular fetch path — no full
// materialization. Stream's block-aligned ReadAtBlock means the small
// tail/superblock reads all land on the same chunks, so cache-ctl dedup
// stays effective. Holes are zero-filled per ReadAtBlock semantics.
//
// Safe for concurrent ReadAt: Stream's methods are concurrency-safe.
type readerAt struct {
	ctx  context.Context
	s    Stream
	size int64
}

// NewReaderAt wraps s as an io.ReaderAt over [0, size). ctx is carried
// into every underlying ReadAtBlock; cancel it to abort in-flight
// fetches.
func NewReaderAt(ctx context.Context, s Stream, size int64) io.ReaderAt {
	return &readerAt{ctx: ctx, s: s, size: size}
}

func (r *readerAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if off >= r.size {
		return 0, io.EOF
	}
	return r.s.ReadAtBlock(r.ctx, p, uint64(off))
}
