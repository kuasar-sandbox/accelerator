package fetch

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
)

// fileStream is a Stream backed by a local file. It is sparse-aware: holes are
// detected once at open via SEEK_DATA/SEEK_HOLE so RunAt can classify offsets
// as Data or Hole (allowing a sparse file to fall through correctly when used
// as an overlay layer, or to be the final merged hole when used as a base).
//
// fileStream does not implement ChunkStream — a flat file has no chunks; ReadAt
// is a single pread (the kernel reads holes as zeros).
type fileStream struct {
	f     *os.File
	size  uint64
	holes []codec.HoleExtent // sorted, disjoint; empty on non-sparse files
}

// OpenFileStream opens path read-only and returns it as a Stream. The caller
// owns the returned stream and must Close it.
func OpenFileStream(path string) (Stream, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("fetch: open %s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: stat %s: %w", path, err)
	}
	size := uint64(st.Size())
	holes, err := codec.DetectHoles(f, size)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("fetch: detect holes %s: %w", path, err)
	}
	return &fileStream{f: f, size: size, holes: holes}, nil
}

func (s *fileStream) Size() uint64 { return s.size }
func (s *fileStream) Close() error { return s.f.Close() }

// RunAt classifies offset as Data (allocated) or Hole (sparse) and bounds the
// run at the next data/hole boundary (clamped to offset+limit and Size).
func (s *fileStream) RunAt(offset, limit uint64) (RunKind, uint64, error) {
	if offset >= s.size {
		return 0, 0, io.EOF
	}
	limEnd := offset + limit
	if limEnd < offset || limEnd > s.size {
		limEnd = s.size
	}
	if h, in := findHoleAt(s.holes, offset); in {
		end := h.Offset + h.Size
		if end > limEnd {
			end = limEnd
		}
		return Hole, end, nil
	}
	return Data, nextHoleStart(s.holes, offset, limEnd), nil
}

// ReadAt is a single pread; sparse holes are read as zeros by the kernel.
func (s *fileStream) ReadAt(_ context.Context, buf []byte, offset uint64) (int, error) {
	if offset >= s.size {
		return 0, io.EOF
	}
	n := len(buf)
	var eof error
	if offset+uint64(n) > s.size {
		n = int(s.size - offset)
		eof = io.EOF
	}
	m, err := s.f.ReadAt(buf[:n], int64(offset))
	if err != nil && err != io.EOF {
		return m, fmt.Errorf("fetch: file read @ %d: %w", offset, err)
	}
	return m, eof
}
