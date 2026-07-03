// Package tarstream packages one sparse-capable byte source as a tar
// stream and reads it back without materializing anything: WriteTo
// emits a sparse.Source's logical view with its hole map as a GNU PAX
// sparse 1.0 entry (byte-mirroring GNU tar's encoding, including its
// trailing (size,0) sentinel extent); ReadFrom / ReadSeekFrom return
// the logical view together with the exact hole map recovered from
// the encoding; SourceFrom opens the entry directly as a sparse.Source
// for pipeline consumers. Only data bytes flow on the wire — holes
// cost nothing.
//
// Zero-valued data is data: a source's Zero runs are written as
// literal zero bytes (synthesized, never read), never as holes — the
// envelope carries exactly two states, absent (hole) and present
// (data), and reading it back never produces Zero runs.
//
// The tar envelope keeps the format inspectable and interoperable:
// GNU tar extracts the stream back into a sparse file, Go's
// archive/tar reads the logical bytes, and this package round-trips
// the hole map. The reverse holds too: ReadFrom understands archives
// produced by `tar --format=posix --sparse` (the modern GNU sparse
// encoding); the legacy GNU binary sparse format and PAX 0.x variants
// are rejected with ErrUnsupportedEncoding.
//
// The package lives at the dependency root and depends only on the
// stdlib plus pkg/sparse.
package tarstream

import (
	"errors"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// Reader is the sequential logical view of the file inside a tar
// stream: Read yields the logical bytes, holes reading as zeros.
// Metadata is available as soon as the constructor returns (the
// sparse map precedes the data on the wire).
type Reader interface {
	io.Reader
	// Name is the entry name inside the tar stream.
	Name() string
	// Size is the logical file size.
	Size() int64
	// Holes returns the canonical hole map (sorted, merged; nil for a
	// dense file). The slice is a copy the caller may keep or modify.
	Holes() []sparse.Extent
}

// ReadSeeker adds random access to Reader: Seek positions within the
// logical file, mapping straight onto the packed data region of the
// underlying tar stream — no extraction, no copies.
type ReadSeeker interface {
	Reader
	io.Seeker
}

// ErrNotFound reports that the named entry (or, for an empty name, any
// regular file entry) is not present in the stream.
var ErrNotFound = errors.New("tarstream: entry not found")

// ErrUnsupportedEncoding reports a sparse member in the legacy GNU
// binary format or a PAX 0.x map, which this package does not decode;
// re-create the archive with `tar --format=posix --sparse` or WriteTo.
var ErrUnsupportedEncoding = errors.New("tarstream: unsupported sparse encoding (use GNU posix sparse 1.0)")

// extent is one data run of the logical file: bytes
// [Offset, Offset+Size) hold data.
type extent struct {
	Offset, Size int64
}

// extentsToHoles inverts data extents (sorted, non-overlapping) back
// into the canonical hole map over [0, size).
func extentsToHoles(size int64, extents []extent) []sparse.Extent {
	var holes []sparse.Extent
	var pos int64
	for _, e := range extents {
		if e.Offset > pos {
			holes = append(holes, sparse.Extent{Offset: uint64(pos), Size: uint64(e.Offset - pos)})
		}
		pos = e.Offset + e.Size
	}
	if pos < size {
		holes = append(holes, sparse.Extent{Offset: uint64(pos), Size: uint64(size - pos)})
	}
	return holes
}
