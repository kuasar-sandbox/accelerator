// Package tarstream packages one sparse-capable file as a tar stream
// and reads it back without materializing anything: WriteTo emits the
// logical view plus its hole map as a GNU PAX sparse 1.0 entry
// (byte-mirroring GNU tar's encoding, including its trailing (size,0)
// sentinel extent), ReadFrom / ReadSeekFrom return the logical view
// together with the exact hole map recovered from the encoding — only
// data bytes ever flow, holes cost nothing on the wire.
//
// The tar envelope keeps the format inspectable and interoperable:
// GNU tar extracts the stream back into a sparse file, Go's
// archive/tar reads the logical bytes, and this package round-trips
// the hole map. The reverse holds too: ReadFrom understands archives
// produced by `tar --format=posix --sparse` (the modern GNU sparse
// encoding); the legacy GNU binary sparse format and PAX 0.x variants
// are rejected with ErrUnsupportedEncoding.
//
// The package is stdlib-only and lives at the dependency root so every
// repo in the platform can import it.
package tarstream

import (
	"errors"
	"io"
)

// Hole marks [Offset, Offset+Length) of the logical file as a hole.
type Hole struct {
	Offset, Length int64
}

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
	Holes() []Hole
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

// holesToExtents validates and normalizes a hole map and inverts it
// into data extents. Holes may be unsorted; zero-length holes are
// dropped, adjacent holes merge, overlap and out-of-bounds are errors.
// sparse=false means there is nothing to encode.
func holesToExtents(size int64, holes []Hole) ([]extent, bool, error) {
	if size < 0 {
		return nil, false, errors.New("negative size")
	}
	hs := make([]Hole, 0, len(holes))
	for _, h := range holes {
		if h.Length == 0 {
			continue
		}
		if h.Length < 0 || h.Offset < 0 || h.Offset+h.Length > size {
			return nil, false, errors.New("hole out of bounds")
		}
		hs = append(hs, h)
	}
	if len(hs) == 0 {
		return nil, false, nil
	}
	sortHoles(hs)
	merged := hs[:1]
	for _, h := range hs[1:] {
		last := &merged[len(merged)-1]
		switch {
		case h.Offset < last.Offset+last.Length:
			return nil, false, errors.New("overlapping holes")
		case h.Offset == last.Offset+last.Length: // adjacent: merge
			last.Length += h.Length
		default:
			merged = append(merged, h)
		}
	}
	var extents []extent
	var pos int64
	for _, h := range merged {
		if h.Offset > pos {
			extents = append(extents, extent{Offset: pos, Size: h.Offset - pos})
		}
		pos = h.Offset + h.Length
	}
	if pos < size {
		extents = append(extents, extent{Offset: pos, Size: size - pos})
	}
	return extents, true, nil
}

// extentsToHoles inverts data extents (sorted, non-overlapping) back
// into the canonical hole map over [0, size).
func extentsToHoles(size int64, extents []extent) []Hole {
	var holes []Hole
	var pos int64
	for _, e := range extents {
		if e.Offset > pos {
			holes = append(holes, Hole{Offset: pos, Length: e.Offset - pos})
		}
		pos = e.Offset + e.Size
	}
	if pos < size {
		holes = append(holes, Hole{Offset: pos, Length: size - pos})
	}
	return holes
}

func sortHoles(hs []Hole) {
	// insertion sort: hole maps are small and often already sorted.
	for i := 1; i < len(hs); i++ {
		for j := i; j > 0 && hs[j].Offset < hs[j-1].Offset; j-- {
			hs[j], hs[j-1] = hs[j-1], hs[j]
		}
	}
}
