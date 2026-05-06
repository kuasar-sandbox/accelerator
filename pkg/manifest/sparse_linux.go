//go:build linux

package manifest

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// SEEK_DATA / SEEK_HOLE whence values (Linux ABI; not exported by Go's
// stdlib `os` package, only by golang.org/x/sys/unix). We avoid
// pulling that dep just for two constants.
const (
	seekData = 3
	seekHole = 4
)

// DetectHoles uses lseek(SEEK_HOLE / SEEK_DATA) to enumerate
// filesystem holes in f. The returned extents are sorted by Offset,
// pairwise disjoint, and contained within [0, size). size is the
// caller-provided file size (typically from f.Stat()) used as the
// upper bound for the walk.
//
// On filesystems that don't support sparse representations, the
// kernel reports the entire file as a single data extent (no holes)
// and this returns an empty slice — behaviour degrades cleanly to
// non-sparse ingest.
//
// ENXIO from SEEK_DATA on a tail of holes is converted to a final
// hole extent reaching size; ENXIO from SEEK_HOLE during the walk is
// surfaced as an error (shouldn't happen — every position has a
// trailing hole at EOF on a Linux fs).
func DetectHoles(f *os.File, size uint64) ([]HoleExtent, error) {
	if size == 0 {
		return nil, nil
	}
	fd := int(f.Fd())

	var holes []HoleExtent
	cursor := int64(0)
	limit := int64(size)

	for cursor < limit {
		// Find the next hole at or after cursor.
		holeStart, err := syscall.Seek(fd, cursor, seekHole)
		if err != nil {
			if errors.Is(err, syscall.ENXIO) {
				// No more holes; remainder is data.
				break
			}
			return nil, fmt.Errorf("manifest: seek SEEK_HOLE @ %d: %w", cursor, err)
		}
		if holeStart >= limit {
			// Hole, if any, starts past EOF; nothing to record.
			break
		}

		// Find where data resumes after the hole. ENXIO means the
		// hole runs to EOF (POSIX-style sparse-file tail).
		dataStart, err := syscall.Seek(fd, holeStart, seekData)
		if err != nil {
			if errors.Is(err, syscall.ENXIO) {
				dataStart = limit
			} else {
				return nil, fmt.Errorf("manifest: seek SEEK_DATA @ %d: %w", holeStart, err)
			}
		}
		if dataStart > limit {
			dataStart = limit
		}

		holeSize := uint64(dataStart - holeStart)
		if holeSize > 0 {
			holes = append(holes, HoleExtent{
				Offset: uint64(holeStart),
				Size:   holeSize,
			})
		}
		cursor = dataStart
	}

	// Restore file position so subsequent reads start from 0 (the
	// caller doesn't have to seek separately after detect).
	if _, err := f.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("manifest: rewind after hole detect: %w", err)
	}
	return holes, nil
}
