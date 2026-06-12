//go:build linux

package sparse

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

// lseek whence values for hole probing; not exported by the syscall
// package but stable kernel ABI since Linux 3.1.
const (
	seekData = 3 // SEEK_DATA
	seekHole = 4 // SEEK_HOLE
)

// ProbeHoles enumerates the filesystem holes of f via
// lseek(SEEK_HOLE/SEEK_DATA) — allocation metadata, not content: an
// allocated block of written zeros is data. The returned extents are
// sorted, disjoint and within [0, size); nil means dense. On
// filesystems that cannot answer (EINVAL/EOPNOTSUPP) it returns nil —
// treating the file as dense is the safe degradation. The file offset
// is restored to the start.
func ProbeHoles(f *os.File) ([]Extent, error) {
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("sparse: probe %s: %w", f.Name(), err)
	}
	defer f.Seek(0, io.SeekStart)
	if size == 0 {
		return nil, nil
	}
	fd := int(f.Fd())

	// Cheap pre-filter: a file whose allocated blocks cover its size
	// has no holes worth probing.
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err == nil {
		if st.Blocks*512 >= size {
			return nil, nil
		}
	}

	var holes []Extent
	cursor := int64(0)
	for cursor < size {
		holeStart, err := syscall.Seek(fd, cursor, seekHole)
		if err != nil {
			if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) {
				return nil, nil // filesystem can't probe: treat as dense
			}
			if errors.Is(err, syscall.ENXIO) {
				break // cursor past EOF (file shrank?): nothing more
			}
			return nil, fmt.Errorf("sparse: SEEK_HOLE @ %d: %w", cursor, err)
		}
		if holeStart >= size {
			break // only the virtual hole at EOF remains
		}
		dataStart, err := syscall.Seek(fd, holeStart, seekData)
		if err != nil {
			if errors.Is(err, syscall.ENXIO) {
				dataStart = size // hole runs to EOF
			} else {
				return nil, fmt.Errorf("sparse: SEEK_DATA @ %d: %w", holeStart, err)
			}
		}
		if dataStart > size {
			dataStart = size
		}
		if dataStart > holeStart {
			holes = append(holes, Extent{Offset: uint64(holeStart), Size: uint64(dataStart - holeStart)})
		}
		cursor = dataStart
	}
	return holes, nil
}
