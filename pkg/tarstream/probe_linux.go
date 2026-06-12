//go:build linux

package tarstream

import (
	"errors"
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

// ProbeHoles maps the holes of f via SEEK_DATA/SEEK_HOLE, returning
// the canonical hole map WriteTo expects (nil when the file is dense
// or the filesystem cannot answer — storing dense is the safe
// fallback). The file offset is left at the start.
func ProbeHoles(f *os.File) ([]Hole, error) {
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	defer f.Seek(0, io.SeekStart)
	if size == 0 {
		return nil, nil
	}
	// Cheap pre-filter: a file whose allocated blocks cover its size
	// has no holes worth probing.
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err == nil {
		if st.Blocks*512 >= size {
			return nil, nil
		}
	}

	var extents []extent
	var off int64
	for off < size {
		dataStart, err := f.Seek(off, seekData)
		if err != nil {
			if errors.Is(err, syscall.ENXIO) {
				break // rest of the file is one hole
			}
			if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) {
				return nil, nil // filesystem can't probe: treat as dense
			}
			return nil, err
		}
		if dataStart >= size {
			break
		}
		holeStart, err := f.Seek(dataStart, seekHole)
		if err != nil {
			return nil, err
		}
		if holeStart > size {
			holeStart = size
		}
		extents = append(extents, extent{Offset: dataStart, Size: holeStart - dataStart})
		off = holeStart
	}
	if len(extents) == 1 && extents[0] == (extent{0, size}) {
		return nil, nil // dense after all
	}
	return extentsToHoles(size, extents), nil
}
