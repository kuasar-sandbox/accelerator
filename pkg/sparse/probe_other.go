//go:build !linux

package sparse

import "os"

// ProbeHoles is a no-op on non-Linux platforms — SEEK_HOLE/SEEK_DATA
// probing is Linux-specific here; files are treated as dense.
func ProbeHoles(_ *os.File) ([]Extent, error) {
	return nil, nil
}
