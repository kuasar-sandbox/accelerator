//go:build !linux

package manifest

import "os"

// DetectHoles is a no-op on non-Linux platforms — SEEK_HOLE/SEEK_DATA
// availability and semantics vary, and the deployment target is
// Linux. Callers needing hole detection on other platforms should
// build the HoleExtent list explicitly and pass it through ingest's
// Config.Holes.
func DetectHoles(_ *os.File, _ uint64) ([]HoleExtent, error) {
	return nil, nil
}
