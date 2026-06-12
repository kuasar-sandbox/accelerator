//go:build !linux

package tarstream

import "os"

// ProbeHoles has no portable implementation; files are treated as
// dense.
func ProbeHoles(*os.File) ([]Hole, error) { return nil, nil }
