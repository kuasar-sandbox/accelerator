//go:build !linux

package main

import (
	"fmt"
	"io"
	"os"
)

// holeFillPunch on non-Linux platforms logs a warning and falls back
// to zero-fill. FALLOC_FL_PUNCH_HOLE is a Linux-only ioctl extension.
func holeFillPunch(_ *os.File) func(io.Writer, uint64, uint64) error {
	fmt.Fprintln(os.Stderr, "warn: --hole=punch requires Linux; falling back to --hole=zero")
	return holeFillZero
}
