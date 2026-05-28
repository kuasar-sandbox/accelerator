//go:build linux

package main

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

// holeFillPunch returns an OnHole callback that produces a sparse
// hole in f covering the given range. The implementation:
//
//  1. Truncate f up to the end of the hole. Truncate-on-extend
//     creates a sparse tail with no allocated blocks — this is what
//     gives `stat` apparent size = ImageSize while keeping disk
//     allocation tight.
//  2. fallocate(PUNCH_HOLE | KEEP_SIZE) within the requested range.
//     Redundant when we extended via truncate (the tail is already
//     sparse), but explicit & safe when a previous run left a
//     larger file behind that the OS reused.
//  3. Seek to the end of the hole so subsequent data writes go to
//     the right offset.
//
// Linux-only because of FALLOC_FL_PUNCH_HOLE; non-Linux builds get
// a stub in punch_other.go that falls back to zero-fill.
func holeFillPunch(f *os.File) func(io.Writer, uint64, uint64) error {
	return func(_ io.Writer, off, sz uint64) error {
		end := off + sz
		// Step 1: extend the file up to the end of this hole if it
		// isn't already that long. Truncate-on-extend creates a
		// sparse tail (no allocated blocks).
		if err := f.Truncate(int64(end)); err != nil {
			return fmt.Errorf("ftruncate %d: %w", end, err)
		}
		// Step 2: punch the addressable range. With KEEP_SIZE the
		// file size stays at `end` (set by Truncate above).
		const punchMode = 0x02 | 0x01 // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
		if err := syscall.Fallocate(int(f.Fd()), punchMode, int64(off), int64(sz)); err != nil {
			return fmt.Errorf("fallocate punch [%d, %d): %w", off, end, err)
		}
		// Step 3: seek so subsequent data writes resume past the hole.
		if _, err := f.Seek(int64(end), io.SeekStart); err != nil {
			return fmt.Errorf("seek past punched hole: %w", err)
		}
		return nil
	}
}
