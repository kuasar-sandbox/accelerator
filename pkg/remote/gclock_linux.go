//go:build linux

package remote

import (
	"os"
	"syscall"
)

// lockFile takes an exclusive advisory lock on path (creating it), returning
// an unlock func. Serialises the cache GC sweep across flatten-ctl processes
// so concurrent prunes don't race on the same blobs.
func lockFile(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
