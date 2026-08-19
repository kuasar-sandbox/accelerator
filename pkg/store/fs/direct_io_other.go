//go:build !linux

package fs

import (
	"fmt"
	"os"
	"runtime"
)

func openExclusiveNoFollow(path string, mode os.FileMode) (*os.File, error) {
	// O_CREATE|O_EXCL refuses an existing symlink rather than following it.
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
}

func openReadNoFollow(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("object is not a regular file (mode %s)", info.Mode())
	}
	return os.Open(path)
}

func readDirectFile(string) ([]byte, error) {
	return nil, fmt.Errorf("%w on %s", ErrDirectIOUnsupported, runtime.GOOS)
}
