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
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("object is not a regular file (mode %s)", pathInfo.Mode())
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	openedInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		_ = f.Close()
		return nil, fmt.Errorf("object changed during no-follow open")
	}
	return f, nil
}

func readDirectFile(string) ([]byte, error) {
	return nil, fmt.Errorf("%w on %s", ErrDirectIOUnsupported, runtime.GOOS)
}
