//go:build !linux

package fs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDirectIOIsExplicitlyUnsupported(t *testing.T) {
	if _, err := readDirectFile("unused"); !errors.Is(err, ErrDirectIOUnsupported) {
		t.Fatalf("readDirectFile error = %v, want ErrDirectIOUnsupported", err)
	}
}

func TestOpenReadNoFollow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "object")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := openReadNoFollow(path)
	if err != nil {
		t.Fatalf("open regular file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := openReadNoFollow(link); err == nil {
		t.Fatal("openReadNoFollow accepted symlink")
	}
}
