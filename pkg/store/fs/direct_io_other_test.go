//go:build !linux

package fs

import (
	"errors"
	"testing"
)

func TestDirectIOIsExplicitlyUnsupported(t *testing.T) {
	if _, err := readDirectFile("unused"); !errors.Is(err, ErrDirectIOUnsupported) {
		t.Fatalf("readDirectFile error = %v, want ErrDirectIOUnsupported", err)
	}
}
