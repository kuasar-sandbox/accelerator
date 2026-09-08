//go:build no_rocksdb

package rocks

import (
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

func TestOpenWithoutRocksDB(t *testing.T) {
	s, err := Open(runtime.RocksConfig{Path: t.TempDir()}, runtime.FreqConfig{})
	if s != nil {
		t.Fatalf("Open: store=%v, want nil", s)
	}
	if !errors.Is(err, ErrNotCompiled) {
		t.Fatalf("Open: err=%v, want ErrNotCompiled", err)
	}
}

func TestOpenReadOnlyWithoutRocksDB(t *testing.T) {
	s, err := OpenReadOnly(runtime.RocksConfig{Path: t.TempDir()})
	if s != nil {
		t.Fatalf("OpenReadOnly: store=%v, want nil", s)
	}
	if !errors.Is(err, ErrNotCompiled) {
		t.Fatalf("OpenReadOnly: err=%v, want ErrNotCompiled", err)
	}
}
