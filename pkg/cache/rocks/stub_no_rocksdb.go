//go:build no_rocksdb

package rocks

import (
	"errors"

	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
)

// ErrNotCompiled is returned by Open and OpenReadOnly when the binary was
// built with -tags no_rocksdb: the RocksDB implementation is compiled out
// and only Redis-compatible backends are available.
var ErrNotCompiled = errors.New(
	"rocks: backend is not compiled into this binary (built with no_rocksdb); use a Redis-compatible backend")

// Open never succeeds in the no_rocksdb build.
func Open(runtime.RocksConfig, runtime.FreqConfig) (Interface, error) {
	return nil, ErrNotCompiled
}

// OpenReadOnly never succeeds in the no_rocksdb build.
func OpenReadOnly(runtime.RocksConfig) (Interface, error) {
	return nil, ErrNotCompiled
}
