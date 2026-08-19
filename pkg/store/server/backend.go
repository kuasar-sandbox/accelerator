// Package server implements the gRPC store-ctl service. It wraps a
// filesystem backend (or future object-storage backend) and exposes
// AdmitWrite / Get / Put over gRPC, with streaming transport for Get
// and Put to avoid buffering large payloads in memory.
package server

import (
	"context"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Backend is the set of operations the store-ctl server needs from
// its underlying storage implementation. The interface stays small
// and server-only.
//
// Implementations: fs.Store (local filesystem) and s3.Store
// (S3-compatible object storage via the S3 API).
type Backend interface {
	// Get fetches an object from exactly one generation.
	// Returns (true, data, nil) on hit, (false, nil, nil) on miss.
	Get(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) (bool, []byte, error)

	// OpenPut returns a streaming-Put handle for the given partition.
	// Callers write bytes, then Commit (with the claimed key and a
	// verification digest) or Abort. The PutHandle contract is
	// defined in pkg/store.
	OpenPut(generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (store.PutHandle, error)

	// Exists checks exactly one generation. A non-nil expectedSize
	// requires a regular object of exactly that size; nil uses existence
	// semantics. Storage errors are never converted into misses.
	Exists(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (bool, error)
}
