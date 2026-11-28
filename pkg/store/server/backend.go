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

// AliasBackend is the optional capability behind Resolve and
// PutHeader.represents. fs.Store and s3.Store implement it; a Backend that does
// not makes Resolve answer UNIMPLEMENTED and makes represents a no-op, which a
// client reads as "this store does not do cross-node delta compression".
//
// An alias is a write-once record in the generation, named by the independent
// address H_ind of a chunk and naming the delta object that represents it.
type AliasBackend interface {
	// GetAlias returns the delta key H_ind names, or (zero, false, nil).
	GetAlias(ctx context.Context, generation store.Generation, hInd store.ContentKey) (store.ContentKey, bool, error)

	// CreateAlias makes hInd name delta unless it already names something. It
	// returns the key hInd names afterwards and whether this call created it.
	CreateAlias(ctx context.Context, generation store.Generation, hInd, delta store.ContentKey) (current store.ContentKey, created bool, err error)

	// Delete removes one object. A missing object is not an error. The server
	// uses it for exactly one thing: removing a delta object that this very
	// Put created and that then lost the alias race, so that losing patches do
	// not accumulate as unreferenced bytes.
	Delete(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) error
}
