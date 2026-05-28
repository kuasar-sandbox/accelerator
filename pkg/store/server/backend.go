// Package server implements the gRPC store-ctl service. It wraps a
// filesystem backend (or future object-storage backend) and exposes
// GetSalt / Get / Put over gRPC, with streaming transport for Get
// and Put to avoid buffering large payloads in memory.
package server

import (
	"context"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
)

// Backend is the set of operations the store-ctl server needs from
// its underlying storage implementation. The interface stays small
// and server-only.
//
// Implementations: fs.Store (local filesystem) and obs.Store (vendor
// Cloud OBS via S3-compatible API).
type Backend interface {
	// Get fetches an object by key under the given partition.
	// Returns (true, data, nil) on hit, (false, nil, nil) on miss.
	Get(ctx context.Context, partition store.Partition, key store.ContentKey) (bool, []byte, error)

	// Put writes data under the given partition. The caller-supplied
	// key is authoritative for path resolution. Returns isNew=true
	// when a new object was written, false on dedup hit.
	Put(ctx context.Context, partition store.Partition, key store.ContentKey, data []byte) (bool, error)

	// OpenPut returns a streaming-Put handle for the given partition.
	// Callers write bytes, then Commit (with the claimed key and a
	// verification digest) or Abort. The PutHandle contract is
	// defined in pkg/store.
	OpenPut(partition store.Partition) (store.PutHandle, error)

	// Exists reports whether the given key already lives under the
	// partition's active generation (a cheap stat check, not a
	// reverse-generation search). Used by the Put handler to
	// SendAndClose early on dedup hits.
	Exists(partition store.Partition, key store.ContentKey) bool

	// ActiveGeneration returns the generation the backend is
	// writing new objects to. Surfaced via GetSalt.
	ActiveGeneration() string
}
