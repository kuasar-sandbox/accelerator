// Package store implements content-addressed chunk storage backends.
package store

import (
	"context"
	"errors"
)

// ErrNotFound is returned when a chunk does not exist in the store.
var ErrNotFound = errors.New("store: chunk not found")

// ContentStore is the interface for chunk storage backends.
type ContentStore interface {
	// Put stores a chunk. If a chunk with the same hash already exists,
	// this is a no-op (put-if-not-exists / natural dedup).
	Put(ctx context.Context, hash [32]byte, data []byte) error

	// Get retrieves a chunk by its ciphertext hash.
	// Returns ErrNotFound if the chunk does not exist.
	Get(ctx context.Context, hash [32]byte) ([]byte, error)

	// Exists checks whether a chunk exists without retrieving it.
	Exists(ctx context.Context, hash [32]byte) (bool, error)

	// Delete removes a chunk. Used by GC (future).
	Delete(ctx context.Context, hash [32]byte) error
}
