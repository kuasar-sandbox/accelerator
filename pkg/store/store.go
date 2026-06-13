// Package store provides the content-addressed storage types.
package store

import (
	"errors"
	"io"
)

var ErrNotFound = errors.New("store: not found")

// Partition distinguishes chunk, manifest, and blob storage.
type Partition string

const (
	PartitionChunk    Partition = "chunk"
	PartitionManifest Partition = "manifest"
	// PartitionBlob holds arbitrary content-addressed data. It is handled
	// identically to chunk/manifest (SHA256 key, generation scope, dedup);
	// the separate partition exists purely for logical isolation.
	PartitionBlob Partition = "blob"
)

// ContentKey is the content-addressed key for stored objects.
type ContentKey [32]byte

// PutHandle is the streaming-Put session abstraction shared by all
// backends (fs / obs / future). Lifecycle:
//
//   - Write any number of frames
//   - Commit (success path) or Abort (error path); both are terminal
//   - Write-after-Commit/Abort, or double-Commit, is a programmer
//     error — implementations may return error or panic
//
// Concrete implementations live in the backend package
// (fs.PutHandle, obs.putHandle, …); the server depends only on
// this interface.
type PutHandle interface {
	io.Writer

	// Commit closes the handle, verifies the streamed bytes against
	// verifyDigest (verifyDigest != key ⇒ key-mismatch error and
	// staged data discarded), and persists the bytes under the
	// content-addressed path. Returns isNew=false on dedup-race
	// short-circuit (another writer landed the same key while we
	// were streaming).
	Commit(key ContentKey, verifyDigest ContentKey) (isNew bool, err error)

	// Abort discards any buffered or staged data. Idempotent and
	// safe to call after Commit (no-op).
	Abort() error
}
