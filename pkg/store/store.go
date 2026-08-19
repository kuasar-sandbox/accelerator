// Package store provides the content-addressed storage types.
package store

import (
	"errors"
	"fmt"
	"io"
	"regexp"
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

// Generation identifies one physical generation of content-addressed
// objects. Generation lists are always ordered oldest to newest.
type Generation string

// WriteAdmission fixes one ingest to a generation and its convergent-
// encryption salt. It is routing information, not an authorization token.
type WriteAdmission struct {
	Generation Generation
	Salt       [32]byte
}

const maxGenerations = 1024

var generationNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidateGeneration rejects names that could escape the single path
// component reserved for a generation.
func ValidateGeneration(g Generation) error {
	if !generationNamePattern.MatchString(string(g)) || g == "." || g == ".." {
		return fmt.Errorf("store: unsafe generation %q (want [A-Za-z0-9][A-Za-z0-9._-]{0,127})", g)
	}
	return nil
}

// ValidateGenerations validates one complete oldest-to-newest generation
// list. It deliberately neither sorts nor removes duplicates.
func ValidateGenerations(gens []Generation) error {
	if len(gens) == 0 {
		return errors.New("store: generation list is empty")
	}
	if len(gens) > maxGenerations {
		return fmt.Errorf("store: generation list has %d entries, maximum is %d", len(gens), maxGenerations)
	}
	seen := make(map[Generation]struct{}, len(gens))
	for i, g := range gens {
		if err := ValidateGeneration(g); err != nil {
			return fmt.Errorf("generation[%d]: %w", i, err)
		}
		if _, ok := seen[g]; ok {
			return fmt.Errorf("store: duplicate generation %q", g)
		}
		seen[g] = struct{}{}
	}
	return nil
}

// PutHandle is the streaming-Put session abstraction shared by all
// backends (fs / s3 / future). Lifecycle:
//
//   - Write any number of frames
//   - Commit (success path) or Abort (error path); both are terminal
//   - Write-after-Commit/Abort, or double-Commit, is a programmer
//     error — implementations may return error or panic
//
// Concrete implementations live in the backend package
// (fs.PutHandle, s3.putHandle, …); the server depends only on
// this interface.
type PutHandle interface {
	io.Writer

	// Commit closes the handle and verifies the streamed bytes against
	// verifyDigest when the caller requested verification. Concrete
	// handles also reject a key different from the key bound by OpenPut.
	// Returns isNew=false when another writer won a dedup race.
	Commit(key ContentKey, verifyDigest ContentKey) (isNew bool, err error)

	// Abort discards buffered data or removes a directly written file
	// only while the handle still owns the final path. Idempotent and safe
	// to call after Commit (no-op).
	Abort() error
}
