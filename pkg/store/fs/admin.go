package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// ErrGenerationNotFound is returned by Drop when the named generation
// is not in the store's __meta/generations list.
var ErrGenerationNotFound = errors.New("fs: generation not found")

// ErrGenerationExists is returned by Rollout when the named
// generation is already in the list (would create a duplicate).
var ErrGenerationExists = errors.New("fs: generation already exists")

// ErrCannotDropActive is returned by Drop when the named generation
// is the currently-active one (the last entry in the meta list).
// Demoting active is rejected because it leaves the store with no
// writable generation; rotate to a new gen via Rollout first.
var ErrCannotDropActive = errors.New("fs: cannot drop the active generation")

// Rollout appends `gen` to the generations list and makes it active.
// `gen` must not already exist. The meta file is rewritten atomically;
// on-disk truth and the in-memory cache are updated together under
// the store lock.
func (s *Store) Rollout(_ context.Context, gen string) error {
	if gen == "" {
		return errors.New("fs: generation is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, g := range s.gens {
		if g == gen {
			return fmt.Errorf("%w: %q", ErrGenerationExists, gen)
		}
	}
	// Persist newest-last on disk (oldest-first), reverse of in-memory.
	oldestFirst := make([]string, 0, len(s.gens)+1)
	for i := len(s.gens) - 1; i >= 0; i-- {
		oldestFirst = append(oldestFirst, s.gens[i])
	}
	oldestFirst = append(oldestFirst, gen)
	if err := writeGenerationsFile(filepath.Join(s.root, metaDir, generationsFile), oldestFirst); err != nil {
		return err
	}
	// Prepend in-memory (newest-first).
	s.gens = append([]string{gen}, s.gens...)
	s.active = gen
	return nil
}

// Drop removes `gen` from the generations list and deletes every
// chunk + manifest object under that generation. Refuses to drop
// the active generation.
func (s *Store) Drop(_ context.Context, gen string) error {
	if gen == "" {
		return errors.New("fs: generation is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if gen == s.active {
		return fmt.Errorf("%w: %q", ErrCannotDropActive, gen)
	}
	idx := -1
	for i, g := range s.gens {
		if g == gen {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("%w: %q (known: %v)", ErrGenerationNotFound, gen, s.gens)
	}

	// Persist list-after-drop first; if data delete fails partway we
	// can re-run Drop and finish (the list-without-gen is the source
	// of truth for what's "live").
	remaining := append(append([]string(nil), s.gens[:idx]...), s.gens[idx+1:]...)
	oldestFirst := make([]string, len(remaining))
	for i := range remaining {
		oldestFirst[i] = remaining[len(remaining)-1-i]
	}
	if err := writeGenerationsFile(filepath.Join(s.root, metaDir, generationsFile), oldestFirst); err != nil {
		return err
	}
	s.gens = remaining

	// Delete data trees.
	for _, p := range []store.Partition{store.PartitionChunk, store.PartitionManifest} {
		dir := filepath.Join(s.root, string(p), gen)
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("fs: remove %s: %w", dir, err)
		}
	}
	return nil
}

// Wipe deletes every file under root, including the meta file. The
// store object becomes unusable after Wipe — callers must construct
// a fresh one via Init+New if they want to reuse the same root.
func (s *Store) Wipe(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.RemoveAll(s.root); err != nil {
		return fmt.Errorf("fs: wipe %s: %w", s.root, err)
	}
	s.gens = nil
	s.active = ""
	return nil
}

// GenerationStats returns the number of objects per partition under
// the given generation. Walks the on-disk tree once; cheap for
// dev-scale stores, linear-but-bounded for production.
func (s *Store) GenerationStats(_ context.Context, gen string) (map[store.Partition]int, error) {
	out := make(map[store.Partition]int)
	for _, p := range []store.Partition{store.PartitionChunk, store.PartitionManifest} {
		dir := filepath.Join(s.root, string(p), gen)
		count := 0
		_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return filepath.SkipDir
				}
				return nil
			}
			if !d.IsDir() {
				count++
			}
			return nil
		})
		out[p] = count
	}
	return out, nil
}
