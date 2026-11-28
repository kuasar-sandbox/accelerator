package fs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// aliasTmpDir is where alias bodies are staged before being linked into place.
// It sits beside the generation directories; it cannot collide with one because
// generation names must start with an alphanumeric character.
const aliasTmpDir = ".tmp"

// maxAliasRaces bounds the create/read retry loop. Losing the link race and then
// finding the winner gone again needs a concurrent purge of that exact alias, so
// two rounds is already pathological.
const maxAliasRaces = 4

// Aliases are one file each:
//
//	{root}/alias/{generation}/{hex[:2]}/{hex[2:4]}/{H_ind}
//
// The file is 33 bytes: version 0x01 and the 32-byte delta key the alias names.
//
// GetAlias reads that file and returns the delta key it names in one generation.
// A missing alias is (zero, false, nil); a present but malformed one is an error.
func (s *Store) GetAlias(ctx context.Context, generation store.Generation, hInd store.ContentKey) (store.ContentKey, bool, error) {
	if err := ctx.Err(); err != nil {
		return store.ContentKey{}, false, err
	}
	if err := validateGeneration(generation); err != nil {
		return store.ContentKey{}, false, err
	}
	path := s.objectPath(store.PartitionAlias, generation, hInd)
	body, err := readBufferedFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return store.ContentKey{}, false, nil
		}
		return store.ContentKey{}, false, fmt.Errorf("fs: read alias %s: %w", path, err)
	}
	delta, err := store.DecodeAlias(body)
	if err != nil {
		return store.ContentKey{}, false, fmt.Errorf("fs: alias %s: %w", path, err)
	}
	return delta, true, nil
}

// CreateAlias makes hInd name delta unless an alias already exists. It returns
// the key the alias names afterwards and whether this call created it.
//
// The 33-byte body is written and synced to a staging file under
// {root}/alias/.tmp, then published with link(2). link fails if the name
// already exists, so the first writer wins and a reader never sees a
// half-written alias: the object path either does not exist or holds a complete
// body. The parent directory is synced after a successful link. (OpenPut's
// create-exclusive-in-place is right for large objects whose partial state is
// detected by size, but a 33-byte alias read mid-write could not be told from a
// corrupt one.)
//
// If the name exists, the existing alias is read and returned with created
// false. If that winner disappears between the failed link and the read, the
// create is retried, up to maxAliasRaces times, and then fails.
func (s *Store) CreateAlias(ctx context.Context, generation store.Generation, hInd, delta store.ContentKey) (store.ContentKey, bool, error) {
	if err := ctx.Err(); err != nil {
		return store.ContentKey{}, false, err
	}
	if err := validateGeneration(generation); err != nil {
		return store.ContentKey{}, false, err
	}
	final := s.objectPath(store.PartitionAlias, generation, hInd)
	if err := s.ops.mkdirAll(filepath.Dir(final), 0o755); err != nil {
		return store.ContentKey{}, false, fmt.Errorf("fs: create alias parent: %w", err)
	}

	tmp, err := s.stageAlias(delta)
	if err != nil {
		return store.ContentKey{}, false, err
	}
	defer os.Remove(tmp)

	for range maxAliasRaces {
		err := os.Link(tmp, final)
		if err == nil {
			if err := syncDirectory(filepath.Dir(final)); err != nil {
				return store.ContentKey{}, false, fmt.Errorf("fs: sync alias parent: %w", err)
			}
			return delta, true, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return store.ContentKey{}, false, fmt.Errorf("fs: publish alias %s: %w", final, err)
		}
		current, ok, err := s.GetAlias(ctx, generation, hInd)
		if err != nil {
			return store.ContentKey{}, false, err
		}
		if ok {
			return current, false, nil
		}
		// The winner vanished between our link and our read; try to create again.
	}
	return store.ContentKey{}, false, fmt.Errorf("fs: alias %s kept appearing and disappearing", final)
}

// Delete removes one object from one generation. A missing object is not an
// error. It exists for the server's discard of a delta object that lost the
// alias race (see server.AliasBackend); it is not a general-purpose delete and
// refuses the alias partition. An alias is write-once, so it goes away only
// when its generation is dropped.
func (s *Store) Delete(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateGeneration(generation); err != nil {
		return err
	}
	if partition == store.PartitionAlias {
		return fmt.Errorf("fs: aliases are write-once and cannot be deleted individually")
	}
	path := s.objectPath(partition, generation, key)
	if err := s.ops.remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("fs: delete %s: %w", path, err)
	}
	return nil
}

// stageAlias writes the 33-byte alias body to a fresh file under
// {root}/alias/.tmp and syncs it before the caller links it into place.
func (s *Store) stageAlias(delta store.ContentKey) (string, error) {
	dir := filepath.Join(s.root, string(store.PartitionAlias), aliasTmpDir)
	if err := s.ops.mkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("fs: create alias staging dir: %w", err)
	}
	f, err := os.CreateTemp(dir, "alias-*")
	if err != nil {
		return "", fmt.Errorf("fs: stage alias: %w", err)
	}
	path := f.Name()
	fail := func(err error) (string, error) {
		_ = f.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("fs: stage alias: %w", err)
	}
	if _, err := f.Write(store.EncodeAlias(delta)); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("fs: stage alias: %w", err)
	}
	return path, nil
}
