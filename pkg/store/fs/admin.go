package fs

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// DropGeneration deletes data for one explicit generation. The caller must
// first remove that generation from its writable generation source.
func (s *Store) DropGeneration(ctx context.Context, generation store.Generation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateGeneration(generation); err != nil {
		return err
	}
	for _, partition := range allPartitions() {
		path := filepath.Join(s.root, string(partition), string(generation))
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("fs: remove generation data %s: %w", path, err)
		}
	}
	return nil
}

// Wipe deletes object data but never generation-source metadata.
func (s *Store) Wipe(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, partition := range allPartitions() {
		path := filepath.Join(s.root, string(partition))
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("fs: wipe partition %s: %w", path, err)
		}
	}
	return nil
}

// GenerationStats counts regular object paths under an explicit generation.
func (s *Store) GenerationStats(ctx context.Context, generation store.Generation) (map[store.Partition]int, error) {
	if err := validateGeneration(generation); err != nil {
		return nil, err
	}
	out := make(map[store.Partition]int)
	for _, partition := range allPartitions() {
		path := filepath.Join(s.root, string(partition), string(generation))
		count := 0
		err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if err != nil {
				if os.IsNotExist(err) {
					return fs.SkipDir
				}
				return err
			}
			if entry.Type().IsRegular() {
				count++
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("fs: walk %s: %w", path, err)
		}
		out[partition] = count
	}
	return out, nil
}

func allPartitions() []store.Partition {
	return []store.Partition{store.PartitionChunk, store.PartitionManifest, store.PartitionBlob}
}
