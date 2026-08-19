package s3

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

func (s *Store) DropGeneration(ctx context.Context, generation store.Generation) error {
	if err := store.ValidateGeneration(generation); err != nil {
		return fmt.Errorf("s3: %w", err)
	}
	for _, partition := range s3Partitions() {
		prefix := path.Join(s.prefix, string(partition), string(generation)) + "/"
		if err := s.deleteUnder(ctx, prefix); err != nil {
			return fmt.Errorf("s3: delete under %s: %w", prefix, err)
		}
	}
	return nil
}

// Wipe deletes object partitions only and never generation-source metadata.
func (s *Store) Wipe(ctx context.Context) error {
	for _, partition := range s3Partitions() {
		prefix := path.Join(s.prefix, string(partition)) + "/"
		if err := s.deleteUnder(ctx, prefix); err != nil {
			return fmt.Errorf("s3: wipe under %s: %w", prefix, err)
		}
	}
	return nil
}

func (s *Store) GenerationStats(ctx context.Context, generation store.Generation) (map[store.Partition]int, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	out := make(map[store.Partition]int)
	for _, partition := range s3Partitions() {
		prefix := path.Join(s.prefix, string(partition), string(generation)) + "/"
		count := 0
		if err := s.listBounded(ctx, prefix, func(string) bool {
			count++
			return true
		}); err != nil {
			return nil, fmt.Errorf("s3: list %s: %w", prefix, err)
		}
		out[partition] = count
	}
	return out, nil
}

func (s *Store) deleteUnder(ctx context.Context, prefix string) error {
	var keys []string
	if err := s.listBounded(ctx, prefix, func(key string) bool {
		keys = append(keys, key)
		return true
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := s.deleteBounded(ctx, key); err != nil {
			return fmt.Errorf("delete %s: %w", key, err)
		}
	}
	return nil
}

func (s *Store) listBounded(parent context.Context, prefix string, visit func(string) bool) error {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return parent.Err()
	}
	defer func() { <-s.sem }()
	timeout := s.timeout * 6
	if timeout < 60*time.Second {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return s.client.List(ctx, prefix, visit)
}

func (s *Store) deleteBounded(parent context.Context, key string) error {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return parent.Err()
	}
	defer func() { <-s.sem }()
	ctx, cancel := s.opContext(parent)
	defer cancel()
	return s.client.Delete(ctx, key)
}

func s3Partitions() []store.Partition {
	return []store.Partition{store.PartitionChunk, store.PartitionManifest, store.PartitionBlob}
}
