package s3

import (
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// maxAliasRaces bounds the create/read retry loop (see fs.maxAliasRaces).
const maxAliasRaces = 4

// GetAlias returns the delta key the alias for hInd names in one generation.
// A missing alias is (zero, false, nil); a malformed one is an error.
func (s *Store) GetAlias(ctx context.Context, generation store.Generation, hInd store.ContentKey) (store.ContentKey, bool, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return store.ContentKey{}, false, fmt.Errorf("s3: %w", err)
	}
	key := s.objectKey(store.PartitionAlias, generation, hInd)
	body, _, err := s.boundedGet(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
		return s.client.Get(ctx, key)
	})
	if errors.Is(err, ErrNotFound) {
		return store.ContentKey{}, false, nil
	}
	if err != nil {
		return store.ContentKey{}, false, fmt.Errorf("s3: get alias generation=%s: %w", generation, err)
	}
	delta, err := store.DecodeAlias(body)
	if err != nil {
		return store.ContentKey{}, false, fmt.Errorf("s3: alias %s: %w", key, err)
	}
	return delta, true, nil
}

// Delete removes one object from one generation. A missing object is not an
// error (S3 DELETE is idempotent). It serves the server's discard of a delta
// object that lost the alias race; aliases themselves are write-once and are
// refused.
func (s *Store) Delete(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) error {
	if err := store.ValidateGeneration(generation); err != nil {
		return fmt.Errorf("s3: %w", err)
	}
	if partition == store.PartitionAlias {
		return fmt.Errorf("s3: aliases are write-once and cannot be deleted individually")
	}
	objKey := s.objectKey(partition, generation, key)
	if err := s.deleteBounded(ctx, objKey); err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("s3: delete %s: %w", objKey, err)
	}
	return nil
}

// CreateAlias makes hInd name delta unless an alias already exists, using a
// conditional PUT (If-None-Match: *) so the decision is the store's, not ours.
// It returns the key the alias names afterwards and whether this call created it.
func (s *Store) CreateAlias(ctx context.Context, generation store.Generation, hInd, delta store.ContentKey) (store.ContentKey, bool, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return store.ContentKey{}, false, fmt.Errorf("s3: %w", err)
	}
	key := s.objectKey(store.PartitionAlias, generation, hInd)
	for range maxAliasRaces {
		_, err := s.boundedPut(ctx, key, store.EncodeAlias(delta), PutOptions{IfNoneMatch: "*"})
		if err == nil {
			return delta, true, nil
		}
		if !errors.Is(err, ErrPreconditionFailed) {
			return store.ContentKey{}, false, fmt.Errorf("s3: create alias generation=%s: %w", generation, err)
		}
		current, ok, err := s.GetAlias(ctx, generation, hInd)
		if err != nil {
			return store.ContentKey{}, false, err
		}
		if ok {
			return current, false, nil
		}
		// The winner was deleted between our PUT and our GET; try again.
	}
	return store.ContentKey{}, false, fmt.Errorf("s3: alias %s kept appearing and disappearing", key)
}
