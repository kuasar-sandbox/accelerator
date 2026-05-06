package obs

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// ErrGenerationNotFound is returned by Drop when the named generation
// is not in the store's __meta/generations list.
var ErrGenerationNotFound = errors.New("obs: generation not found")

// ErrGenerationExists is returned by Rollout when the named
// generation is already in the list (would create a duplicate).
var ErrGenerationExists = errors.New("obs: generation already exists")

// ErrCannotDropActive is returned by Drop when the named generation
// is the currently-active one. Demoting active is rejected because
// it leaves the store with no writable generation; rotate to a new
// gen via Rollout first.
var ErrCannotDropActive = errors.New("obs: cannot drop the active generation")

// Rollout appends `gen` to the generations list and makes it active.
// `gen` must not already exist. The meta object is rewritten via
// CAS (If-Match: <etag>); a concurrent writer triggers a re-read +
// retry up to MetaCASRetries times.
func (s *Store) Rollout(ctx context.Context, gen string) error {
	if gen == "" {
		return errors.New("obs: generation is required")
	}
	for attempt := 0; attempt < s.metaTries; attempt++ {
		if err := s.refreshMeta(ctx); err != nil {
			return err
		}
		s.mu.RLock()
		current := append([]string(nil), s.gens...)
		etag := s.metaETag
		s.mu.RUnlock()

		if contains(current, gen) {
			return fmt.Errorf("%w: %q", ErrGenerationExists, gen)
		}

		// On-disk is oldest-first (reverse of in-memory).
		oldestFirst := make([]string, 0, len(current)+1)
		for i := len(current) - 1; i >= 0; i-- {
			oldestFirst = append(oldestFirst, current[i])
		}
		oldestFirst = append(oldestFirst, gen)

		newETag, err := s.boundedPut(ctx, s.metaKey(),
			renderGenerations(oldestFirst),
			PutOptions{IfMatch: etag, ContentType: "text/plain"})
		if errors.Is(err, ErrPreconditionFailed) {
			continue // raced; refresh and retry
		}
		if err != nil {
			return fmt.Errorf("obs: rollout meta: %w", err)
		}
		s.setGenerations(oldestFirst, newETag)
		return nil
	}
	return fmt.Errorf("obs: rollout exceeded %d CAS retries (concurrent writers?)", s.metaTries)
}

// Drop removes `gen` from the generations list and deletes every
// chunk + manifest object under that generation. Refuses to drop
// the active generation.
//
// Order of operations:
//
//  1. CAS-rewrite the meta object minus `gen` (so failures partway
//     through delete leave the list as the source of truth for
//     "live" generations and a re-run can finish the cleanup).
//  2. List + delete every key under <prefix>/{chunk,manifest}/<gen>/.
func (s *Store) Drop(ctx context.Context, gen string) error {
	if gen == "" {
		return errors.New("obs: generation is required")
	}
	for attempt := 0; attempt < s.metaTries; attempt++ {
		if err := s.refreshMeta(ctx); err != nil {
			return err
		}
		s.mu.RLock()
		current := append([]string(nil), s.gens...)
		active := s.active
		etag := s.metaETag
		s.mu.RUnlock()

		if gen == active {
			return fmt.Errorf("%w: %q", ErrCannotDropActive, gen)
		}
		idx := -1
		for i, g := range current {
			if g == gen {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("%w: %q (known: %v)", ErrGenerationNotFound, gen, current)
		}

		remaining := append(append([]string(nil), current[:idx]...), current[idx+1:]...)
		oldestFirst := make([]string, len(remaining))
		for i := range remaining {
			oldestFirst[i] = remaining[len(remaining)-1-i]
		}
		newETag, err := s.boundedPut(ctx, s.metaKey(),
			renderGenerations(oldestFirst),
			PutOptions{IfMatch: etag, ContentType: "text/plain"})
		if errors.Is(err, ErrPreconditionFailed) {
			continue
		}
		if err != nil {
			return fmt.Errorf("obs: drop meta: %w", err)
		}
		s.setGenerations(oldestFirst, newETag)

		// Delete data trees. Failures here are recoverable by re-Drop
		// (the meta is already updated, so the gen is gone from the
		// "live" list and a second Drop call would short-circuit on
		// ErrGenerationNotFound — but the data sweep is idempotent
		// so a manual purge can clean up).
		for _, p := range []store.Partition{store.PartitionChunk, store.PartitionManifest} {
			pref := path.Join(s.prefix, string(p), gen) + "/"
			if err := s.deleteUnder(ctx, pref); err != nil {
				return fmt.Errorf("obs: delete under %s: %w", pref, err)
			}
		}
		return nil
	}
	return fmt.Errorf("obs: drop exceeded %d CAS retries (concurrent writers?)", s.metaTries)
}

// Wipe deletes every object under the store's prefix, including the
// meta object. The Store stays usable for a subsequent Init; until
// then any Get/Put/Exists call will produce ErrUninitialised-style
// errors at the s3 layer (NoSuchKey).
func (s *Store) Wipe(ctx context.Context) error {
	pref := s.prefix
	if pref != "" {
		pref += "/"
	}
	if err := s.deleteUnder(ctx, pref); err != nil {
		return fmt.Errorf("obs: wipe: %w", err)
	}
	s.mu.Lock()
	s.gens = nil
	s.active = ""
	s.metaETag = ""
	s.mu.Unlock()
	return nil
}

// GenerationStats returns the number of objects per partition under
// the given generation. Walks the in-bucket tree once via List.
func (s *Store) GenerationStats(ctx context.Context, gen string) (map[store.Partition]int, error) {
	out := make(map[store.Partition]int)
	for _, p := range []store.Partition{store.PartitionChunk, store.PartitionManifest} {
		pref := path.Join(s.prefix, string(p), gen) + "/"
		count := 0
		if err := s.listBounded(ctx, pref, func(_ string) bool {
			count++
			return true
		}); err != nil {
			return nil, fmt.Errorf("obs: list %s: %w", pref, err)
		}
		out[p] = count
	}
	return out, nil
}

// refreshMeta re-reads the meta object so subsequent CAS attempts
// see the latest etag. Used by Rollout / Drop on each attempt.
func (s *Store) refreshMeta(ctx context.Context) error {
	body, meta, err := s.bounded(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
		return s.client.Get(ctx, s.metaKey())
	})
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: missing %s", ErrUninitialised, s.metaKey())
	}
	if err != nil {
		return fmt.Errorf("obs: read meta: %w", err)
	}
	gens := parseGenerations(body)
	if len(gens) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrUninitialised, s.metaKey())
	}
	s.setGenerations(gens, meta.ETag)
	return nil
}

// deleteUnder lists every key under `prefix` and deletes them. The
// caller picks the prefix scope; this helper does NOT skip the meta
// object — Drop and Wipe both want the cleanup, and the meta key is
// covered (Drop targets per-generation subtrees that don't include
// the meta path; Wipe deliberately includes it).
func (s *Store) deleteUnder(ctx context.Context, prefix string) error {
	var collected []string
	if err := s.listBounded(ctx, prefix, func(k string) bool {
		collected = append(collected, k)
		return true
	}); err != nil {
		return err
	}
	for _, k := range collected {
		if err := s.deleteBounded(ctx, k); err != nil {
			return fmt.Errorf("delete %s: %w", k, err)
		}
	}
	return nil
}

// listBounded wraps s3Client.List with the semaphore + timeout.
// Pagination happens inside List itself; we budget a single context
// deadline generous enough to span all pages of a typical sweep
// (tens of thousands of keys at most).
func (s *Store) listBounded(parent context.Context, prefix string, visit func(string) bool) error {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return parent.Err()
	}
	defer func() { <-s.sem }()
	timeout := s.opTimeout * 6
	if timeout < 60*time.Second {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return s.client.List(ctx, prefix, visit)
}

// deleteBounded wraps s3Client.Delete with the semaphore + timeout.
func (s *Store) deleteBounded(parent context.Context, key string) error {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return parent.Err()
	}
	defer func() { <-s.sem }()
	ctx, cancel := context.WithTimeout(parent, s.opTimeout)
	defer cancel()
	return s.client.Delete(ctx, key)
}
