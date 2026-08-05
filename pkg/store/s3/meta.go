package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// loadGenerations reads <prefix>/__meta/generations from the bucket
// and populates the Store's in-memory cache. Read-only — Init is the
// only writer of fresh meta objects; Rollout / Drop CAS-rewrite an
// existing one.
//
// An absent or empty meta object is ErrUninitialised; admin commands
// surface this as a clear "run init" hint.
//
// Final state on success:
//
//	s.gens     newest-first list of generations (so Get scans new→old)
//	s.active   last entry of the on-disk list (matches fs.Store)
//	s.metaETag etag of the meta object (used for If-Match on rotation)
func (s *Store) loadGenerations(ctx context.Context) error {
	body, meta, err := s.bounded(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
		return s.client.Get(ctx, s.metaKey())
	})
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: missing %s", ErrUninitialised, s.metaKey())
	}
	if err != nil {
		return fmt.Errorf("read meta: %w", err)
	}
	gens := parseGenerations(body)
	if len(gens) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrUninitialised, s.metaKey())
	}
	s.setGenerations(gens, meta.ETag)
	return nil
}

// setGenerations updates the in-memory cache and computes
// newest-first ordering for Get.
func (s *Store) setGenerations(gens []string, etag string) {
	rev := make([]string, len(gens))
	for i, g := range gens {
		rev[len(gens)-1-i] = g
	}
	s.mu.Lock()
	s.gens = rev
	s.active = gens[len(gens)-1]
	s.metaETag = etag
	s.mu.Unlock()
}

// parseGenerations splits the meta body into a list of generation
// names, dropping blank lines and trailing whitespace.
func parseGenerations(body []byte) []string {
	var out []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

// renderGenerations is the inverse: produce a newline-terminated
// representation that round-trips through parseGenerations.
func renderGenerations(gens []string) []byte {
	var b strings.Builder
	for _, g := range gens {
		b.WriteString(g)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

func contains(haystack []string, needle string) bool {
	for _, x := range haystack {
		if x == needle {
			return true
		}
	}
	return false
}
