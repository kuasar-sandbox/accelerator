// Package fs implements a filesystem-backed store.ContentStore.
//
// The backend is the sole writer to its root directory (by convention,
// not by filesystem locks); it is wrapped by the store-ctl gRPC server
// and never exposed directly to cache-ctl or manifest-ctl processes.
//
// Layout:
//
//	{root}/__meta/generations              -- newline-separated generation list, newest last
//	{root}/__meta/tmp/                     -- temp files for atomic Put
//	{root}/chunk/{gen}/{hex[:2]}/{hex[2:4]}/{hex}
//	{root}/manifest/{gen}/{hex[:2]}/{hex[2:4]}/{hex}
//
// The ContentKey layout is `hex = lowercase-hex(SHA256(data))`.
// Clients compute the SHA256 themselves and pass it to Put; the
// backend optionally re-hashes to verify (controlled by verifyKey).
package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// ErrKeyMismatch is returned by Put / PutHandle.Commit when the
// server-computed SHA256 digest does not match the client-supplied
// ContentKey. The on-disk state is cleaned up before the error is
// returned.
var ErrKeyMismatch = errors.New("fs: content key mismatch")

// ErrUninitialised is returned by New when the store root has no
// __meta/generations file. Recovery: run `store-ctl init`.
var ErrUninitialised = errors.New("fs: store uninitialised (run `store-ctl init`)")

// ErrAlreadyInitialised is returned by Init when the target root
// already carries an __meta/generations file. Init never overwrites
// existing meta — caller must Wipe first.
var ErrAlreadyInitialised = errors.New("fs: store already initialised")

const (
	metaDir         = "__meta"
	generationsFile = "generations"
	tmpDir          = "tmp"
)

// Store is a content-addressed filesystem backend.
//
// The store is stateful only in the in-memory sense: `active` and
// `gens` are loaded from `__meta/generations` at construction time
// and never mutated afterwards (restart to change). All Put writes
// go to `active`; all Get reads scan `gens` in newest-first order.
type Store struct {
	root      string
	active    string
	gens      []string // newest first
	verifyKey bool

	// mu protects nothing at runtime — all fields are frozen after New
	// returns — but we keep it for future extensions (e.g. generation
	// rotation via SIGHUP).
	mu sync.RWMutex
}

// Config controls Store construction. The schema is intentionally
// minimal — generation lifecycle is owned by store-ctl admin
// commands (init / rollout / purge), not by serve config.
type Config struct {
	// Root is the filesystem root. Required; must already be
	// initialised (see Init).
	Root string

	// VerifyKey controls whether Put re-hashes the incoming data and
	// compares against the caller-supplied key. Default true (pass a
	// pointer-to-false via explicit config to disable).
	VerifyKey bool
}

// New opens an existing fs store rooted at cfg.Root. The store must
// already carry a valid `__meta/generations` file — call Init first
// for a fresh root. Returns ErrUninitialised when the meta file is
// absent so callers can give a clear remediation hint.
func New(cfg Config) (*Store, error) {
	if cfg.Root == "" {
		return nil, errors.New("fs: root is required")
	}
	if _, err := os.Stat(cfg.Root); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: root %s does not exist", ErrUninitialised, cfg.Root)
		}
		return nil, fmt.Errorf("fs: stat root %s: %w", cfg.Root, err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.Root, metaDir, tmpDir), 0o755); err != nil {
		return nil, fmt.Errorf("fs: create temp dir: %w", err)
	}
	cleanOrphanTemps(cfg.Root)

	gens, active, err := loadGenerations(cfg.Root)
	if err != nil {
		return nil, err
	}
	return &Store{
		root:      cfg.Root,
		active:    active,
		gens:      gens,
		verifyKey: cfg.VerifyKey,
	}, nil
}

// Init creates a fresh fs store at cfg.Root with `generation` as
// the single (and active) generation. Refuses to clobber an
// existing meta file — caller must Wipe first if intentional.
func Init(cfg Config, generation string) error {
	if cfg.Root == "" {
		return errors.New("fs: root is required")
	}
	if generation == "" {
		return errors.New("fs: generation is required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.Root, metaDir, tmpDir), 0o755); err != nil {
		return fmt.Errorf("fs: create meta dir: %w", err)
	}
	metaPath := filepath.Join(cfg.Root, metaDir, generationsFile)
	if _, err := os.Stat(metaPath); err == nil {
		return fmt.Errorf("%w: %s", ErrAlreadyInitialised, cfg.Root)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("fs: stat meta: %w", err)
	}
	return writeGenerationsFile(metaPath, []string{generation})
}

// ActiveGeneration returns the generation this store writes to.
func (s *Store) ActiveGeneration() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// Generations returns the known generations in newest-first order.
// Returned slice is a copy — safe for the caller to mutate.
func (s *Store) Generations() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.gens))
	copy(out, s.gens)
	return out
}

// Get searches all known generations (newest-first) for the given
// key under the partition. Returns (true, data, nil) on hit,
// (false, nil, nil) on miss, (false, nil, err) on a filesystem error
// that isn't IsNotExist.
func (s *Store) Get(_ context.Context, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	s.mu.RLock()
	gens := s.gens
	s.mu.RUnlock()

	for _, gen := range gens {
		p := s.objectPath(partition, gen, key)
		data, err := os.ReadFile(p)
		if err == nil {
			return true, data, nil
		}
		if !os.IsNotExist(err) {
			return false, nil, fmt.Errorf("fs: read %s: %w", p, err)
		}
	}
	return false, nil, nil
}

// Put writes data under the active generation. The caller-supplied
// key is treated as authoritative for path resolution; if verifyKey
// is enabled, the store re-hashes `data` and rejects with
// ErrKeyMismatch on disagreement (deleting any temp file).
//
// Returns (true, nil) when a new object was written, (false, nil)
// when the key already existed (dedup hit).
func (s *Store) Put(_ context.Context, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	if s.Exists(partition, key) {
		return false, nil
	}
	h, err := s.OpenPut(partition)
	if err != nil {
		return false, err
	}
	if _, err := h.Write(data); err != nil {
		_ = h.Abort()
		return false, err
	}
	var verifyDigest store.ContentKey
	if s.verifyKey {
		verifyDigest = store.ContentKey(sha256.Sum256(data))
	} else {
		verifyDigest = key
	}
	return h.Commit(key, verifyDigest)
}

// Exists is a lightweight check against the current active generation
// only (NOT a reverse-generation search). It lets the gRPC server
// short-circuit Put streams on dedup hits before allocating a temp
// file.
func (s *Store) Exists(partition store.Partition, key store.ContentKey) bool {
	s.mu.RLock()
	active := s.active
	s.mu.RUnlock()
	p := s.objectPath(partition, active, key)
	_, err := os.Stat(p)
	return err == nil
}

// PutHandle is a streaming-Put session returned by OpenPut. Callers
// write bytes via Write, then call Commit with the expected key (and
// the verification digest they want the commit to check against).
// If the session is abandoned, Abort must be called to clean up the
// temp file.
type PutHandle struct {
	store     *Store
	partition store.Partition
	tmp       *os.File
	tmpName   string
	committed bool
}

// OpenPut starts a new streaming Put session. The returned handle
// owns a freshly-created temp file in the target partition directory;
// data written to the handle lands on disk directly (no in-memory
// staging). Commit atomically renames the temp into place; Abort
// deletes it.
func (s *Store) OpenPut(partition store.Partition) (store.PutHandle, error) {
	// Temp files live in __meta/tmp — a single shared directory so
	// the atomic rename target path (under the partition/generation
	// tree) only needs to be created at commit time, and cleanup on
	// startup is localised.
	dir := filepath.Join(s.root, metaDir, tmpDir)
	tmp, err := os.CreateTemp(dir, "put-*")
	if err != nil {
		return nil, fmt.Errorf("fs: create temp: %w", err)
	}
	return &PutHandle{
		store:     s,
		partition: partition,
		tmp:       tmp,
		tmpName:   tmp.Name(),
	}, nil
}

// Write forwards bytes into the temp file. Returns the number of
// bytes written and any filesystem error.
func (h *PutHandle) Write(p []byte) (int, error) {
	return h.tmp.Write(p)
}

// Commit closes the temp file, optionally verifies the digest, and
// renames the temp into its final content-addressed path. On digest
// mismatch or filesystem failure, Commit cleans up the temp and
// returns an error.
//
// Contract:
//   - key is the ContentKey the caller claims for the written data
//     (used to derive the final path).
//   - verifyDigest is the caller-side re-hash used as the server-
//     side check. Pass `key` itself to skip verification, or the
//     actual sha256 over the streamed bytes to enforce it.
func (h *PutHandle) Commit(key store.ContentKey, verifyDigest store.ContentKey) (bool, error) {
	if h.committed {
		return false, errors.New("fs: handle already committed")
	}
	if err := h.tmp.Close(); err != nil {
		_ = os.Remove(h.tmpName)
		return false, fmt.Errorf("fs: close temp: %w", err)
	}
	h.committed = true

	if verifyDigest != key {
		_ = os.Remove(h.tmpName)
		return false, ErrKeyMismatch
	}

	h.store.mu.RLock()
	active := h.store.active
	h.store.mu.RUnlock()
	finalPath := h.store.objectPath(h.partition, active, key)

	// Last-chance dedup check: another writer may have won the race
	// while we were streaming. If so, drop our temp and return
	// (false, nil).
	if _, err := os.Stat(finalPath); err == nil {
		_ = os.Remove(h.tmpName)
		return false, nil
	}

	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		_ = os.Remove(h.tmpName)
		return false, fmt.Errorf("fs: mkdir: %w", err)
	}
	if err := os.Rename(h.tmpName, finalPath); err != nil {
		_ = os.Remove(h.tmpName)
		return false, fmt.Errorf("fs: rename: %w", err)
	}
	return true, nil
}

// Abort closes the temp file and deletes it. Safe to call after
// Commit (no-op when already committed).
func (h *PutHandle) Abort() error {
	if h.committed {
		return nil
	}
	h.committed = true
	_ = h.tmp.Close()
	return os.Remove(h.tmpName)
}

// objectPath returns the absolute file path for a key under a
// partition and generation.
func (s *Store) objectPath(partition store.Partition, generation string, key store.ContentKey) string {
	h := hex.EncodeToString(key[:])
	return filepath.Join(s.root, string(partition), generation, h[:2], h[2:4], h)
}

// ---------------------------------------------------------------------------
// generations file management
// ---------------------------------------------------------------------------

// loadGenerations reads and validates `__meta/generations`. The store
// must already be initialised — Init is the only writer of fresh
// meta files. An absent or empty file is ErrUninitialised; admin
// commands surface this as a clear "run init" hint.
//
// Returns the list newest-first (fs file is oldest-first; reverse
// happens here so the in-memory representation matches obs.Store).
func loadGenerations(root string) (gens []string, active string, err error) {
	file := filepath.Join(root, metaDir, generationsFile)
	data, readErr := os.ReadFile(file)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return nil, "", fmt.Errorf("%w: missing %s", ErrUninitialised, file)
		}
		return nil, "", fmt.Errorf("fs: read generations: %w", readErr)
	}
	lines := parseGenerations(data)
	if len(lines) == 0 {
		return nil, "", fmt.Errorf("%w: %s is empty", ErrUninitialised, file)
	}
	newestFirst := make([]string, len(lines))
	for i, v := range lines {
		newestFirst[len(lines)-1-i] = v
	}
	return newestFirst, lines[len(lines)-1], nil
}

// parseGenerations splits a newline-separated generation file into
// trimmed non-empty entries. Duplicates are preserved in the order
// they appear — the caller handles deduplication if needed (we don't
// today because the file is written atomically by this process and
// duplicates should never arise).
func parseGenerations(data []byte) []string {
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

// writeGenerationsFile atomically writes the generation list to the
// target file via temp + rename.
func writeGenerationsFile(target string, lines []string) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("fs: create meta dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "generations.*.tmp")
	if err != nil {
		return fmt.Errorf("fs: create generations temp: %w", err)
	}
	tmpName := tmp.Name()
	content := strings.Join(lines, "\n") + "\n"
	if _, err := io.WriteString(tmp, content); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("fs: write generations temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("fs: close generations temp: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("fs: rename generations: %w", err)
	}
	return nil
}

// cleanOrphanTemps removes stale temp files from a prior aborted
// Put session. Called from New before the store starts accepting
// traffic, so there are no concurrent writers.
func cleanOrphanTemps(root string) {
	dir := filepath.Join(root, metaDir, tmpDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "put-") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}
