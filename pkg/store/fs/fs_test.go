package fs

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// newTestStore initialises a fresh fs store at a temp dir with the
// given generation, then opens it. The test gets a ready-to-use
// Store and the temp dir is cleaned up by t.TempDir().
func newTestStore(t *testing.T, gen string) *Store {
	t.Helper()
	root := t.TempDir()
	if err := Init(Config{Root: root}, gen); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, err := New(Config{Root: root, VerifyKey: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func sum(b []byte) store.ContentKey {
	return store.ContentKey(sha256.Sum256(b))
}

// TestRoundtrip exercises the happy path: Put a blob, read it back,
// bytes should match.
func TestRoundtrip(t *testing.T) {
	s := newTestStore(t, "G1")
	ctx := context.Background()

	data := []byte("hello store")
	key := sum(data)

	isNew, err := s.Put(ctx, store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !isNew {
		t.Fatal("first Put should report isNew=true")
	}

	found, blob, err := s.Get(ctx, store.PartitionChunk, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("Get should find the just-Put key")
	}
	got := blob
	if string(got) != "hello store" {
		t.Fatalf("Get returned %q, want %q", got, "hello store")
	}
}

// TestDedupShortCircuit verifies that a second Put with the same
// key returns isNew=false and does NOT rewrite the temp/final file.
func TestDedupShortCircuit(t *testing.T) {
	s := newTestStore(t, "G1")
	ctx := context.Background()

	data := []byte("dedup me")
	key := sum(data)

	if _, err := s.Put(ctx, store.PartitionChunk, key, data); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	isNew, err := s.Put(ctx, store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if isNew {
		t.Fatal("second Put should report isNew=false (dedup hit)")
	}
}

// TestVerifyKeyMismatch asserts that a Put with a deliberately
// wrong key is rejected and no temp file is left behind.
func TestVerifyKeyMismatch(t *testing.T) {
	s := newTestStore(t, "G1")
	ctx := context.Background()

	data := []byte("real bytes")
	wrongKey := sum([]byte("lying about these"))

	_, err := s.Put(ctx, store.PartitionChunk, wrongKey, data)
	if err == nil {
		t.Fatal("Put with mismatched key should return an error")
	}

	// tmp dir should be clean — verify by listing.
	tmpRoot := filepath.Join(s.root, metaDir, tmpDir)
	entries, _ := os.ReadDir(tmpRoot)
	for _, e := range entries {
		if !e.IsDir() {
			t.Errorf("leftover temp file %s after verify-mismatch abort", e.Name())
		}
	}
}

// TestVerifyOff — VerifyKey=false lets a mismatched key through
// (dangerous but documented; useful for trusted-loader scenarios).
func TestVerifyOff(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, err := New(Config{Root: root, VerifyKey: false})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	data := []byte("real bytes")
	wrongKey := sum([]byte("but we tell the store this"))

	isNew, err := s.Put(ctx, store.PartitionChunk, wrongKey, data)
	if err != nil {
		t.Fatalf("Put with verify off: %v", err)
	}
	if !isNew {
		t.Fatal("Put with verify off should still write")
	}

	// Get using the wrong-but-committed key should return the data.
	found, blob, err := s.Get(ctx, store.PartitionChunk, wrongKey)
	if err != nil || !found {
		t.Fatal("Get on verify-off key should hit")
	}
	got := blob
	if string(got) != "real bytes" {
		t.Fatalf("Get returned %q, want %q", got, "real bytes")
	}
}

// TestGetMiss returns (false, nil, nil) for unknown keys.
func TestGetMiss(t *testing.T) {
	s := newTestStore(t, "G1")
	found, blob, err := s.Get(context.Background(), store.PartitionChunk, sum([]byte("nope")))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("Get on unknown key should miss")
	}
	if blob != nil {
		t.Fatal("Get miss should return nil blob")
	}
}

// TestNewUninitialised — opening a never-initialised root must
// return ErrUninitialised so the caller can suggest `store-ctl init`.
func TestNewUninitialised(t *testing.T) {
	root := t.TempDir()
	_, err := New(Config{Root: root})
	if !errors.Is(err, ErrUninitialised) {
		t.Fatalf("New on fresh root: got %v, want ErrUninitialised", err)
	}
}

// TestNewMissingRoot — root directory absent → still ErrUninitialised
// (with the missing-path detail in the wrapped message).
func TestNewMissingRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "does-not-exist")
	_, err := New(Config{Root: root})
	if !errors.Is(err, ErrUninitialised) {
		t.Fatalf("got %v, want ErrUninitialised", err)
	}
}

// TestInit_Then_New — Init writes the meta file with the given
// generation, then New picks it up as the single active generation.
func TestInit_Then_New(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(root, metaDir, generationsFile))
	if string(data) != "G1\n" {
		t.Errorf("generations file: got %q, want %q", data, "G1\n")
	}
	s, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := s.ActiveGeneration(); got != "G1" {
		t.Errorf("active: got %q, want G1", got)
	}
	if got := s.Generations(); len(got) != 1 || got[0] != "G1" {
		t.Errorf("gens: got %v, want [G1]", got)
	}
}

// TestInitTwiceRejects — Init refuses to clobber an existing meta
// file. The recovery path is Wipe (deliberate) + re-Init.
func TestInitTwiceRejects(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("first Init: %v", err)
	}
	err := Init(Config{Root: root}, "G2")
	if !errors.Is(err, ErrAlreadyInitialised) {
		t.Errorf("second Init: got %v, want ErrAlreadyInitialised", err)
	}
}

// TestInitRequiresGeneration — empty generation string is rejected
// (avoids creating a meta file containing only "\n" which would
// break New's empty-file check).
func TestInitRequiresGeneration(t *testing.T) {
	if err := Init(Config{Root: t.TempDir()}, ""); err == nil {
		t.Error("Init with empty generation should fail")
	}
}

// TestRollout_AppendsAndActivates — Rollout adds a new generation,
// makes it active, persists oldest-first to disk, and the in-memory
// cache reflects newest-first ordering.
func TestRollout_AppendsAndActivates(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := s.Rollout(context.Background(), "G2"); err != nil {
		t.Fatalf("Rollout G2: %v", err)
	}
	if got := s.ActiveGeneration(); got != "G2" {
		t.Errorf("active after Rollout: got %q, want G2", got)
	}
	gens := s.Generations()
	want := []string{"G2", "G1"}
	if len(gens) != 2 || gens[0] != want[0] || gens[1] != want[1] {
		t.Errorf("gens: got %v, want %v", gens, want)
	}
	data, _ := os.ReadFile(filepath.Join(root, metaDir, generationsFile))
	if string(data) != "G1\nG2\n" {
		t.Errorf("meta file: got %q, want %q", data, "G1\nG2\n")
	}
}

// TestRollout_RejectsDuplicate — re-Rollout the same generation
// returns ErrGenerationExists; meta file is left untouched.
func TestRollout_RejectsDuplicate(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, _ := New(Config{Root: root})
	err := s.Rollout(context.Background(), "G1")
	if !errors.Is(err, ErrGenerationExists) {
		t.Errorf("got %v, want ErrGenerationExists", err)
	}
}

// TestExistingMetaIsLoaded_NewestFirst — New on a hand-written meta
// file picks the last line as active and reverses for in-memory.
// Mirrors the on-disk convention: oldest-first persisted, newest-
// first in memory.
func TestExistingMetaIsLoaded_NewestFirst(t *testing.T) {
	root := t.TempDir()
	metaFile := filepath.Join(root, metaDir, generationsFile)
	_ = os.MkdirAll(filepath.Dir(metaFile), 0o755)
	_ = os.WriteFile(metaFile, []byte("G1\nG2\nG3\n"), 0o644)

	s, err := New(Config{Root: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := s.ActiveGeneration(); got != "G3" {
		t.Errorf("active: got %q, want G3", got)
	}
	gens := s.Generations()
	want := []string{"G3", "G2", "G1"}
	if len(gens) != len(want) {
		t.Fatalf("gens: got %v, want %v", gens, want)
	}
	for i := range want {
		if gens[i] != want[i] {
			t.Errorf("gens[%d] = %q, want %q", i, gens[i], want[i])
		}
	}
}

// TestGetReverseSearch — a key written under an older generation is
// still findable after Rollout to a younger active generation.
func TestGetReverseSearch(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Phase 1: open as G1, write a blob.
	s1, err := New(Config{Root: root, VerifyKey: true})
	if err != nil {
		t.Fatalf("New G1: %v", err)
	}
	data := []byte("old-gen payload")
	key := sum(data)
	if _, err := s1.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put G1: %v", err)
	}

	// Phase 2: rotate to G2 (in-place, no reopen needed).
	if err := s1.Rollout(context.Background(), "G2"); err != nil {
		t.Fatalf("Rollout G2: %v", err)
	}
	if got := s1.ActiveGeneration(); got != "G2" {
		t.Fatalf("active: got %q, want G2", got)
	}

	// Get the G1 blob from the G2 store — reverse search should hit.
	found, blob, err := s1.Get(context.Background(), store.PartitionChunk, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("reverse search should find G1 blob from G2 store")
	}
	if string(blob) != "old-gen payload" {
		t.Fatalf("got %q, want %q", blob, "old-gen payload")
	}
}

// TestDrop_RefusesActive — refuses to drop the currently active
// generation (would leave the store with no writable destination).
func TestDrop_RefusesActive(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, _ := New(Config{Root: root})
	err := s.Drop(context.Background(), "G1")
	if !errors.Is(err, ErrCannotDropActive) {
		t.Errorf("got %v, want ErrCannotDropActive", err)
	}
}

// TestDrop_RemovesGenAndData — Drop removes the gen from meta and
// deletes per-partition data trees underneath.
func TestDrop_RemovesGenAndData(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, _ := New(Config{Root: root, VerifyKey: true})

	// Write a blob into G1 and rotate to G2.
	data := []byte("g1 data")
	key := sum(data)
	if _, err := s.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Rollout(context.Background(), "G2"); err != nil {
		t.Fatalf("Rollout: %v", err)
	}

	// Drop G1.
	if err := s.Drop(context.Background(), "G1"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	gens := s.Generations()
	if len(gens) != 1 || gens[0] != "G2" {
		t.Errorf("gens after Drop: got %v, want [G2]", gens)
	}

	// Data tree for G1 must be gone.
	g1Dir := filepath.Join(root, "chunk", "G1")
	if _, err := os.Stat(g1Dir); !os.IsNotExist(err) {
		t.Errorf("G1 chunk dir should be gone, stat err: %v", err)
	}

	// Get for the now-orphaned key must miss (G1 no longer searched).
	found, _, _ := s.Get(context.Background(), store.PartitionChunk, key)
	if found {
		t.Error("Get should miss after dropping the source generation")
	}
}

// TestDrop_UnknownGeneration — Drop on a name not in the meta list
// returns ErrGenerationNotFound.
func TestDrop_UnknownGeneration(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, _ := New(Config{Root: root})
	err := s.Drop(context.Background(), "ZZZ")
	if !errors.Is(err, ErrGenerationNotFound) {
		t.Errorf("got %v, want ErrGenerationNotFound", err)
	}
}

// TestWipe_RemovesEverything — Wipe blows away the whole root,
// including the meta file. Subsequent New must report ErrUninitialised.
func TestWipe_RemovesEverything(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, _ := New(Config{Root: root, VerifyKey: true})

	data := []byte("to be wiped")
	key := sum(data)
	if _, err := s.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s.Wipe(context.Background()); err != nil {
		t.Fatalf("Wipe: %v", err)
	}

	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("root should be gone, stat err: %v", err)
	}
	if _, err := New(Config{Root: root}); !errors.Is(err, ErrUninitialised) {
		t.Errorf("New after Wipe: got %v, want ErrUninitialised", err)
	}
}

// TestGenerationStats — reports per-partition object counts after a
// few writes.
func TestGenerationStats(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, _ := New(Config{Root: root, VerifyKey: true})
	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		if _, err := s.Put(context.Background(), store.PartitionChunk, sum(data), data); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	stats, err := s.GenerationStats(context.Background(), "G1")
	if err != nil {
		t.Fatalf("GenerationStats: %v", err)
	}
	if stats[store.PartitionChunk] != 3 {
		t.Errorf("chunk count: got %d, want 3", stats[store.PartitionChunk])
	}
	if stats[store.PartitionManifest] != 0 {
		t.Errorf("manifest count: got %d, want 0", stats[store.PartitionManifest])
	}
}

// TestPutHandleAbort exercises the OpenPut/Abort path: opening a
// handle, writing some bytes, then Aborting should leave no temp
// file and the target path should stay absent.
func TestPutHandleAbort(t *testing.T) {
	s := newTestStore(t, "G1")

	h, err := s.OpenPut(store.PartitionChunk)
	if err != nil {
		t.Fatalf("OpenPut: %v", err)
	}
	if _, err := h.Write([]byte("partial")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := h.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	// tmp dir should be empty.
	tmpRoot := filepath.Join(s.root, metaDir, tmpDir)
	entries, _ := os.ReadDir(tmpRoot)
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("temp dir should be empty after Abort, got: %v", names)
	}
}

// TestOrphanCleanup — a leftover put-* temp file is removed when a
// new Store opens an already-initialised root.
func TestOrphanCleanup(t *testing.T) {
	root := t.TempDir()
	if err := Init(Config{Root: root}, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	tmpRoot := filepath.Join(root, metaDir, tmpDir)
	orphan := filepath.Join(tmpRoot, "put-stale-123")
	if err := os.WriteFile(orphan, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := New(Config{Root: root}); err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan temp file should have been cleaned up, stat err: %v", err)
	}
}
