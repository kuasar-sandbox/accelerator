package obs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// newStoreT creates a fresh fake bucket, runs Init with `gen`, then
// opens a Store. Mirrors fs.newTestStore — Init+New as the only path
// to a usable Store.
func newStoreT(t *testing.T, fake *fakeS3, gen string) *Store {
	t.Helper()
	cfg := Config{
		Bucket:    "test-bucket",
		Prefix:    "test/",
		VerifyKey: true,
	}
	if err := Init(context.Background(), fake, cfg, gen); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, err := New(context.Background(), fake, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func keyFromBytes(data []byte) store.ContentKey {
	return store.ContentKey(sha256.Sum256(data))
}

// TestNew_Uninitialised — empty bucket → ErrUninitialised.
func TestNew_Uninitialised(t *testing.T) {
	fake := newFakeS3()
	_, err := New(context.Background(), fake, Config{Bucket: "test-bucket", Prefix: "test/"})
	if !errors.Is(err, ErrUninitialised) {
		t.Fatalf("New on empty bucket: got %v, want ErrUninitialised", err)
	}
}

// TestInit_Then_New — Init writes meta with the given gen, New picks
// it up as the single active generation.
func TestInit_Then_New(t *testing.T) {
	fake := newFakeS3()
	cfg := Config{Bucket: "test-bucket", Prefix: "test/"}
	if err := Init(context.Background(), fake, cfg, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, err := New(context.Background(), fake, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := s.ActiveGeneration(); got != "G1" {
		t.Errorf("active=%q want G1", got)
	}
	if got := s.Generations(); len(got) != 1 || got[0] != "G1" {
		t.Errorf("gens=%v want [G1]", got)
	}
	body, _, _ := fake.Get(context.Background(), "test/__meta/generations")
	if string(body) != "G1\n" {
		t.Errorf("meta body=%q want %q", body, "G1\n")
	}
}

// TestInitTwiceRejects — Init refuses to overwrite an existing meta.
func TestInitTwiceRejects(t *testing.T) {
	fake := newFakeS3()
	cfg := Config{Bucket: "test-bucket", Prefix: "test/"}
	if err := Init(context.Background(), fake, cfg, "G1"); err != nil {
		t.Fatalf("first Init: %v", err)
	}
	err := Init(context.Background(), fake, cfg, "G2")
	if !errors.Is(err, ErrAlreadyInitialised) {
		t.Errorf("got %v, want ErrAlreadyInitialised", err)
	}
}

// TestInitRequiresGeneration — empty generation rejected.
func TestInitRequiresGeneration(t *testing.T) {
	fake := newFakeS3()
	err := Init(context.Background(), fake, Config{Bucket: "test-bucket"}, "")
	if err == nil {
		t.Error("Init with empty generation should fail")
	}
}

// TestRollout_AppendsAndActivates — Rollout adds a generation,
// makes it active, persists oldest-first to the bucket, in-memory
// stays newest-first.
func TestRollout_AppendsAndActivates(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")

	if err := s.Rollout(context.Background(), "G2"); err != nil {
		t.Fatalf("Rollout: %v", err)
	}
	if got := s.ActiveGeneration(); got != "G2" {
		t.Errorf("active=%q want G2", got)
	}
	if got := s.Generations(); len(got) != 2 || got[0] != "G2" || got[1] != "G1" {
		t.Errorf("gens=%v want [G2 G1]", got)
	}
	body, _, _ := fake.Get(context.Background(), "test/__meta/generations")
	if string(body) != "G1\nG2\n" {
		t.Errorf("meta body=%q want %q", body, "G1\nG2\n")
	}
}

// TestRollout_RejectsDuplicate — re-Rollout the same gen fails.
func TestRollout_RejectsDuplicate(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	err := s.Rollout(context.Background(), "G1")
	if !errors.Is(err, ErrGenerationExists) {
		t.Errorf("got %v, want ErrGenerationExists", err)
	}
}

// TestPut_GetRoundTrip — basic happy path.
func TestPut_GetRoundTrip(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	data := []byte("hello world")
	key := keyFromBytes(data)

	isNew, err := s.Put(context.Background(), store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !isNew {
		t.Errorf("Put isNew=false on first write")
	}

	hit, body, err := s.Get(context.Background(), store.PartitionChunk, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Errorf("Get hit=false")
	}
	if !bytes.Equal(body, data) {
		t.Errorf("Get body mismatch: got %q want %q", body, data)
	}
}

// TestPut_DedupShortCircuit — second Put on same key reports
// isNew=false without re-uploading.
func TestPut_DedupShortCircuit(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	data := []byte("once and only once")
	key := keyFromBytes(data)

	if _, err := s.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put #1: %v", err)
	}
	putCountBefore := fake.hits.put.Load()
	isNew, err := s.Put(context.Background(), store.PartitionChunk, key, data)
	if err != nil {
		t.Fatalf("Put #2: %v", err)
	}
	if isNew {
		t.Errorf("Put #2 isNew=true; expected dedup hit")
	}
	if put := fake.hits.put.Load() - putCountBefore; put != 0 {
		t.Errorf("Put #2 issued %d uploads; expected 0 (dedup)", put)
	}
}

// TestPut_VerifyKeyMismatch — provided key disagrees with hashed
// content; Put rejects with ErrKeyMismatch and nothing uploaded.
func TestPut_VerifyKeyMismatch(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	data := []byte("legitimate")
	wrongKey := keyFromBytes([]byte("different"))

	putCountBefore := fake.hits.put.Load()
	_, err := s.Put(context.Background(), store.PartitionChunk, wrongKey, data)
	if !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("err=%v want ErrKeyMismatch", err)
	}
	if put := fake.hits.put.Load() - putCountBefore; put != 0 {
		t.Errorf("uploaded on mismatch: %d puts", put)
	}
}

// TestGet_MultiGenWalk — value lives in older generation; Get
// finds it after the active-gen miss.
func TestGet_MultiGenWalk(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	data := []byte("from G1")
	key := keyFromBytes(data)
	if _, err := s.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put under G1: %v", err)
	}

	if err := s.Rollout(context.Background(), "G2"); err != nil {
		t.Fatalf("Rollout: %v", err)
	}
	if got := s.Generations(); len(got) != 2 || got[0] != "G2" || got[1] != "G1" {
		t.Fatalf("gens=%v want [G2 G1]", got)
	}
	hit, body, err := s.Get(context.Background(), store.PartitionChunk, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit || !bytes.Equal(body, data) {
		t.Fatalf("Get hit=%v body=%q (want hit=true, body=%q)", hit, body, data)
	}
}

// TestExists_ActiveOnly — Exists checks active generation only,
// not historical generations.
func TestExists_ActiveOnly(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	data := []byte("only in G1")
	key := keyFromBytes(data)
	if _, err := s.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s.Rollout(context.Background(), "G2"); err != nil {
		t.Fatalf("Rollout: %v", err)
	}
	if s.Exists(store.PartitionChunk, key) {
		t.Errorf("Exists=true for object only in older gen; expected false")
	}
	hit, _, _ := s.Get(context.Background(), store.PartitionChunk, key)
	if !hit {
		t.Errorf("Get hit=false; expected to find in older gen")
	}
}

// TestObjectKeyLayout — verify the in-bucket key layout matches
// fs.Store's filesystem path layout.
func TestObjectKeyLayout(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	var key store.ContentKey
	for i := range key {
		key[i] = byte(i)
	}
	got := s.objectKey(store.PartitionChunk, "G1", key)
	want := "test/chunk/G1/00/01/000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	if got != want {
		t.Errorf("objectKey=%q want %q", got, want)
	}
	// Path layout intentionally mirrors filepath.Join semantics for
	// fs/obs symmetry: posix-style slashes either way.
	_ = filepath.Join
}

// TestParseGenerations — robustness around blank lines / trailing
// newline / whitespace.
func TestParseGenerations(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"G1\n", []string{"G1"}},
		{"G1\nG2\nG3\n", []string{"G1", "G2", "G3"}},
		{"G1\n\nG2\n", []string{"G1", "G2"}},
		{"  G1  \n", []string{"G1"}},
	}
	for _, tc := range cases {
		got := parseGenerations([]byte(tc.in))
		if !equalSlice(got, tc.want) {
			t.Errorf("parse(%q) = %v want %v", tc.in, got, tc.want)
		}
	}
}

// TestRenderGenerations_RoundTrip — render then parse yields the
// original list.
func TestRenderGenerations_RoundTrip(t *testing.T) {
	in := []string{"G1", "G2", "G3"}
	out := parseGenerations(renderGenerations(in))
	if !equalSlice(in, out) {
		t.Errorf("round-trip: %v → %v", in, out)
	}
	if !strings.HasSuffix(string(renderGenerations(in)), "\n") {
		t.Errorf("renderGenerations should terminate with newline")
	}
}

// TestDrop_RefusesActive — refuses to drop the currently active
// generation.
func TestDrop_RefusesActive(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	err := s.Drop(context.Background(), "G1")
	if !errors.Is(err, ErrCannotDropActive) {
		t.Errorf("got %v, want ErrCannotDropActive", err)
	}
}

// TestDrop_RemovesGenAndData — Drop removes the gen from meta and
// deletes per-partition data trees underneath.
func TestDrop_RemovesGenAndData(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")

	data := []byte("g1 data")
	key := keyFromBytes(data)
	if _, err := s.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Rollout(context.Background(), "G2"); err != nil {
		t.Fatalf("Rollout: %v", err)
	}
	if err := s.Drop(context.Background(), "G1"); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	gens := s.Generations()
	if len(gens) != 1 || gens[0] != "G2" {
		t.Errorf("gens after Drop: got %v, want [G2]", gens)
	}

	// G1 chunk objects must be gone.
	g1Prefix := "test/chunk/G1/"
	var found []string
	_ = fake.List(context.Background(), g1Prefix, func(k string) bool {
		found = append(found, k)
		return true
	})
	if len(found) != 0 {
		t.Errorf("G1 chunk objects survived: %v", found)
	}

	// Get for the now-orphaned key must miss.
	hit, _, _ := s.Get(context.Background(), store.PartitionChunk, key)
	if hit {
		t.Error("Get should miss after dropping the source generation")
	}
}

// TestDrop_UnknownGeneration — Drop on a name not in the meta list
// returns ErrGenerationNotFound.
func TestDrop_UnknownGeneration(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	err := s.Drop(context.Background(), "ZZZ")
	if !errors.Is(err, ErrGenerationNotFound) {
		t.Errorf("got %v, want ErrGenerationNotFound", err)
	}
}

// TestWipe_RemovesEverything — Wipe blows away every key under the
// store's prefix; subsequent New must report ErrUninitialised.
func TestWipe_RemovesEverything(t *testing.T) {
	fake := newFakeS3()
	cfg := Config{Bucket: "test-bucket", Prefix: "test/", VerifyKey: true}
	if err := Init(context.Background(), fake, cfg, "G1"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	s, err := New(context.Background(), fake, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	data := []byte("to be wiped")
	key := keyFromBytes(data)
	if _, err := s.Put(context.Background(), store.PartitionChunk, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s.Wipe(context.Background()); err != nil {
		t.Fatalf("Wipe: %v", err)
	}

	// Bucket must be empty under the test/ prefix.
	var found []string
	_ = fake.List(context.Background(), "test/", func(k string) bool {
		found = append(found, k)
		return true
	})
	if len(found) != 0 {
		t.Errorf("keys survived Wipe: %v", found)
	}
	if _, err := New(context.Background(), fake, cfg); !errors.Is(err, ErrUninitialised) {
		t.Errorf("New after Wipe: got %v, want ErrUninitialised", err)
	}
}

// TestGenerationStats — reports per-partition object counts after a
// few writes.
func TestGenerationStats(t *testing.T) {
	fake := newFakeS3()
	s := newStoreT(t, fake, "G1")
	for i := 0; i < 3; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		if _, err := s.Put(context.Background(), store.PartitionChunk, keyFromBytes(data), data); err != nil {
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

func equalSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
