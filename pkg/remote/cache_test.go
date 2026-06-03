package remote

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

func hashOf(t *testing.T, b []byte) v1.Hash {
	t.Helper()
	h, _, err := v1.SHA256(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("sha256: %v", err)
	}
	return h
}

func TestCachePutGetHas(t *testing.T) {
	c, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello layer blob")
	h := hashOf(t, data)
	if c.Has(h) {
		t.Fatal("blob present before Put")
	}
	n, err := c.Put(h, io.NopCloser(bytes.NewReader(data)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("Put wrote %d bytes, want %d", n, len(data))
	}
	if !c.Has(h) {
		t.Fatal("blob absent after Put")
	}
	rc, err := c.Get(h)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}
}

// TestCacheDigestMismatch — Put must reject content that does not hash to the
// claimed digest, and leave no committed blob or leftover temp file.
func TestCacheDigestMismatch(t *testing.T) {
	c, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	wrong := hashOf(t, []byte("expected"))
	if _, err := c.Put(wrong, io.NopCloser(bytes.NewReader([]byte("actual")))); err == nil {
		t.Fatal("expected digest mismatch error, got nil")
	}
	if c.Has(wrong) {
		t.Fatal("mismatched blob must not be committed")
	}
	entries, _ := os.ReadDir(filepath.Join(c.dir, "blobs", "sha256"))
	if len(entries) != 0 {
		t.Fatalf("expected no leftover files, got %d", len(entries))
	}
}

// TestCacheEvictLRU — eviction drops least-recently-used blobs (by mtime)
// until under the target, keeping the newest.
func TestCacheEvictLRU(t *testing.T) {
	c, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	hashes := map[string]v1.Hash{}
	for _, name := range []string{"old", "mid", "new"} {
		b := bytes.Repeat([]byte(name[:1]), 100)
		h := hashOf(t, b)
		hashes[name] = h
		if _, err := c.Put(h, io.NopCloser(bytes.NewReader(b))); err != nil {
			t.Fatal(err)
		}
	}
	// Backdate past the grace period (oldest first) so all are eligible.
	base := time.Now().Add(-time.Hour)
	mustChtime(t, c.blobPath(hashes["old"]), base)
	mustChtime(t, c.blobPath(hashes["mid"]), base.Add(10*time.Minute))
	mustChtime(t, c.blobPath(hashes["new"]), base.Add(20*time.Minute))

	// 300 bytes total; target 150 → evict old then mid (200), keep new (100).
	freed, err := c.Evict(150)
	if err != nil {
		t.Fatal(err)
	}
	if freed != 200 {
		t.Fatalf("freed %d bytes, want 200", freed)
	}
	if c.Has(hashes["old"]) || c.Has(hashes["mid"]) {
		t.Fatal("LRU eviction should have removed old and mid")
	}
	if !c.Has(hashes["new"]) {
		t.Fatal("newest blob must survive")
	}
}

// TestCacheEvictGracePeriod — a just-written blob (mtime ~now) is protected
// from eviction even when over budget.
func TestCacheEvictGracePeriod(t *testing.T) {
	c, err := OpenCache(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	b := bytes.Repeat([]byte("x"), 100)
	h := hashOf(t, b)
	if _, err := c.Put(h, io.NopCloser(bytes.NewReader(b))); err != nil {
		t.Fatal(err)
	}
	freed, err := c.Evict(0) // try to evict everything
	if err != nil {
		t.Fatal(err)
	}
	if freed != 0 || !c.Has(h) {
		t.Fatalf("blob within grace period must survive (freed=%d, has=%v)", freed, c.Has(h))
	}
}

func mustChtime(t *testing.T, path string, mt time.Time) {
	t.Helper()
	if err := os.Chtimes(path, mt, mt); err != nil {
		t.Fatal(err)
	}
}
