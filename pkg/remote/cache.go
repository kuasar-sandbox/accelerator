package remote

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
)

// Cache is a content-addressed blob cache laid out as a standard OCI image
// layout (oci-layout + index.json + blobs/<algo>/<hex>), so the same dir is
// inspectable with crane/skopeo and shareable across flatten-ctl processes.
//
// The hot path never consults the mutable index.json: a cache hit is simply
// the existence of blobs/<algo>/<hex>. Blobs are written atomically
// (temp + digest-verify + rename), which is what makes concurrent writers and
// crash recovery safe — ggcr's own layout.WriteBlob writes the final path
// directly (not atomic) and is deliberately not used here. index.json is
// maintained best-effort for tooling; cache correctness never depends on it.
// Size-cap LRU eviction lives in gc.go and keys off blob mtime, refreshed by
// Get so a recently-read blob survives the sweep.
type Cache struct {
	dir     string
	maxSize int64 // bytes; 0 = unlimited
	// ephemeral is set only by Config.OpenCache for a private per-run cache.
	ephemeral bool
}

// OpenCache opens (or initialises) an OCI-layout cache rooted at dir. The
// layout package is used only to lay down a valid oci-layout + index.json so
// the dir is inspectable with crane/skopeo; blobs are then read/written
// directly (content-addressed, atomic) rather than through layout.Path, whose
// blob writes are non-atomic and whose index.json mutation is not
// concurrency-safe.
func OpenCache(dir string, maxSize int64) (*Cache, error) {
	if dir == "" {
		return nil, fmt.Errorf("remote: cache dir is empty")
	}
	if _, err := layout.FromPath(dir); err != nil {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("remote: mkdir cache %q: %w", dir, err)
		}
		if _, err := layout.Write(dir, empty.Index); err != nil {
			return nil, fmt.Errorf("remote: init cache layout %q: %w", dir, err)
		}
	}
	return &Cache{dir: dir, maxSize: maxSize}, nil
}

func (c *Cache) blobPath(h v1.Hash) string {
	return filepath.Join(c.dir, "blobs", h.Algorithm, h.Hex)
}

// Has reports whether the blob is cached — the hot-path cache-hit test.
// Content-addressed and index-free, hence inherently multi-process safe.
func (c *Cache) Has(h v1.Hash) bool {
	st, err := os.Stat(c.blobPath(h))
	return err == nil && !st.IsDir()
}

// Get opens a cached blob and refreshes its mtime so the LRU sweep treats it
// as recently used.
func (c *Cache) Get(h v1.Hash) (io.ReadCloser, error) {
	p := c.blobPath(h)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	touch(p)
	return f, nil
}

// Put writes rc into the cache atomically, verifying the content hashes to h.
// Two writers of the same content are harmless: each writes a distinct temp
// file and the final rename is atomic (identical bytes either way).
func (c *Cache) Put(h v1.Hash, rc io.ReadCloser) (int64, error) {
	defer rc.Close()
	dir := filepath.Join(c.dir, "blobs", h.Algorithm)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-"+h.Hex+"-*")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename

	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hasher), rc)
	if err != nil {
		tmp.Close()
		return 0, fmt.Errorf("remote: cache write %s: %w", h, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return 0, err
	}
	if err := tmp.Close(); err != nil {
		return 0, err
	}
	if h.Algorithm == "sha256" {
		if got := hex.EncodeToString(hasher.Sum(nil)); got != h.Hex {
			return 0, fmt.Errorf("remote: cache digest mismatch for %s: got sha256:%s", h, got)
		}
	}
	if err := os.Rename(tmpName, c.blobPath(h)); err != nil {
		return 0, fmt.Errorf("remote: cache commit %s: %w", h, err)
	}
	return n, nil
}

// touch refreshes a blob's access/modify time to now (best-effort), feeding
// the LRU eviction in gc.go.
func touch(path string) {
	now := time.Now()
	_ = os.Chtimes(path, now, now)
}
