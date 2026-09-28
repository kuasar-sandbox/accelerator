package remote

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/kuasar-sandbox/accelerator/pkg/flatten"
)

func TestBuildReleasesOnlyEphemeralCache(t *testing.T) {
	if !mkfsAvailable() {
		t.Skip("mkfs.erofs required")
	}
	if err := flatten.RequireOwnershipCap(); err != nil {
		t.Skip(err)
	}
	img := syntheticImage(t)
	layers, err := img.Layers()
	if err != nil {
		t.Fatal(err)
	}
	// The same digest is read twice before anything can be released.
	img, err = mutate.AppendLayers(img, layers[0])
	if err != nil {
		t.Fatal(err)
	}
	host := startRegistry(t)
	ref := host + "/test/release:v1"
	pushImage(t, ref, img)
	var expected string
	for _, mode := range []string{"persistent", "direct", "ephemeral"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.TmpDir = t.TempDir()
			if mode == "ephemeral" {
				cfg.Cache.Dir = ""
			}
			var cache *Cache
			var cleanup func()
			var err error
			if mode == "direct" {
				cache, err = OpenCache(t.TempDir(), 0)
				cleanup = func() {}
			} else {
				cache, cleanup, err = cfg.OpenCache()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			res, err := cfg.Resolve(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			src, err := cfg.Pull(context.Background(), res, cache)
			if err != nil {
				t.Fatal(err)
			}
			before, err := src.ConfigJSON()
			if err != nil {
				t.Fatal(err)
			}
			seen := false
			out := filepath.Join(t.TempDir(), "image.erofs")
			err = flatten.Build(src, out, flatten.Options{TmpDir: cfg.TmpDir, Progress: func(stage string, _, _ int) {
				if stage != "build-erofs" {
					return
				}
				seen = true
				_, statErr := os.Stat(cache.dir)
				if mode == "ephemeral" {
					if !os.IsNotExist(statErr) {
						t.Fatalf("cache still exists at mkfs: %v", statErr)
					}
				} else if statErr != nil {
					t.Fatalf("shared cache removed: %v", statErr)
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			if !seen {
				t.Fatal("mkfs not reached")
			}
			after, err := src.ConfigJSON()
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("config lost: %v", err)
			}
			got := sha256File(t, out)
			if expected == "" {
				expected = got
			} else if got != expected {
				t.Fatal("output changed")
			}
			// Existing caller cleanup/eviction remains safe after early release.
			cache.maxSize = 1
			if mode == "ephemeral" {
				if err := cache.MaybeEvict(); err != nil {
					t.Fatal(err)
				}
				if err := src.(flatten.LayerReleaser).ReleaseLayers(); err != nil {
					t.Fatal(err)
				}
			} else {
				again, err := src.Layers()
				if err != nil {
					t.Fatal(err)
				}
				for _, open := range again {
					r, err := open()
					if err != nil {
						t.Fatal(err)
					}
					r.Close()
				}
			}
		})
	}
}
