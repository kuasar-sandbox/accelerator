package remote

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/random"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/klauspost/compress/zstd"
)

// startRegistry spins up an in-memory OCI registry over httptest and returns
// its host:port (HTTP, hence insecure).
func startRegistry(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func pushImage(t *testing.T, ref string, img v1.Image) {
	t.Helper()
	r, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	if err := ggcrremote.Write(r, img, ggcrremote.WithContext(context.Background())); err != nil {
		t.Fatalf("push %s: %v", ref, err)
	}
}

func testConfig(t *testing.T) *Config {
	t.Helper()
	c := &Config{Insecure: true, Cache: CacheConfig{Dir: t.TempDir()}}
	if err := c.normalize(); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestPullSourceMatches — Resolve pins the right digest, and Pull yields a
// flatten.Source whose config and uncompressed layers match the pushed image,
// with every blob landed in the cache.
func TestPullSourceMatches(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(2048, 3)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/app:v1"
	pushImage(t, ref, img)

	cfg := testConfig(t)
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	wantDigest, _ := img.Digest()
	if res.Hash != wantDigest {
		t.Fatalf("resolved %s, want %s", res.Hash, wantDigest)
	}

	cache, err := OpenCache(cfg.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	src, err := cfg.Pull(ctx, res, cache)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}

	wantCfg, _ := img.RawConfigFile()
	gotCfg, err := src.ConfigJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCfg, wantCfg) {
		t.Fatal("config JSON mismatch")
	}

	openers, err := src.Layers()
	if err != nil {
		t.Fatal(err)
	}
	imgLayers, _ := img.Layers()
	if len(openers) != len(imgLayers) {
		t.Fatalf("got %d layers, want %d", len(openers), len(imgLayers))
	}
	for i, open := range openers {
		rc, err := open()
		if err != nil {
			t.Fatalf("layer %d open: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		wrc, _ := imgLayers[i].Uncompressed()
		want, _ := io.ReadAll(wrc)
		wrc.Close()
		if !bytes.Equal(got, want) {
			t.Fatalf("layer %d uncompressed content mismatch", i)
		}
	}

	mfst, _ := img.Manifest()
	if !cache.Has(mfst.Config.Digest) {
		t.Fatal("config blob not cached")
	}
	for _, l := range mfst.Layers {
		if !cache.Has(l.Digest) {
			t.Fatalf("layer %s not cached", l.Digest)
		}
	}

	// A second Pull is served entirely from cache (blobs already present).
	if _, err := cfg.Pull(ctx, res, cache); err != nil {
		t.Fatalf("second Pull: %v", err)
	}
}

// TestDecompress — gzip / zstd / plain layer streams all yield the original
// uncompressed tar bytes.
func TestDecompress(t *testing.T) {
	payload := []byte("the quick brown fox jumps over the lazy dog")

	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	gw.Write(payload)
	gw.Close()

	var zs bytes.Buffer
	zw, _ := zstd.NewWriter(&zs)
	zw.Write(payload)
	zw.Close()

	cases := []struct {
		mt types.MediaType
		in []byte
	}{
		{"application/vnd.oci.image.layer.v1.tar+gzip", gz.Bytes()},
		{"application/vnd.docker.image.rootfs.diff.tar.gzip", gz.Bytes()},
		{"application/vnd.oci.image.layer.v1.tar+zstd", zs.Bytes()},
		{"application/vnd.oci.image.layer.v1.tar", payload},
	}
	for _, tc := range cases {
		rc, err := decompress(bytes.NewReader(tc.in), tc.mt)
		if err != nil {
			t.Fatalf("%s: %v", tc.mt, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, payload) {
			t.Fatalf("%s: got %q, want %q", tc.mt, got, payload)
		}
	}
}

// TestLooksLikeReference — the export source detector accepts docker refs and
// rejects obvious non-refs.
func TestLooksLikeReference(t *testing.T) {
	for _, s := range []string{"nginx", "nginx:1.27", "gcr.io/p/x@sha256:" + strings.Repeat("a", 64), "registry.example.com:5000/team/app:v1"} {
		if !LooksLikeReference(s) {
			t.Errorf("LooksLikeReference(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "UPPER/CASE:Bad", "bad ref with spaces"} {
		if LooksLikeReference(s) {
			t.Errorf("LooksLikeReference(%q) = true, want false", s)
		}
	}
}
