package remote

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"

	"github.com/kuasar-sandbox/sandbox-builder/pkg/flatten"
)

// syntheticImage builds a small single-layer image whose files have normal
// 0644 modes. ggcr's random.Image / crane.Image emit mode-0 (0000) tar
// entries, which flatten faithfully writes to the staging tree — then a
// non-root mkfs.erofs can't read them. Real-world images use readable modes
// (and the data-plane flattens as root), so a 0644 layer is the realistic
// fixture for an as-non-root unit test.
func syntheticImage(t *testing.T) v1.Image {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	// Deterministic entry order → reproducible layer.
	entries := []struct {
		name, body string
	}{
		{"app/run.sh", "#!/bin/sh\necho hi\n"},
		{"data/blob", "some content here\n"},
		{"etc/conf", "k=v\n"},
	}
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Mode: 0o644, Size: int64(len(e.body)), Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(e.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	tarBytes := buf.Bytes()
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(tarBytes)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

func mkfsAvailable() bool {
	if os.Getenv("MKFS_EROFS_PATH") != "" {
		return true
	}
	_, err := exec.LookPath("mkfs.erofs")
	return err == nil
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestCrossSourceByteEqual — the same image flattened from a registry pull and
// from an equivalent docker-archive must produce byte-identical EROFS. This is
// the core determinism guarantee of routing both sources through flatten.Build.
// It needs mkfs.erofs at runtime, so it is skipped in pure-unit environments
// and runs in e2e / CI where the binary is built.
func TestCrossSourceByteEqual(t *testing.T) {
	if !mkfsAvailable() {
		t.Skip("mkfs.erofs not found (set MKFS_EROFS_PATH or add to PATH); cross-source equality runs in e2e")
	}
	host := startRegistry(t)
	img := syntheticImage(t)
	ref := host + "/test/equal:v1"
	pushImage(t, ref, img)

	dir := t.TempDir()

	// (A) registry source → flatten.
	cfg := testConfig(t)
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := OpenCache(cfg.CacheDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	src, err := cfg.Pull(ctx, res, cache)
	if err != nil {
		t.Fatal(err)
	}
	outA := filepath.Join(dir, "a.erofs")
	if err := flatten.Build(src, outA, flatten.Options{TmpDir: dir}); err != nil {
		t.Fatalf("registry flatten: %v", err)
	}

	// (B) equivalent docker-archive → flatten.
	archive := filepath.Join(dir, "img.tar")
	tagRef, err := name.NewTag(ref, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	if err := tarball.WriteToFile(archive, tagRef, img); err != nil {
		t.Fatalf("write docker-archive: %v", err)
	}
	outB := filepath.Join(dir, "b.erofs")
	if err := flatten.FlattenFileWith(archive, outB, flatten.Options{TmpDir: dir}); err != nil {
		t.Fatalf("archive flatten: %v", err)
	}

	if a, b := sha256File(t, outA), sha256File(t, outB); a != b {
		t.Fatalf("cross-source EROFS differ:\n  registry: %s\n  archive:  %s", a, b)
	}
}
