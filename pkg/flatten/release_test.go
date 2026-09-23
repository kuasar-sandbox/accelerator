package flatten

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

type releaseTestSource struct {
	opened, closed, releases int
	badLayer                 bool
	releaseErr               error
}
type releaseTestReader struct {
	io.Reader
	close func()
}

func (r *releaseTestReader) Close() error { r.close(); return nil }
func (s *releaseTestSource) Layers() ([]LayerOpener, error) {
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	_ = w.Close()
	data := b.Bytes()
	if s.badLayer {
		data = []byte("broken tar")
	}
	open := func() (io.ReadCloser, error) {
		s.opened++
		return &releaseTestReader{Reader: bytes.NewReader(data), close: func() { s.closed++ }}, nil
	}
	return []LayerOpener{open, open}, nil
}
func (s *releaseTestSource) ConfigJSON() ([]byte, error) { return []byte(`{"config":{}}`), nil }
func (s *releaseTestSource) ReleaseLayers() error {
	s.releases++
	if s.opened != 2 || s.closed != 2 {
		return errors.New("readers still in use")
	}
	return s.releaseErr
}
func TestBuildReleaseLayers(t *testing.T) {
	sentinel := errors.New("cleanup failed")
	for _, tt := range []struct {
		name        string
		bad         bool
		releaseErr  error
		wantRelease int
	}{
		{"before-mkfs", false, nil, 1},
		{"release-error", false, sentinel, 1},
		{"apply-error", true, nil, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("MKFS_EROFS_PATH", filepath.Join(dir, "absent-mkfs"))
			src := &releaseTestSource{badLayer: tt.bad, releaseErr: tt.releaseErr}
			reachedMkfs := false
			err := Build(src, filepath.Join(dir, "out"), Options{TmpDir: dir, Progress: func(stage string, _, _ int) {
				if stage == "build-erofs" {
					reachedMkfs = true
					if src.releases != 1 || src.closed != 2 {
						t.Fatal("mkfs reached before closed layers were released")
					}
				}
			}})
			if err == nil {
				t.Fatal("expected failure")
			}
			if src.releases != tt.wantRelease {
				t.Fatalf("release calls=%d", src.releases)
			}
			if src.closed != src.opened {
				t.Fatal("reader not closed")
			}
			if tt.releaseErr != nil && !errors.Is(err, sentinel) {
				t.Fatalf("release error lost: %v", err)
			}
			if reachedMkfs != (tt.name == "before-mkfs") {
				t.Fatalf("unexpected mkfs stage: %v", err)
			}
			if tt.bad && !strings.Contains(err.Error(), "apply layer") {
				t.Fatal(err)
			}
		})
	}
}
