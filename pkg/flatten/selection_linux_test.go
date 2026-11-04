//go:build linux

package flatten

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSelectionDefaultAndExplicitBudget(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 12; i++ {
		d := filepath.Join(root, fmt.Sprintf("dir-%02d", i))
		if err := os.Mkdir(d, 0755); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 12; j++ {
			if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("file-%02d", j)), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Explicit quotas retain their conservative cumulative admission behavior.
	if _, err := selectSource(context.Background(), root, nil, 8<<10); err == nil || !strings.Contains(err.Error(), "budget exceeded") {
		t.Fatalf("explicit quota no longer enforced: %v", err)
	}
	limits, err := (PlanLimits{}).defaults()
	if err != nil {
		t.Fatal(err)
	}
	s, err := selectSource(context.Background(), root, nil, limits.IndexMemoryBytes/4)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "dir-00", "file-00"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.validate(context.Background()); err == nil || !strings.Contains(err.Error(), "source tree changed") {
		t.Fatalf("source mutation was not detected: %v", err)
	}
}

func TestVirtualSelectionCapturesSpoolIdentityBeforePlanning(t *testing.T) {
	scratch := t.TempDir()
	b := testLayerTar(t, layerEntry{name: "payload", body: "verified-data"})
	body := &observedBody{Reader: bytes.NewReader(b)}
	layer := streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body)
	config := []byte(fmt.Sprintf(`{"rootfs":{"type":"layers","diff_ids":[%q]}}`, layer.DiffID))
	in, err := NewImageInput(config, []LayerInput{layer})
	if err != nil {
		t.Fatal(err)
	}
	mutated := false
	_, err = PrepareImage(context.Background(), in, Options{
		TmpDir: scratch,
		Progress: func(stage string, _, _ int) {
			if stage != "build-erofs" {
				return
			}
			matches, globErr := filepath.Glob(filepath.Join(scratch, "flatten-prepared-*", "payloads"))
			if globErr != nil || len(matches) != 1 {
				t.Errorf("expected one payload spool in %s, got %v (%v)", scratch, matches, globErr)
				return
			}
			now := time.Now().Add(2 * time.Second)
			if writeErr := os.WriteFile(matches[0], []byte("tampered-data"), 0600); writeErr != nil {
				t.Errorf("tamper payload spool: %v", writeErr)
				return
			}
			if chtimesErr := os.Chtimes(matches[0], now, now); chtimesErr != nil {
				t.Errorf("chtimes payload spool: %v", chtimesErr)
				return
			}
			mutated = true
		},
	})
	if !mutated {
		t.Fatal("build-erofs progress callback did not mutate payload spool")
	}
	if err == nil || !strings.Contains(err.Error(), "virtual payload spool changed") {
		t.Fatalf("expected virtual payload spool changed error, got %v", err)
	}
}
