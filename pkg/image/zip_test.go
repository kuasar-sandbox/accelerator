package image

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestAppendAndReadConfigZip — append a ZIP to an EROFS-shaped
// prefix, then read it back. Verifies (a) Go's archive/zip locates
// the trailing ZIP via EOCD scan despite the prefix, and (b) the
// roundtrip is faithful.
func TestAppendAndReadConfigZip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blob.erofs")
	prefix := []byte("FAKE EROFS BYTES — flatten-ctl puts the real EROFS here.\n")
	prefix = append(prefix, make([]byte, 4096-len(prefix))...) // pad to 4 KiB block
	if err := os.WriteFile(path, prefix, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := &RuntimeConfig{
		Architecture: "arm64",
		Os:           "linux",
		User:         "1000",
		Env:          []string{"PATH=/usr/bin", "LANG=C.UTF-8"},
		Entrypoint:   []string{"/bin/sh"},
		Cmd:          []string{"-c", "echo hi"},
		WorkingDir:   "/app",
		Labels:       map[string]string{"app": "x", "tier": "y"},
		ExposedPorts: map[string]struct{}{"80/tcp": {}},
	}
	if err := AppendConfigZip(path, cfg); err != nil {
		t.Fatalf("AppendConfigZip: %v", err)
	}

	got, err := ReadConfigFromFile(path)
	if err != nil {
		t.Fatalf("ReadConfigFromFile: %v", err)
	}
	// reflect.DeepEqual handles maps/slices correctly here; the
	// roundtrip is exact.
	if !reflect.DeepEqual(cfg, got) {
		t.Fatalf("roundtrip mismatch:\n  want: %+v\n  got:  %+v", cfg, got)
	}
}

// TestAppendConfigZip_DeterministicBytes — appending the same config
// to two identical prefixes twice yields byte-identical files.
func TestAppendConfigZip_DeterministicBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := &RuntimeConfig{
		Architecture: "amd64",
		Os:           "linux",
		Cmd:          []string{"/bin/sh", "-c", "echo hi"},
		Labels:       map[string]string{"a": "1", "b": "2", "c": "3"},
	}

	mk := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("PREFIX"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := AppendConfigZip(p, cfg); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p1, p2 := mk("a.bin"), mk("b.bin")
	d1, _ := os.ReadFile(p1)
	d2, _ := os.ReadFile(p2)
	if !equalBytes(d1, d2) {
		t.Fatalf("non-deterministic output:\n  a: % x\n  b: % x", d1, d2)
	}
}

func equalBytes(a, b []byte) bool {
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
