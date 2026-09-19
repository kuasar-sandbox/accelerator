package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocationFlagsRequireExplicitAbsoluteFileMapping(t *testing.T) {
	var locations locationFlags
	if err := locations.Set("images=file:///srv/artifacts"); err != nil {
		t.Fatal(err)
	}
	if got := locations["images"]; got != "/srv/artifacts" {
		t.Fatalf("path = %q", got)
	}
	for _, bad := range []string{"images=/tmp", "images=file://relative", "=file:///tmp", "images=file:///again"} {
		if err := locations.Set(bad); err == nil {
			t.Errorf("Set(%q) succeeded", bad)
		}
	}
}

func TestDirectOutputExclusiveAndCleansFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result")
	f, finish, err := openDirectOutput(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	writeErr := os.ErrInvalid
	if got := finish(writeErr); got == nil {
		t.Fatal("finish accepted failure")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("partial output remains: %v", err)
	}

	if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openDirectOutput(path); err == nil {
		t.Fatal("existing output was overwritten")
	}
}

func TestSamePathUsesCanonicalAbsolutePath(t *testing.T) {
	dir := t.TempDir()
	if !samePath(filepath.Join(dir, "x"), filepath.Join(dir, ".", "x")) {
		t.Fatal("same path not detected")
	}
}
