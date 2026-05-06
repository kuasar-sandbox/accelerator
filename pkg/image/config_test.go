package flatten

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestRuntimeConfig_DeterministicMarshal — same input fields produce
// byte-identical output across N invocations, including for fields
// with map types whose iteration order is randomised by Go.
func TestRuntimeConfig_DeterministicMarshal(t *testing.T) {
	cfg := &RuntimeConfig{
		Architecture: "amd64",
		Os:           "linux",
		User:         "1000",
		Env:          []string{"PATH=/usr/bin", "LANG=C.UTF-8"},
		Cmd:          []string{"-c", "echo hi"},
		Labels: map[string]string{
			"app":   "frontend",
			"layer": "edge",
			"tier":  "web",
		},
		ExposedPorts: map[string]struct{}{
			"443/tcp": {},
			"80/tcp":  {},
		},
	}
	var first []byte
	for i := 0; i < 100; i++ {
		out, err := cfg.MarshalDeterministic()
		if err != nil {
			t.Fatalf("MarshalDeterministic: %v", err)
		}
		if first == nil {
			first = out
			continue
		}
		if !bytes.Equal(first, out) {
			t.Fatalf("round %d: output drift\n  first: %s\n  this:  %s", i, first, out)
		}
	}
	// Spot-check: keys are sorted alphabetically inside Labels.
	got := string(first)
	idxApp := bytes.Index(first, []byte(`"app"`))
	idxLayer := bytes.Index(first, []byte(`"layer"`))
	idxTier := bytes.Index(first, []byte(`"tier"`))
	if !(idxApp >= 0 && idxLayer > idxApp && idxTier > idxLayer) {
		t.Fatalf("Labels not sorted alphabetically in output: %s", got)
	}
}

// TestRuntimeConfig_OmitemptyClean — only Architecture/Os should
// appear when the rest of the struct is zero. No "User":"" noise.
func TestRuntimeConfig_OmitemptyClean(t *testing.T) {
	cfg := &RuntimeConfig{Architecture: "arm64", Os: "linux"}
	out, err := cfg.MarshalDeterministic()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"Architecture":"arm64","Os":"linux"}`
	if string(out) != want {
		t.Fatalf("output:\n  got:  %s\n  want: %s", out, want)
	}
}

// TestRuntimeConfig_PassThroughArch — flatten-ctl is content-agnostic;
// it must not validate Architecture/Os, even for unusual values.
func TestRuntimeConfig_PassThroughArch(t *testing.T) {
	cases := []struct{ arch, os string }{
		{"amd64", "linux"},
		{"arm64", "linux"},
		{"riscv64", "linux"},  // exotic — must not error
		{"amd64", "windows"},  // not supported by platform — must not error here
		{"", ""},              // source image bug — pass through
	}
	for _, tc := range cases {
		cfg := &RuntimeConfig{Architecture: tc.arch, Os: tc.os}
		out, err := cfg.MarshalDeterministic()
		if err != nil {
			t.Errorf("(%q, %q): %v", tc.arch, tc.os, err)
			continue
		}
		var got map[string]any
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatal(err)
		}
		if got["Architecture"] != tc.arch || got["Os"] != tc.os {
			t.Errorf("roundtrip mismatch: got %v", got)
		}
	}
}

// TestExtractRuntimeConfig_DropsNonDeterministic — `created`,
// `history`, `rootfs.diff_ids` and friends must not appear in the
// projected RuntimeConfig.
func TestExtractRuntimeConfig_DropsNonDeterministic(t *testing.T) {
	dir := t.TempDir()
	src := `{
		"created": "2026-04-23T09:32:11.123456789Z",
		"author": "ci@example.com",
		"architecture": "amd64",
		"os": "linux",
		"config": {
			"User": "1000",
			"Env": ["PATH=/bin"],
			"Cmd": ["/bin/sh"],
			"Hostname": "should-be-dropped",
			"Domainname": "should-be-dropped",
			"AttachStdin": false,
			"AttachStdout": true,
			"AttachStderr": true,
			"Tty": false,
			"OpenStdin": false,
			"StdinOnce": false,
			"ArgsEscaped": true,
			"OnBuild": null,
			"MacAddress": ""
		},
		"rootfs": {
			"type": "layers",
			"diff_ids": ["sha256:aaa", "sha256:bbb"]
		},
		"history": [
			{"created": "2026-04-23T...", "created_by": "/bin/sh -c apk add"},
			{"created": "2026-04-23T...", "empty_layer": true}
		]
	}`
	cfgPath := "image-config.json"
	if err := os.WriteFile(filepath.Join(dir, cfgPath), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	rc, err := ExtractRuntimeConfig(dir, cfgPath)
	if err != nil {
		t.Fatalf("ExtractRuntimeConfig: %v", err)
	}
	out, err := rc.MarshalDeterministic()
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	for _, banned := range []string{"created", "author", "history", "diff_ids", "Hostname", "Domainname", "OnBuild", "ArgsEscaped"} {
		if bytes.Contains(out, []byte(banned)) {
			t.Errorf("output contains banned field %q: %s", banned, got)
		}
	}
	for _, want := range []string{`"Architecture":"amd64"`, `"Os":"linux"`, `"User":"1000"`, `"Cmd":["/bin/sh"]`} {
		if !bytes.Contains(out, []byte(want)) {
			t.Errorf("output missing %q: %s", want, got)
		}
	}
}

// TestReadConfigFromFile_NoZip — file with only an EROFS-shaped
// header (no trailing ZIP) returns fs.ErrNotExist.
func TestReadConfigFromFile_NoZip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no-zip.bin")
	// 64 KiB of zeros — no ZIP trailer.
	if err := os.WriteFile(path, make([]byte, 64*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ReadConfigFromFile(path)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("want fs.ErrNotExist, got %v", err)
	}
}
