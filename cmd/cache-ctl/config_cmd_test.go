package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigValidateMaintainedExamples(t *testing.T) {
	paths, err := filepath.Glob("../../pkg/cache/runtime/examples/*.yaml")
	if err != nil || len(paths) == 0 {
		t.Fatalf("find maintained examples: %v (%d files)", err, len(paths))
	}
	paths = append(paths, "../../deploy/systemd/cache-activation.yaml.example")
	generated := filepath.Join(t.TempDir(), "generated.yaml")
	if err := os.WriteFile(generated, []byte(cacheConfigTemplate), 0600); err != nil {
		t.Fatal(err)
	}
	paths = append(paths, generated)
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			var output bytes.Buffer
			if err := runConfigValidate([]string{"--config", path}, "", &output); err != nil {
				t.Fatal(err)
			}
			if output.String() != "{\"valid\":true}\n" {
				t.Fatalf("unexpected output: %q", output.String())
			}
		})
	}
}

func TestConfigValidatePrecedenceAndNoBackendSideEffects(t *testing.T) {
	dir := t.TempDir()
	backend := filepath.Join(dir, "must-not-be-created")
	path := filepath.Join(dir, "cache.yaml")
	config := "mode: tiered\nlisten: fd=unavailable\ntiers:\n  - type: embedded\n    rocks:\n      path: " + backend + "\norigin:\n  type: store\n  store:\n    endpoint: unix:///nonexistent/store.sock\n"
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		args     []string
		fallback string
	}{
		{"flag-overrides-env", []string{"--config", path}, filepath.Join(dir, "missing.yaml")},
		{"environment-fallback", nil, path},
		{"empty-flag-fallback", []string{"--config="}, path},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := runConfigValidate(test.args, test.fallback, io.Discard); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := os.Stat(backend); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation touched backend directory: %v", err)
	}
}

func TestConfigValidateRejectsInvalidInput(t *testing.T) {
	validTier := "mode: tiered\nlisten: 127.0.0.1:7070\ntiers:\n  - type: upstream\n    endpoint: 127.0.0.1:7072\n"
	for _, test := range []struct{ name, config, want string }{
		{"legacy-origin", validTier + "origin:\n  backend: fs\n  fs: {root: /var/store}\n", "origin.type"},
		{"missing-origin", validTier, "origin is required"},
		{"missing-tier", "mode: tiered\nlisten: :7070\n", "at least one tier"},
		{"conflicting-backend", "mode: local\ntype: redis\nlisten: :7070\nredis: {endpoint: '127.0.0.1:6379'}\nrocks: {path: /var/cache}\n", "rocks config is not valid"},
		{"malformed-yaml", "mode: [", "parse config"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid.yaml")
			if err := os.WriteFile(path, []byte(test.config), 0600); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			err := runConfigValidate([]string{"--config", path}, "", &output)
			if err == nil || !strings.Contains(err.Error(), test.want) || output.Len() != 0 {
				t.Fatalf("got error %v and output %q, want %q and no success output", err, output.String(), test.want)
			}
		})
	}
	for _, args := range [][]string{nil, {"--config", "missing.yaml"}, {"extra"}, {"--unknown"}} {
		if err := runConfigValidate(args, "", io.Discard); err == nil {
			t.Fatalf("accepted invalid arguments %q", args)
		}
	}
	if err := runConfigValidate([]string{"--help"}, "", io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help: %v", err)
	}
}
