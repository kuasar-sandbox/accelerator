package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "store.yaml")
	if err := os.WriteFile(name, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestLoadConfigCompatibility(t *testing.T) {
	t.Setenv("STORE_TEST_BUCKET", "expanded")
	cfg, err := LoadConfig(writeConfig(t, `
backend: obs
obs:
  endpoint: https://objects.example
  bucket: ${STORE_TEST_BUCKET}
`), false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backend != "s3" || cfg.OBS != nil || cfg.S3.Bucket != "expanded" || cfg.S3.Region != "us-east-1" {
		t.Fatalf("legacy normalization changed: %#v", cfg)
	}
	if cfg.Listen != "" {
		t.Fatalf("admin load unexpectedly required listen: %q", cfg.Listen)
	}
	if cfg.Generations == nil || cfg.Generations.S3 == nil || cfg.GenerationRefreshInterval() != 5*time.Second {
		t.Fatalf("default generation source changed: %#v", cfg.Generations)
	}
}

func TestLoadConfigPreservesMappingPresenceValidation(t *testing.T) {
	_, err := LoadConfig(writeConfig(t, `
backend: fs
fs:
  root: /store
s3: null
`), false)
	if err == nil || !strings.Contains(err.Error(), "object storage config is not valid") {
		t.Fatalf("mixed section error = %v", err)
	}
}

func TestNormalizeDirectGoHasNoFileOrEnvironmentSideEffects(t *testing.T) {
	t.Setenv("STORE_TEST_ROOT", t.TempDir())
	cfg := Config{
		Backend: "fs",
		FS:      &FSConfig{Root: "${STORE_TEST_ROOT}"},
		Generations: &GenerationsConfig{
			File: &GenerationFileConfig{Path: "${STORE_TEST_GENERATIONS}"},
		},
	}
	if err := cfg.Normalize(false); err != nil {
		t.Fatal(err)
	}
	if cfg.FS.Root != "${STORE_TEST_ROOT}" || cfg.Generations.File.Path != "${STORE_TEST_GENERATIONS}" {
		t.Fatalf("Normalize expanded environment: %#v", cfg)
	}
	if cfg.GenerationRefreshInterval() != 5*time.Second {
		t.Fatalf("refresh = %v, want 5s", cfg.GenerationRefreshInterval())
	}
}

func TestGenerationRefreshValidation(t *testing.T) {
	for _, refresh := range []string{"bad", "0", "0s", "-1s"} {
		t.Run(refresh, func(t *testing.T) {
			cfg := Config{Backend: "fs", FS: &FSConfig{Root: "/store"}, Generations: &GenerationsConfig{
				RefreshInterval: refresh,
				File:            &GenerationFileConfig{Path: "/generations"},
			}}
			if err := cfg.Normalize(false); err == nil {
				t.Fatalf("Normalize accepted refresh %q", refresh)
			}
		})
	}
}
