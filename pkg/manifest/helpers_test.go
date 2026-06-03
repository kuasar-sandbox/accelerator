package manifest

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

// TestCustomerKeyResolution covers the $MANIFEST_KEY override:
// env-overrides-file precedence, env-only (file omitted), file-only, the
// neither-set error, and malformed env values.
func TestCustomerKeyResolution(t *testing.T) {
	fileKey := strings.Repeat("ab", 32) // 64 hex chars = 32 bytes
	envKey := strings.Repeat("cd", 32)
	wantFile, _ := hex.DecodeString(fileKey)
	wantEnv, _ := hex.DecodeString(envKey)

	t.Run("file only (env unset)", func(t *testing.T) {
		t.Setenv(CustomerKeyEnv, "") // empty == unset for our resolution
		got, err := (&Config{Manifest: ManifestSubConfig{Key: fileKey}}).CustomerKey()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[:], wantFile) {
			t.Errorf("got %x, want file key %x", got, wantFile)
		}
	})

	t.Run("env only (file omitted)", func(t *testing.T) {
		t.Setenv(CustomerKeyEnv, envKey)
		got, err := (&Config{}).CustomerKey()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[:], wantEnv) {
			t.Errorf("got %x, want env key %x", got, wantEnv)
		}
	})

	t.Run("env overrides file", func(t *testing.T) {
		t.Setenv(CustomerKeyEnv, envKey)
		got, err := (&Config{Manifest: ManifestSubConfig{Key: fileKey}}).CustomerKey()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[:], wantEnv) {
			t.Errorf("env must override file: got %x, want env key %x", got, wantEnv)
		}
	})

	t.Run("env not written back into Config", func(t *testing.T) {
		// Guards the reason the override lives in CustomerKey, not LoadConfig:
		// the env key must never enter the Config struct, or `config show`
		// (which marshals the loaded Config) would echo the secret.
		t.Setenv(CustomerKeyEnv, envKey)
		cfg := &Config{Manifest: ManifestSubConfig{Key: ""}}
		if _, err := cfg.CustomerKey(); err != nil {
			t.Fatal(err)
		}
		if cfg.Manifest.Key != "" {
			t.Errorf("CustomerKey wrote the env key back into Config (%q) — would leak via `config show`", cfg.Manifest.Key)
		}
	})

	t.Run("neither set", func(t *testing.T) {
		t.Setenv(CustomerKeyEnv, "")
		if _, err := (&Config{}).CustomerKey(); err == nil {
			t.Fatal("expected error when neither manifest.key nor $MANIFEST_KEY is set")
		}
	})

	t.Run("malformed env (not hex)", func(t *testing.T) {
		t.Setenv(CustomerKeyEnv, "not-a-hex-string")
		if _, err := (&Config{Manifest: ManifestSubConfig{Key: fileKey}}).CustomerKey(); err == nil {
			t.Fatal("expected error for non-hex env key")
		}
	})

	t.Run("wrong-length env", func(t *testing.T) {
		t.Setenv(CustomerKeyEnv, "abcd") // valid hex, only 2 bytes
		if _, err := (&Config{}).CustomerKey(); err == nil {
			t.Fatal("expected error for short env key")
		}
	})
}
