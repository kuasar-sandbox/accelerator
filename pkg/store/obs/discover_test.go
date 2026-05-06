package obs

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadObsConfigFile_HappyPath — JSON shape produced by obsutil
// parses into the expected struct.
func TestLoadObsConfigFile_HappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".obsconfig")
	body := `{
  "endpoint": "http://obs.cn-north-7.example.com",
  "access-key": "AK_TEST",
  "secret-key": "SK_TEST",
  "security-token": ""
}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	cfg, err := loadObsConfigFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg == nil {
		t.Fatal("nil config")
	}
	if cfg.Endpoint != "http://obs.cn-north-7.example.com" {
		t.Errorf("Endpoint=%q", cfg.Endpoint)
	}
	if cfg.AccessKey != "AK_TEST" || cfg.SecretKey != "SK_TEST" {
		t.Errorf("AK/SK mismatch: %+v", cfg)
	}
}

// TestLoadObsConfigFile_AbsentReturnsNil — missing ~/.obsconfig is
// the common case on production hosts; treated as "no fallback",
// not an error.
func TestLoadObsConfigFile_AbsentReturnsNil(t *testing.T) {
	cfg, err := loadObsConfigFile(filepath.Join(t.TempDir(), "nonexistent"))
	if err != nil {
		t.Fatalf("load: unexpected err %v", err)
	}
	if cfg != nil {
		t.Fatalf("got %+v, want nil", cfg)
	}
}

// TestLoadObsConfigFile_MalformedReturnsErr — silent acceptance of
// a malformed file would mask real misconfiguration.
func TestLoadObsConfigFile_MalformedReturnsErr(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".obsconfig")
	if err := os.WriteFile(path, []byte("not-json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := loadObsConfigFile(path); err == nil {
		t.Fatalf("expected error for malformed JSON")
	}
}

// TestRegionFromEndpoint — covers the canonical OBS endpoint
// shapes plus a few edge cases.
func TestRegionFromEndpoint(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"https://obs.cn-north-4.example.com", "cn-north-4"},
		{"http://obs.cn-north-7.example.com", "cn-north-7"},
		{"https://obs.cn-east-3.example.com/", "cn-east-3"},
		{"", ""},
		{"https://example.com", ""},
		{"obs.cn-north-4.example.com", ""}, // no scheme
	}
	for _, tc := range cases {
		got := regionFromEndpoint(tc.in)
		if got != tc.want {
			t.Errorf("regionFromEndpoint(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
