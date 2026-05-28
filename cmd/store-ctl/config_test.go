package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "store-ctl.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return p
}

// TestLoadConfig_FS — minimal valid fs yaml.
func TestLoadConfig_FS(t *testing.T) {
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: fs
fs:
  root: /var/lib/store
`)
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Backend != "fs" || cfg.FS.Root != "/var/lib/store" {
		t.Errorf("unexpected: %+v", cfg.FS)
	}
	if !cfg.VerifyKey() {
		t.Errorf("VerifyKey should default to true")
	}
}

// TestLoadConfig_FS_VerifyOff — nullable bool honoured.
func TestLoadConfig_FS_VerifyOff(t *testing.T) {
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: fs
fs:
  root: /var/lib/store
  verify_content_key: false
`)
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.VerifyKey() {
		t.Error("VerifyKey should be false")
	}
}

// TestLoadConfig_OBS_HappyPath — minimal valid obs yaml.
func TestLoadConfig_OBS_HappyPath(t *testing.T) {
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: obs
obs:
  endpoint: https://obs.cn-north-4.example.com
  region: cn-north-4
  bucket: test-bucket
  prefix: store/
  access_key: ABC
  secret_key: XYZ
  op_timeout: 7s
`)
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Backend != "obs" {
		t.Errorf("Backend=%q want obs", cfg.Backend)
	}
	if cfg.OBS.Bucket != "test-bucket" || cfg.OBS.Endpoint == "" {
		t.Errorf("obs fields unexpected: %+v", cfg.OBS)
	}
	to, err := cfg.OBSOpTimeout()
	if err != nil {
		t.Fatalf("OBSOpTimeout: %v", err)
	}
	if to.String() != "7s" {
		t.Errorf("OpTimeout=%v want 7s", to)
	}
}

// TestLoadConfig_OBS_EnvExpansion — ${OBS_AK} / ${OBS_SK} resolve
// to the corresponding env vars.
func TestLoadConfig_OBS_EnvExpansion(t *testing.T) {
	t.Setenv("MY_TEST_AK", "from-env-AK")
	t.Setenv("MY_TEST_SK", "from-env-SK")
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: obs
obs:
  endpoint: https://obs.example.com
  bucket: b
  access_key: ${MY_TEST_AK}
  secret_key: ${MY_TEST_SK}
`)
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.OBS.AccessKey != "from-env-AK" {
		t.Errorf("AccessKey=%q want from-env-AK", cfg.OBS.AccessKey)
	}
	if cfg.OBS.SecretKey != "from-env-SK" {
		t.Errorf("SecretKey=%q want from-env-SK", cfg.OBS.SecretKey)
	}
}

// TestLoadConfig_OBS_DiscoverFromHome — yaml leaves AK/SK/endpoint
// blank; ~/.obsconfig (HOME-redirected via t.Setenv) supplies them.
func TestLoadConfig_OBS_DiscoverFromHome(t *testing.T) {
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	obsCfg := `{
  "endpoint": "http://obs.cn-north-7.example.com",
  "access-key": "DISCOVERED_AK",
  "secret-key": "DISCOVERED_SK"
}`
	if err := os.WriteFile(filepath.Join(tmpHome, ".obsconfig"), []byte(obsCfg), 0o600); err != nil {
		t.Fatalf("write .obsconfig: %v", err)
	}
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: obs
obs:
  bucket: ops-dev
`)
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.OBS.Endpoint != "http://obs.cn-north-7.example.com" {
		t.Errorf("Endpoint not auto-discovered: %q", cfg.OBS.Endpoint)
	}
	if cfg.OBS.AccessKey != "DISCOVERED_AK" {
		t.Errorf("AccessKey not auto-discovered: %q", cfg.OBS.AccessKey)
	}
	if cfg.OBS.SecretKey != "DISCOVERED_SK" {
		t.Errorf("SecretKey not auto-discovered: %q", cfg.OBS.SecretKey)
	}
	if cfg.OBS.Region != "cn-north-7" {
		t.Errorf("Region not auto-derived from endpoint: %q", cfg.OBS.Region)
	}
}

// TestLoadConfig_OBS_YAMLBeatsDiscover — yaml-explicit value wins
// over ~/.obsconfig fallback.
func TestLoadConfig_OBS_YAMLBeatsDiscover(t *testing.T) {
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)
	obsCfg := `{
  "endpoint": "http://obs.discovered.example.com",
  "access-key": "DISCOVERED_AK",
  "secret-key": "DISCOVERED_SK"
}`
	if err := os.WriteFile(filepath.Join(tmpHome, ".obsconfig"), []byte(obsCfg), 0o600); err != nil {
		t.Fatalf("write .obsconfig: %v", err)
	}
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: obs
obs:
  endpoint: http://obs.cn-north-4.example.com
  bucket: ops-dev
  access_key: YAML_AK
  secret_key: YAML_SK
`)
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.OBS.Endpoint != "http://obs.cn-north-4.example.com" ||
		cfg.OBS.AccessKey != "YAML_AK" || cfg.OBS.SecretKey != "YAML_SK" {
		t.Errorf("yaml didn't win: %+v", cfg.OBS)
	}
	if cfg.OBS.Region != "cn-north-4" {
		t.Errorf("Region auto-derive should still apply when not in yaml: %q", cfg.OBS.Region)
	}
}

// TestLoadConfig_OBS_NoDiscoverNoCreds — no ~/.obsconfig + no creds
// in yaml: not an error at config parse time (SDK default chain
// takes over at runtime). Endpoint still required though.
func TestLoadConfig_OBS_NoDiscoverNoCreds(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: obs
obs:
  endpoint: http://obs.cn-north-4.example.com
  bucket: ops-dev
`)
	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.OBS.AccessKey != "" || cfg.OBS.SecretKey != "" {
		t.Errorf("expected empty creds; got %q/%q", cfg.OBS.AccessKey, cfg.OBS.SecretKey)
	}
}

// TestLoadConfig_AdminCommandsSkipListen — non-serve subcommands
// pass requireListen=false; a yaml without `listen:` parses fine.
func TestLoadConfig_AdminCommandsSkipListen(t *testing.T) {
	path := writeYAML(t, `
backend: fs
fs:
  root: /var/lib/store
`)
	cfg, err := LoadConfig(path, false)
	if err != nil {
		t.Fatalf("LoadConfig (admin mode): %v", err)
	}
	if cfg.Listen != "" {
		t.Errorf("Listen should be empty: %q", cfg.Listen)
	}
}

// TestLoadConfig_RequiredFields — missing required fields produce
// a clear error per backend.
func TestLoadConfig_RequiredFields(t *testing.T) {
	cases := []struct {
		name          string
		body          string
		requireListen bool
		want          string
	}{
		{
			name:          "serve missing listen",
			body:          "backend: fs\nfs:\n  root: /tmp/x\n",
			requireListen: true,
			want:          "listen is required",
		},
		{
			name:          "missing backend",
			body:          "listen: 127.0.0.1:1\n",
			requireListen: true,
			want:          "backend is required",
		},
		{
			name:          "fs missing root",
			body:          "listen: 127.0.0.1:1\nbackend: fs\n",
			requireListen: true,
			want:          "fs.root is required",
		},
		{
			name:          "obs missing bucket",
			body:          "listen: 127.0.0.1:1\nbackend: obs\nobs:\n  endpoint: x\n",
			requireListen: true,
			want:          "obs.bucket is required",
		},
		{
			name:          "obs missing endpoint",
			body:          "listen: 127.0.0.1:1\nbackend: obs\nobs:\n  bucket: b\n",
			requireListen: true,
			want:          "obs.endpoint is required",
		},
		{
			name:          "unknown backend",
			body:          "listen: 127.0.0.1:1\nbackend: alien\n",
			requireListen: true,
			want:          "unknown backend",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			path := writeYAML(t, tc.body)
			_, err := LoadConfig(path, tc.requireListen)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}
