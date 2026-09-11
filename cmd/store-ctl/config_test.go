package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old }()
	defer r.Close()
	defer w.Close()

	fn()
	os.Stderr = old
	if err := w.Close(); err != nil {
		t.Fatalf("close stderr capture: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stderr capture: %v", err)
	}
	return string(out)
}

func TestLoadConfigFS(t *testing.T) {
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
	if cfg.Backend != "fs" || cfg.FS == nil || cfg.FS.Root != "/var/lib/store" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if !cfg.VerifyKey() {
		t.Error("VerifyKey should default to true")
	}
}

func TestLoadConfigFSVerifyOff(t *testing.T) {
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

func TestLoadConfigFSDirectIO(t *testing.T) {
	path := writeYAML(t, `
backend: fs
fs:
  root: /var/lib/store
  direct_io: true
`)
	cfg, err := LoadConfig(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.FS.DirectIO {
		t.Fatal("fs.direct_io=true was not preserved")
	}
}

func TestLoadConfigS3HappyPath(t *testing.T) {
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: s3
s3:
  endpoint: https://objects.example.com
  region: test-region-1
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
	if cfg.Backend != "s3" || cfg.S3 == nil {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.S3.Bucket != "test-bucket" || cfg.S3.Region != "test-region-1" {
		t.Errorf("s3 fields unexpected: %+v", cfg.S3)
	}
	if !cfg.VerifyKey() {
		t.Error("VerifyKey should default to true")
	}
	got, err := cfg.S3OpTimeout()
	if err != nil {
		t.Fatalf("S3OpTimeout: %v", err)
	}
	if got.String() != "7s" {
		t.Errorf("OpTimeout=%v want 7s", got)
	}
}

func TestLoadConfigS3PathStyle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		want  bool
	}{
		{name: "absent defaults on", want: true},
		{name: "explicit on", field: "  path_style: true\n", want: true},
		{name: "explicit off", field: "  path_style: false\n", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeYAML(t, "backend: s3\ns3:\n  endpoint: https://objects.example.com\n  bucket: b\n"+tc.field)
			cfg, err := LoadConfig(path, false)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if got := cfg.S3PathStyle(); got != tc.want {
				t.Errorf("S3PathStyle=%v want %v", got, tc.want)
			}
		})
	}
}

func TestLoadConfigLegacyOBSNormalizesSilently(t *testing.T) {
	path := writeYAML(t, `
listen: 127.0.0.1:50051
backend: obs
obs:
  endpoint: https://legacy-objects.example.com
  region: legacy-region-1
  bucket: legacy-bucket
  path_style: false
`)
	var (
		cfg     *Config
		loadErr error
	)
	stderr := captureStderr(t, func() {
		cfg, loadErr = LoadConfig(path, true)
	})
	if loadErr != nil {
		t.Fatalf("LoadConfig: %v", loadErr)
	}
	if stderr != "" {
		t.Fatalf("legacy config emitted stderr: %q", stderr)
	}
	if cfg.Backend != "s3" {
		t.Errorf("Backend=%q want s3", cfg.Backend)
	}
	if cfg.S3 == nil || cfg.S3.Bucket != "legacy-bucket" {
		t.Fatalf("S3 not normalised: %+v", cfg.S3)
	}
	if cfg.S3PathStyle() {
		t.Fatal("legacy path_style=false was not preserved during normalisation")
	}
	if cfg.OBS != nil {
		t.Fatalf("OBS must be nil after normalisation: %+v", cfg.OBS)
	}

	out, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal normalised config: %v", err)
	}
	if strings.Contains(string(out), "obs:") || !strings.Contains(string(out), "s3:") {
		t.Fatalf("normalised YAML exposed legacy section:\n%s", out)
	}
}

func TestLoadConfigS3EnvExpansion(t *testing.T) {
	t.Setenv("TEST_S3_ENDPOINT", "https://env-objects.example.com")
	t.Setenv("TEST_S3_REGION", "env-region-1")
	t.Setenv("TEST_S3_BUCKET", "env-bucket")
	t.Setenv("TEST_S3_AK", "from-env-AK")
	t.Setenv("TEST_S3_SK", "from-env-SK")
	path := writeYAML(t, `
backend: s3
s3:
  endpoint: ${TEST_S3_ENDPOINT}
  region: ${TEST_S3_REGION}
  bucket: ${TEST_S3_BUCKET}
  access_key: ${TEST_S3_AK}
  secret_key: ${TEST_S3_SK}
`)
	cfg, err := LoadConfig(path, false)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.S3.Endpoint != "https://env-objects.example.com" ||
		cfg.S3.Region != "env-region-1" || cfg.S3.Bucket != "env-bucket" ||
		cfg.S3.AccessKey != "from-env-AK" || cfg.S3.SecretKey != "from-env-SK" {
		t.Fatalf("environment expansion failed: %+v", cfg.S3)
	}
}

func TestLoadConfigS3RegionBehavior(t *testing.T) {
	t.Run("explicit value preserved", func(t *testing.T) {
		path := writeYAML(t, `
backend: s3
s3:
  endpoint: https://objects.internal.example
  region: signing-region-9
  bucket: b
`)
		cfg, err := LoadConfig(path, false)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if cfg.S3.Region != "signing-region-9" {
			t.Errorf("Region=%q", cfg.S3.Region)
		}
	})

	t.Run("empty value defaults without endpoint inference", func(t *testing.T) {
		path := writeYAML(t, `
backend: s3
s3:
  endpoint: https://objects.vendor-region-7.example
  bucket: b
`)
		cfg, err := LoadConfig(path, false)
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if cfg.S3.Region != "us-east-1" {
			t.Errorf("Region=%q want us-east-1", cfg.S3.Region)
		}
	})
}

func TestLoadConfigS3EmptyCredentialsUseDefaultChain(t *testing.T) {
	path := writeYAML(t, `
backend: s3
s3:
  endpoint: https://objects.example.com
  bucket: b
`)
	cfg, err := LoadConfig(path, false)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.S3.AccessKey != "" || cfg.S3.SecretKey != "" {
		t.Fatalf("expected empty static credentials, got %q/%q", cfg.S3.AccessKey, cfg.S3.SecretKey)
	}
}

func TestLoadConfigRejectsIncompleteStaticCredentials(t *testing.T) {
	for _, tc := range []struct {
		name        string
		credentials string
	}{
		{name: "access key only", credentials: "  access_key: AK\n"},
		{name: "secret key only", credentials: "  secret_key: SK\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeYAML(t, "backend: s3\ns3:\n  endpoint: https://objects.example.com\n  bucket: b\n"+tc.credentials)
			_, err := LoadConfig(path, false)
			if err == nil || !strings.Contains(err.Error(), "must be set together") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestLoadConfigS3InsecureSkipVerify(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want func(*Config) bool
	}{
		{"absent defaults to strict",
			"backend: s3\ns3:\n  endpoint: https://objects.example.com\n  bucket: b\n",
			func(c *Config) bool { return !c.S3.InsecureSkipVerify }},
		{"explicit true preserved",
			"backend: s3\ns3:\n  endpoint: http://objects.example.com\n  bucket: b\n  insecure_skip_verify: true\n",
			func(c *Config) bool { return c.S3.InsecureSkipVerify }},
		{"legacy obs keeps the flag through normalisation",
			"backend: obs\nobs:\n  endpoint: https://legacy-objects.example.com\n  bucket: legacy-bucket\n  insecure_skip_verify: true\n",
			func(c *Config) bool { return c.Backend == "s3" && c.S3 != nil && c.S3.InsecureSkipVerify }},
		{"generations.s3 honored independently",
			"backend: fs\nfs:\n  root: /objects\ngenerations:\n  refresh_interval: 5s\n  s3:\n    endpoint: https://meta.example\n    bucket: metadata\n    key: prod/generations\n    insecure_skip_verify: true\n",
			func(c *Config) bool {
				return c.Generations != nil && c.Generations.S3 != nil && c.Generations.S3.InsecureSkipVerify
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfig(writeYAML(t, tc.body), false)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if !tc.want(cfg) {
				t.Fatalf("unexpected resolved config: %+v", cfg)
			}
		})
	}
}

func TestLoadConfigRejectsMismatchedSections(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "s3 backend with obs section",
			body: "backend: s3\nobs:\n",
			want: `"obs" config cannot be used with backend=s3`,
		},
		{
			name: "legacy backend with s3 section",
			body: "backend: obs\ns3:\n",
			want: `"s3" config cannot be used with backend=obs`,
		},
		{
			name: "both object storage sections",
			body: "backend: s3\ns3:\nobs:\n",
			want: "cannot both be set",
		},
		{
			name: "fs backend with s3 section",
			body: "backend: fs\nfs:\n  root: /tmp/store\ns3:\n",
			want: "object storage config is not valid for backend=fs",
		},
		{
			name: "fs backend with legacy section",
			body: "backend: fs\nfs:\n  root: /tmp/store\nobs:\n",
			want: "object storage config is not valid for backend=fs",
		},
		{
			name: "s3 backend with fs section",
			body: "backend: s3\nfs:\n  root: /tmp/store\ns3:\n  endpoint: https://objects.example.com\n  bucket: b\n",
			want: `"fs" config cannot be used with backend=s3`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeYAML(t, tc.body), false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestLoadConfigRequiredFields(t *testing.T) {
	cases := []struct {
		name          string
		body          string
		requireListen bool
		want          string
	}{
		{name: "serve missing listen", body: "backend: fs\nfs:\n  root: /tmp/x\n", requireListen: true, want: "listen is required"},
		{name: "missing backend", body: "listen: 127.0.0.1:1\n", requireListen: true, want: "backend is required"},
		{name: "fs missing section", body: "backend: fs\n", want: "fs config is required"},
		{name: "fs missing root", body: "backend: fs\nfs: {}\n", want: "fs.root is required"},
		{name: "s3 missing section", body: "backend: s3\n", want: "s3 config is required"},
		{name: "legacy missing section", body: "backend: obs\n", want: "obs config is required"},
		{name: "s3 missing bucket", body: "backend: s3\ns3:\n  endpoint: https://objects.example.com\n", want: "s3.bucket is required"},
		{name: "s3 missing endpoint", body: "backend: s3\ns3:\n  bucket: b\n", want: "s3.endpoint is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig(writeYAML(t, tc.body), tc.requireListen)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestLoadConfigUnknownBackendAdvertisesOnlyFSAndS3(t *testing.T) {
	_, err := LoadConfig(writeYAML(t, "backend: alien\n"), false)
	if err == nil {
		t.Fatal("expected unknown-backend error")
	}
	if !strings.Contains(err.Error(), "(want fs|s3)") || strings.Contains(err.Error(), "fs|obs") {
		t.Fatalf("unexpected supported-backend list: %v", err)
	}
}

func TestLoadConfigAdminCommandsSkipListen(t *testing.T) {
	cfg, err := LoadConfig(writeYAML(t, "backend: fs\nfs:\n  root: /var/lib/store\n"), false)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listen != "" {
		t.Errorf("Listen=%q want empty", cfg.Listen)
	}
}

func TestStoreConfigTemplateUsesS3(t *testing.T) {
	if !strings.Contains(storeConfigTemplate, "# Backend: fs | s3.") ||
		!strings.Contains(storeConfigTemplate, "# S3-compatible object storage backend") ||
		!strings.Contains(storeConfigTemplate, "# s3:") ||
		!strings.Contains(storeConfigTemplate, "#   path_style: true") {
		t.Fatalf("generated template does not describe s3 backend:\n%s", storeConfigTemplate)
	}
	if !strings.Contains(storeConfigTemplate, "#   insecure_skip_verify: false") ||
		!strings.Contains(storeConfigTemplate, "Skip TLS certificate verification") {
		t.Fatalf("generated template does not document the insecure_skip_verify option:\n%s", storeConfigTemplate)
	}
	if strings.Contains(storeConfigTemplate, "backend: obs") || strings.Contains(storeConfigTemplate, "# obs:") {
		t.Fatalf("generated template exposes legacy configuration:\n%s", storeConfigTemplate)
	}
	if !strings.Contains(storeConfigTemplate, "direct_io: false") ||
		!strings.Contains(storeConfigTemplate, "# generations:") {
		t.Fatalf("generated template omits direct I/O or generation sources:\n%s", storeConfigTemplate)
	}
	path := writeYAML(t, storeConfigTemplate)
	if _, err := LoadConfig(path, true); err != nil {
		t.Fatalf("generated template is not valid config: %v", err)
	}
}
