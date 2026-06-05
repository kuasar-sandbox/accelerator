// Package remote pulls OCI images straight from a registry and adapts them
// into a flatten.Source, so flatten-ctl can flatten `repo:tag` without an
// intermediate `docker save`. It is the single home of the
// go-containerregistry dependency — pkg/flatten and pkg/image stay
// stdlib-only (pkg/image is imported by sandbox-runtime).
//
// Responsibilities: reference resolution + platform selection (pull.go),
// env-based credentials (auth.go), a shared OCI-layout blob cache with
// atomic writes and size-cap LRU eviction (cache.go, gc.go), and the OCI
// Referrers write-back / idempotent-skip (referrer.go).
package remote

import (
	"fmt"
	"os"
	"runtime"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/sandbox-builder/internal/util"
)

const (
	defaultCacheDir = "/var/cache/flatten-ctl"
	defaultMaxSize  = 10 << 30 // 10 GiB
	defaultPullJobs = 4
)

// Config is the YAML schema for flatten-ctl's remote-pull, blob-cache, and
// Referrers behaviour. It mirrors the manifest-config convention: load via
// LoadConfig from --config or $FLATTEN_CONFIG. Secrets never live here
// — registry credentials come from FLATTEN_REGISTRY_* env, the HMAC key from
// the manifest customer key. A missing file is fine: defaults apply.
// RefererArtifactType is the fixed OCI artifact type of the flatten-manifest
// referrer. It is a constant (not configurable) so FindReferrer/PutReferrer
// always agree across tools and versions.
const RefererArtifactType = "application/vnd.kuasar.flatten-manifest.v1"

type Config struct {
	// TmpDir is the parent of per-run scratch directories (flatten output, the
	// ephemeral blob cache). Empty → $TMPDIR or /tmp.
	TmpDir string `yaml:"tmpdir"`
	// Platform selects which manifest to pull from a multi-arch index, as
	// "os/arch[/variant]". Empty follows the host: linux/<runtime.GOARCH>.
	Platform string `yaml:"platform"`
	// Insecure allows plain-HTTP / skip-TLS registries (dev / private).
	Insecure bool `yaml:"insecure"`
	// PullJobs bounds concurrent layer downloads. <1 → defaultPullJobs.
	PullJobs int           `yaml:"pull_jobs"`
	Cache    CacheConfig   `yaml:"cache"`
	Referer  RefererConfig `yaml:"referer"`

	platform v1.Platform // parsed from Platform in normalize
	maxSize  int64       // parsed from Cache.MaxSize (bytes; 0 = unlimited)
}

// CacheConfig configures the shared OCI-layout blob cache.
type CacheConfig struct {
	// Dir is the cache root (a standard OCI image layout). A shared path
	// dedups across tenants; a per-task path isolates them. Empty → default.
	Dir string `yaml:"dir"`
	// MaxSize caps the cache (human size, e.g. "10GiB"). "" → default,
	// "0" → unlimited (prune only via `cache gc`).
	MaxSize string `yaml:"max_size"`
}

// RefererConfig configures the OCI Referrers write-back (--with-referer). The
// artifact type is fixed (RefererArtifactType), not configurable.
type RefererConfig struct {
	// Enabled turns on the idempotent OCI-Referrers flow by default (equivalent
	// to passing --with-referer); the flag can still force it on per-run.
	Enabled bool `yaml:"enabled"`
	// Desc is the public owner descriptor appended to the owner annotation.
	Desc string `yaml:"desc"`
	// Key is the HMAC message paired with the customer key; defaults to Desc.
	Key string `yaml:"key"`
	// Validity, when set (a Go duration), writes an expiry into valid_at.
	Validity string `yaml:"validity"`
}

// LoadConfig reads the remote config from flagPath, or $envName when flagPath
// is empty. A missing path yields an all-defaults Config (the common case for
// a plain `export <ref>`); a present-but-unreadable/invalid file errors.
func LoadConfig(flagPath, envName string) (*Config, error) {
	path := flagPath
	if path == "" {
		path = os.Getenv(envName)
	}
	c := &Config{}
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("remote: read config %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, c); err != nil {
			return nil, fmt.Errorf("remote: parse config %q: %w", path, err)
		}
	}
	if err := c.normalize(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) normalize() error {
	if c.PullJobs < 1 {
		c.PullJobs = defaultPullJobs
	}
	// Cache is opt-in: an empty cache.dir means "no persistent cache". OpenCache
	// then uses an ephemeral scratch dir under tmpdir, removed after the run.
	switch c.Cache.MaxSize {
	case "":
		c.maxSize = defaultMaxSize
	case "0":
		c.maxSize = 0
	default:
		v, err := util.ParseSize(c.Cache.MaxSize)
		if err != nil {
			return fmt.Errorf("remote: cache.max_size: %w", err)
		}
		c.maxSize = int64(v)
	}
	if c.Platform == "" {
		c.platform = v1.Platform{OS: "linux", Architecture: runtime.GOARCH}
	} else {
		p, err := v1.ParsePlatform(c.Platform)
		if err != nil {
			return fmt.Errorf("remote: platform %q: %w", c.Platform, err)
		}
		c.platform = *p
	}
	if c.Referer.Key == "" {
		c.Referer.Key = c.Referer.Desc
	}
	return nil
}

// CacheDir returns the configured cache root ("" = ephemeral).
func (c *Config) CacheDir() string { return c.Cache.Dir }

// MaxCacheBytes returns the resolved cache size cap (0 = unlimited).
func (c *Config) MaxCacheBytes() int64 { return c.maxSize }

// SetPlatform overrides the configured platform (the --platform flag) and
// re-parses it. Empty is a no-op (keeps the configured/host default).
func (c *Config) SetPlatform(p string) error {
	if p == "" {
		return nil
	}
	pl, err := v1.ParsePlatform(p)
	if err != nil {
		return fmt.Errorf("remote: platform %q: %w", p, err)
	}
	c.Platform, c.platform = p, *pl
	return nil
}

// OpenCache opens the blob cache. With cache.dir set it is persistent and the
// returned cleanup is a no-op; empty (the default) yields an ephemeral cache
// under tmpdir that cleanup removes. Callers must defer cleanup().
func (c *Config) OpenCache() (*Cache, func(), error) {
	dir := c.Cache.Dir
	cleanup := func() {}
	if dir == "" {
		d, err := os.MkdirTemp(c.TmpDir, "flatten-cache-")
		if err != nil {
			return nil, nil, fmt.Errorf("remote: ephemeral cache: %w", err)
		}
		dir, cleanup = d, func() { os.RemoveAll(d) }
	}
	cache, err := OpenCache(dir, c.maxSize)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return cache, cleanup, nil
}
