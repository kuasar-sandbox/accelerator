package main

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store/obs"
)

// Config is the YAML configuration shared by every store-ctl
// subcommand. Two backend flavours, selected by `backend:`:
//
//	backend: fs   — local filesystem
//	backend: obs  — S3-compatible object storage
//
// Generation lifecycle is owned by the admin subcommands
// (init / rollout / purge), NOT by the yaml. `serve` reads whatever
// the backend reports as the active generation; refusing to start
// when the meta object is absent so misconfiguration surfaces fast.
type Config struct {
	// Listen is the gRPC server address (host:port). Required by
	// `serve`; ignored by other subcommands.
	Listen string `yaml:"listen"`

	// Backend selects the backing storage. "fs" or "obs". Required.
	Backend string `yaml:"backend"`

	// FS / OBS hold backend-specific config. Exactly one is read,
	// keyed by Backend.
	FS  FSConfig  `yaml:"fs"`
	OBS OBSConfig `yaml:"obs"`
}

// FSConfig is the filesystem backend's parameter set.
type FSConfig struct {
	// Root is the on-disk directory that holds the store. Must be
	// initialised by `store-ctl init` before `serve` will start.
	Root string `yaml:"root"`

	// VerifyContentKey: re-hash on Put. Default true.
	VerifyContentKey *bool `yaml:"verify_content_key"`
}

// OBSConfig is the OBS backend's parameter set. AccessKey / SecretKey
// support `${VAR}` shell-style env-var expansion so the yaml can be
// committed without secrets.
type OBSConfig struct {
	// Endpoint is the OBS S3-compatible URL, e.g.
	// "https://obs.cn-north-4.example.com".
	Endpoint string `yaml:"endpoint"`

	// Region is the OBS region (used in signatures), e.g.
	// "cn-north-4". Auto-derived from Endpoint when blank.
	Region string `yaml:"region"`

	// Bucket is the OBS bucket name. Required.
	Bucket string `yaml:"bucket"`

	// Prefix is an optional in-bucket key prefix (multi-tenant
	// namespacing). Trailing slash optional; normalised internally.
	Prefix string `yaml:"prefix"`

	// AccessKey / SecretKey: static AK/SK with `${VAR}` expansion.
	// Empty falls through to ~/.obsconfig auto-discovery, then to
	// the AWS SDK's default credentials chain.
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`

	// VerifyContentKey: re-hash on Put. Default true.
	VerifyContentKey *bool `yaml:"verify_content_key"`

	// MaxInflight bounds concurrent S3 calls. Default 64.
	MaxInflight int `yaml:"max_inflight"`

	// OpTimeout is the per-call wall-clock budget (Go duration
	// string, e.g. "10s"). Empty/absent = 0 = no per-op deadline.
	OpTimeout string `yaml:"op_timeout"`

	// MaxObjectSize bounds Get response bytes. Default 16 MiB.
	MaxObjectSize int64 `yaml:"max_object_size_bytes"`
}

// envVarPattern matches `${VAR_NAME}` with alphanumeric / underscore
// names. Used by expandEnv on selected string fields.
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces every `${VAR}` in s with os.Getenv(VAR). An
// undefined env var maps to empty string (callers must validate
// required fields after expansion).
func expandEnv(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		name := match[2 : len(match)-1]
		return os.Getenv(name)
	})
}

// LoadConfig reads, validates, and normalises a Config from path.
// `requireListen` is set by `serve` (which needs to bind a port);
// admin subcommands clear it because they never bind anything.
func LoadConfig(path string, requireListen bool) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("store-ctl: read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("store-ctl: parse config %s: %w", path, err)
	}
	if requireListen && cfg.Listen == "" {
		return nil, fmt.Errorf("store-ctl: listen is required")
	}
	if cfg.Backend == "" {
		return nil, fmt.Errorf("store-ctl: backend is required (fs or obs)")
	}

	switch cfg.Backend {
	case "fs":
		if cfg.FS.Root == "" {
			return nil, fmt.Errorf("store-ctl: fs.root is required for backend=fs")
		}
	case "obs":
		// Expand env vars first — yaml-explicit ${VAR} wins over
		// auto-discovery.
		cfg.OBS.AccessKey = expandEnv(cfg.OBS.AccessKey)
		cfg.OBS.SecretKey = expandEnv(cfg.OBS.SecretKey)

		// Auto-discover from ~/.obsconfig for any field still empty.
		// Precedence:
		//
		//   1. yaml literal value
		//   2. yaml ${VAR} env expansion
		//   3. ~/.obsconfig JSON
		//   4. AWS SDK default credentials chain (handled in s3client.New
		//      when AccessKey is "")
		if cfg.OBS.Endpoint == "" || cfg.OBS.AccessKey == "" || cfg.OBS.SecretKey == "" {
			disc, err := obs.DiscoverObsConfig()
			if err != nil {
				return nil, fmt.Errorf("store-ctl: ~/.obsconfig: %w", err)
			}
			if disc != nil {
				if cfg.OBS.Endpoint == "" {
					cfg.OBS.Endpoint = disc.Endpoint
				}
				if cfg.OBS.AccessKey == "" {
					cfg.OBS.AccessKey = disc.AccessKey
				}
				if cfg.OBS.SecretKey == "" {
					cfg.OBS.SecretKey = disc.SecretKey
				}
			}
		}

		// Auto-derive region from endpoint when not given.
		if cfg.OBS.Region == "" {
			cfg.OBS.Region = obs.RegionFromEndpoint(cfg.OBS.Endpoint)
		}

		if cfg.OBS.Endpoint == "" {
			return nil, fmt.Errorf("store-ctl: obs.endpoint is required for backend=obs (set yaml or ~/.obsconfig)")
		}
		if cfg.OBS.Bucket == "" {
			return nil, fmt.Errorf("store-ctl: obs.bucket is required for backend=obs")
		}
	default:
		return nil, fmt.Errorf("store-ctl: unknown backend %q (want fs|obs)", cfg.Backend)
	}
	return &cfg, nil
}

// VerifyKey returns the resolved verify-content-key value for the
// active backend. Both flavours default to true.
func (c *Config) VerifyKey() bool {
	switch c.Backend {
	case "obs":
		if c.OBS.VerifyContentKey == nil {
			return true
		}
		return *c.OBS.VerifyContentKey
	default: // "fs"
		if c.FS.VerifyContentKey == nil {
			return true
		}
		return *c.FS.VerifyContentKey
	}
}

// OBSOpTimeout returns the parsed OBS op-timeout. Empty/absent = 0 =
// no per-op deadline: an obs op is bounded only by the caller's
// context, never an arbitrary number. Operators opt into a finite
// budget by setting obs.op_timeout explicitly.
func (c *Config) OBSOpTimeout() (time.Duration, error) {
	if c.OBS.OpTimeout == "" {
		return 0, nil
	}
	return time.ParseDuration(c.OBS.OpTimeout)
}
