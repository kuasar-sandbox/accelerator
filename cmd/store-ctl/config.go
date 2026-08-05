package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultStatsInterval is the base period for the adaptive stats line when
// stats_interval is unset.
const defaultStatsInterval = 30 * time.Second

const defaultS3Region = "us-east-1"

// StatsIntervalDur parses stats_interval. Empty/absent → 30s (on by default);
// "0"/"off"/"none" → 0 (disabled); any valid Go duration overrides; an invalid
// value falls back to the default rather than silently disabling output.
func (c *Config) StatsIntervalDur() time.Duration {
	switch strings.ToLower(strings.TrimSpace(c.StatsInterval)) {
	case "":
		return defaultStatsInterval
	case "0", "off", "none", "disabled":
		return 0
	}
	d, err := time.ParseDuration(c.StatsInterval)
	if err != nil || d <= 0 {
		return defaultStatsInterval
	}
	return d
}

// Config is the YAML configuration shared by every store-ctl
// subcommand. Two backend flavours, selected by `backend:`:
//
//	backend: fs   — local filesystem
//	backend: s3   — S3-compatible object storage
//
// Generation lifecycle is owned by the admin subcommands
// (init / rollout / purge), NOT by the yaml. `serve` reads whatever
// the backend reports as the active generation; refusing to start
// when the meta object is absent so misconfiguration surfaces fast.
type Config struct {
	// Listen is the gRPC server address (host:port). Required by
	// `serve`; ignored by other subcommands.
	Listen string `yaml:"listen"`

	// Backend selects the backing storage. "fs" or "s3". Required.
	Backend string `yaml:"backend"`

	// StatsInterval is the base period for the adaptive stats line the
	// daemon prints to stderr (silent when there was no traffic in the
	// window). Empty/absent → 30s (on by default); "0"/"off" disables it.
	StatsInterval string `yaml:"stats_interval"`

	// CacheListen, when non-empty, starts an embedded read-only cache wire
	// server on this address (host:port or unix:/path) alongside the store
	// gRPC. It serves cache-protocol object reads (chunk/manifest/blob)
	// straight from the backend, so cache clients can read store content
	// without a separate cache-ctl. Writes are rejected (use the store
	// gRPC). Empty = disabled.
	CacheListen string `yaml:"cache_listen"`

	// FS / S3 hold backend-specific config. Exactly one must match
	// Backend after legacy input has been normalised.
	FS *FSConfig `yaml:"fs,omitempty"`
	S3 *S3Config `yaml:"s3,omitempty"`

	// OBS exists only to detect and silently normalise the complete
	// legacy backend=obs + obs: input pair. It is nil after LoadConfig.
	OBS *S3Config `yaml:"obs,omitempty"`

	fsSectionSet  bool
	s3SectionSet  bool
	obsSectionSet bool
}

// UnmarshalYAML records mapping-key presence independently of decoded pointer
// values. This distinguishes an omitted section from an explicitly present
// empty/null section, which is required to reject every mixed config form.
func (c *Config) UnmarshalYAML(node *yaml.Node) error {
	type plainConfig Config
	var decoded plainConfig
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*c = Config(decoded)
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		switch node.Content[i].Value {
		case "fs":
			c.fsSectionSet = true
		case "s3":
			c.s3SectionSet = true
		case "obs":
			c.obsSectionSet = true
		}
	}
	return nil
}

// FSConfig is the filesystem backend's parameter set.
type FSConfig struct {
	// Root is the on-disk directory that holds the store. Must be
	// initialised by `store-ctl init` before `serve` will start.
	Root string `yaml:"root"`

	// VerifyContentKey: re-hash on Put. Default true.
	VerifyContentKey *bool `yaml:"verify_content_key"`
}

// S3Config is the S3-compatible backend's parameter set. String fields
// support `${VAR}` shell-style env-var expansion so the yaml can be
// committed without secrets.
type S3Config struct {
	// Endpoint is the S3-compatible endpoint URL. Required.
	Endpoint string `yaml:"endpoint"`

	// Region is the signing region. Empty defaults to us-east-1.
	Region string `yaml:"region"`

	// Bucket is the target bucket name. Required.
	Bucket string `yaml:"bucket"`

	// Prefix is an optional in-bucket key prefix (multi-tenant
	// namespacing). Trailing slash optional; normalised internally.
	Prefix string `yaml:"prefix"`

	// AccessKey / SecretKey are optional static credentials. When both
	// are empty, the AWS SDK default credential provider chain is used.
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

func (c *S3Config) expandEnv() {
	c.Endpoint = expandEnv(c.Endpoint)
	c.Region = expandEnv(c.Region)
	c.Bucket = expandEnv(c.Bucket)
	c.Prefix = expandEnv(c.Prefix)
	c.AccessKey = expandEnv(c.AccessKey)
	c.SecretKey = expandEnv(c.SecretKey)
	c.OpTimeout = expandEnv(c.OpTimeout)
}

// normalizeBackend validates that the selected backend and config section
// match. The only accepted legacy form is the complete backend=obs + obs:
// pair, which is immediately converted to backend=s3 + S3.
func (c *Config) normalizeBackend() error {
	hasFS := c.fsSectionSet || c.FS != nil
	hasS3 := c.s3SectionSet || c.S3 != nil
	hasOBS := c.obsSectionSet || c.OBS != nil
	if hasS3 && hasOBS {
		return fmt.Errorf(`store-ctl: "s3" and "obs" config sections cannot both be set`)
	}

	switch c.Backend {
	case "fs":
		if hasS3 || hasOBS {
			return fmt.Errorf("store-ctl: object storage config is not valid for backend=fs")
		}
		if !hasFS || c.FS == nil {
			return fmt.Errorf("store-ctl: fs config is required for backend=fs")
		}
	case "s3":
		if hasOBS {
			return fmt.Errorf(`store-ctl: "obs" config cannot be used with backend=s3`)
		}
		if hasFS {
			return fmt.Errorf(`store-ctl: "fs" config cannot be used with backend=s3`)
		}
		if !hasS3 || c.S3 == nil {
			return fmt.Errorf("store-ctl: s3 config is required for backend=s3")
		}
	case "obs":
		if hasS3 {
			return fmt.Errorf(`store-ctl: "s3" config cannot be used with backend=obs`)
		}
		if hasFS {
			return fmt.Errorf(`store-ctl: "fs" config cannot be used with backend=obs`)
		}
		if !hasOBS || c.OBS == nil {
			return fmt.Errorf("store-ctl: obs config is required for backend=obs")
		}
		c.Backend = "s3"
		c.S3 = c.OBS
		c.OBS = nil
		c.s3SectionSet = true
		c.obsSectionSet = false
	default:
		return fmt.Errorf("store-ctl: unknown backend %q (want fs|s3)", c.Backend)
	}
	return nil
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
		return nil, fmt.Errorf("store-ctl: backend is required (want fs|s3)")
	}
	if err := cfg.normalizeBackend(); err != nil {
		return nil, err
	}

	switch cfg.Backend {
	case "fs":
		if cfg.FS.Root == "" {
			return nil, fmt.Errorf("store-ctl: fs.root is required for backend=fs")
		}
	case "s3":
		cfg.S3.expandEnv()
		if cfg.S3.Region == "" {
			cfg.S3.Region = defaultS3Region
		}
		if cfg.S3.Endpoint == "" {
			return nil, fmt.Errorf("store-ctl: s3.endpoint is required for backend=s3")
		}
		if cfg.S3.Bucket == "" {
			return nil, fmt.Errorf("store-ctl: s3.bucket is required for backend=s3")
		}
		if (cfg.S3.AccessKey == "") != (cfg.S3.SecretKey == "") {
			return nil, fmt.Errorf("store-ctl: s3.access_key and s3.secret_key must be set together")
		}
	}
	return &cfg, nil
}

// VerifyKey returns the resolved verify-content-key value for the
// active backend. Both flavours default to true.
func (c *Config) VerifyKey() bool {
	switch c.Backend {
	case "s3":
		if c.S3.VerifyContentKey == nil {
			return true
		}
		return *c.S3.VerifyContentKey
	default: // "fs"
		if c.FS.VerifyContentKey == nil {
			return true
		}
		return *c.FS.VerifyContentKey
	}
}

// S3OpTimeout returns the parsed S3 op-timeout. Empty/absent = 0 =
// no per-op deadline: an S3 op is bounded only by the caller's
// context, never an arbitrary number. Operators opt into a finite
// budget by setting s3.op_timeout explicitly.
func (c *Config) S3OpTimeout() (time.Duration, error) {
	if c.S3.OpTimeout == "" {
		return 0, nil
	}
	return time.ParseDuration(c.S3.OpTimeout)
}
