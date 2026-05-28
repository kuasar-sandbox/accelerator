package manifest

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ErrConfigNotProvided is returned by LoadConfig when neither the
// explicit flag nor the environment variable points at a file.
// Callers decide whether this is fatal (e.g. manifest-ctl, which
// always needs a config) or a soft "skip manifest features" signal
// (e.g. sandbox-ctl with file://-only resources).
var ErrConfigNotProvided = errors.New("manifest: no config path provided via flag or env")

// LoadConfig parses a YAML file into a *Config. The path is chosen
// by, in priority order, flagPath, then the environment variable
// envName. Both empty → ErrConfigNotProvided.
//
// Auto-discovery (./accelerator.yaml, ~/.config/...) is intentionally
// not supported: an unconfigured invocation should fail loudly rather
// than silently fall back to defaults.
func LoadConfig(flagPath, envName string) (*Config, error) {
	path := flagPath
	if path == "" && envName != "" {
		path = os.Getenv(envName)
	}
	if path == "" {
		return nil, ErrConfigNotProvided
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: read %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("manifest: parse %s: %w", path, err)
	}
	return &cfg, nil
}
