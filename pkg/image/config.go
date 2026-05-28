package image

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ConfigFileName is the name of the config entry inside the appended
// ZIP — published so external tools (`unzip -p file.erofs config.json`)
// can be predictable.
const ConfigFileName = "config.json"

// RuntimeConfig is the runtime-relevant subset of an OCI image config
// that flatten-ctl appends to its EROFS output. The field selection
// targets two goals:
//
//   - everything an upstream container runtime / orchestrator needs
//     to actually start the container (User, Env, Entrypoint, ...);
//   - byte-stable, deterministic output so the same source image
//     produces the same manifest content (no timestamps, no build
//     history, no layer digests).
//
// Architecture and Os are passed through transparently — flatten-ctl
// is a content-agnostic byte-level converter and does NOT validate
// either field. Platform support decisions (amd64 vs arm64, linux vs
// other) belong to the orchestrator that schedules the resulting
// image, not to this tool.
type RuntimeConfig struct {
	// Pass-through platform identifiers. Not omitempty — emit even
	// when empty so downstream observers can see source-image issues
	// rather than silently inheriting a default.
	Architecture string `json:"Architecture"`
	Os           string `json:"Os"`

	User       string   `json:"User,omitempty"`
	Env        []string `json:"Env,omitempty"`
	Entrypoint []string `json:"Entrypoint,omitempty"`
	Cmd        []string `json:"Cmd,omitempty"`
	WorkingDir string   `json:"WorkingDir,omitempty"`

	ExposedPorts map[string]struct{} `json:"ExposedPorts,omitempty"`
	Volumes      map[string]struct{} `json:"Volumes,omitempty"`
	StopSignal   string              `json:"StopSignal,omitempty"`
	Labels       map[string]string   `json:"Labels,omitempty"`
	Healthcheck  *Healthcheck        `json:"Healthcheck,omitempty"`
}

// Healthcheck mirrors OCI image-config Healthcheck. Durations are
// nanoseconds (OCI native); we don't translate to "30s" strings to
// avoid format drift across libraries.
type Healthcheck struct {
	Test        []string `json:"Test,omitempty"`
	Interval    int64    `json:"Interval,omitempty"`
	Timeout     int64    `json:"Timeout,omitempty"`
	StartPeriod int64    `json:"StartPeriod,omitempty"`
	Retries     int      `json:"Retries,omitempty"`
}

// ociImageConfig is a minimal projection of the OCI image-config
// JSON used only for parsing. We deliberately list the source fields
// we care about; unlisted fields (created, author, history,
// rootfs.diff_ids, comment, OnBuild, ArgsEscaped, Domainname,
// Hostname, AttachStdin/Stdout/Stderr, Tty, OpenStdin, StdinOnce,
// MacAddress, ...) are silently dropped by encoding/json.
type ociImageConfig struct {
	Architecture string `json:"architecture"`
	Os           string `json:"os"`
	Config       struct {
		User         string              `json:"User"`
		Env          []string            `json:"Env"`
		Entrypoint   []string            `json:"Entrypoint"`
		Cmd          []string            `json:"Cmd"`
		WorkingDir   string              `json:"WorkingDir"`
		ExposedPorts map[string]struct{} `json:"ExposedPorts"`
		Volumes      map[string]struct{} `json:"Volumes"`
		StopSignal   string              `json:"StopSignal"`
		Labels       map[string]string   `json:"Labels"`
		Healthcheck  *Healthcheck        `json:"Healthcheck"`
	} `json:"config"`
}

// ExtractRuntimeConfig reads the docker-archive's image config JSON
// (configPath is relative to archiveDir, sourced from manifest.json's
// "Config" field) and projects it into a RuntimeConfig.
//
// Architecture / Os are passed through transparently with no
// validation. Unknown source fields are dropped.
func ExtractRuntimeConfig(archiveDir, configPath string) (*RuntimeConfig, error) {
	data, err := os.ReadFile(filepath.Join(archiveDir, configPath))
	if err != nil {
		return nil, fmt.Errorf("read image config %q: %w", configPath, err)
	}
	var src ociImageConfig
	if err := json.Unmarshal(data, &src); err != nil {
		return nil, fmt.Errorf("parse image config %q: %w", configPath, err)
	}
	rc := &RuntimeConfig{
		Architecture: src.Architecture,
		Os:           src.Os,
		User:         src.Config.User,
		Env:          src.Config.Env,
		Entrypoint:   src.Config.Entrypoint,
		Cmd:          src.Config.Cmd,
		WorkingDir:   src.Config.WorkingDir,
		ExposedPorts: src.Config.ExposedPorts,
		Volumes:      src.Config.Volumes,
		StopSignal:   src.Config.StopSignal,
		Labels:       src.Config.Labels,
	}
	// Healthcheck: only attach if the source carries any signal.
	// A nil pointer (omitempty) keeps the JSON tidy.
	if h := src.Config.Healthcheck; h != nil && (len(h.Test) > 0 || h.Interval != 0 || h.Timeout != 0 || h.StartPeriod != 0 || h.Retries != 0) {
		hc := *h
		rc.Healthcheck = &hc
	}
	return rc, nil
}

// MarshalDeterministic returns byte-stable JSON of the config. Go's
// stdlib encoding/json sorts map keys alphabetically, slice order is
// preserved (semantically required for Env/Cmd/Entrypoint), and our
// omitempty annotations drop zero-valued fields for a tidy output.
// Two RuntimeConfig structs with equal fields produce byte-identical
// outputs.
func (c *RuntimeConfig) MarshalDeterministic() ([]byte, error) {
	return json.Marshal(c)
}
