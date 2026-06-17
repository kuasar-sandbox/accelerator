package obs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
)

// ObsConfigFile is the JSON shape of `~/.obsconfig` as written by
// the official obsutil / obs-browser tooling. We use it as a
// last-ditch source for endpoint + AK + SK when those fields are
// missing from the store-ctl yaml — convenient on dev workstations
// where engineers already have obsutil configured, awkward to
// require yet-another secret duplication.
//
// The "security-token" field is captured for future support of STS
// session credentials but currently not propagated into the AWS
// SDK static-credentials path.
type ObsConfigFile struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access-key"`
	SecretKey string `json:"secret-key"`
	Token     string `json:"security-token,omitempty"`
}

// DiscoverObsConfig returns the parsed ~/.obsconfig if present,
// or (nil, nil) when absent. Any other error (permission, malformed
// JSON) is returned to the caller — silent failure on a malformed
// file would mask a real misconfiguration.
//
// The file is only consulted when the corresponding yaml fields are
// blank; if the user explicitly set values in store-ctl.yaml those
// always win.
func DiscoverObsConfig() (*ObsConfigFile, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("discover ~/.obsconfig: home: %w", err)
	}
	return loadObsConfigFile(filepath.Join(home, ".obsconfig"))
}

// loadObsConfigFile is the path-explicit form, exposed for tests
// that don't want to mutate $HOME.
func loadObsConfigFile(path string) (*ObsConfigFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// Absent file is the common case on production hosts —
			// not an error.
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var c ObsConfigFile
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &c, nil
}

// RegionFromEndpoint is the exported form of regionFromEndpoint
// used by store-ctl config to auto-fill obs.region when yaml leaves
// it blank. Kept separate from the unexported helper so internal
// callers don't accidentally bake the regex into hot paths.
func RegionFromEndpoint(endpoint string) string {
	return regionFromEndpoint(endpoint)
}

// regionFromEndpoint extracts the region name from an OBS-style
// endpoint of the canonical form
//
//	http(s)://obs.<region>.<flavour>.<rest>
//
// e.g.
//
//	https://obs.cn-north-4.example.com  → "cn-north-4"
//	http://obs.cn-north-7.example.com  → "cn-north-7"
//
// Returns "" when the endpoint doesn't match the pattern; the
// caller should require region explicitly in that case.
func regionFromEndpoint(endpoint string) string {
	m := endpointRegionRe.FindStringSubmatch(endpoint)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

var endpointRegionRe = regexp.MustCompile(`^https?://obs\.([a-z0-9-]+)\.[^/]+`)
