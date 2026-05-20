package manifest

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/fullof-work/mass-sandbox/pkg/manifest/fetch"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/ingest"
	"github.com/fullof-work/mass-sandbox/pkg/store"
)

// ParseHexKey decodes a 64-character hex string into a store.ContentKey.
// Used by CLI tools and any caller that holds a manifest key on the
// wire (logs, flags, env). Empty input or wrong-length input is
// reported as an error rather than truncated.
func ParseHexKey(s string) (store.ContentKey, error) {
	var k store.ContentKey
	if len(s) != 64 {
		return k, fmt.Errorf("manifest: hex key must be 64 chars, got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return k, fmt.Errorf("manifest: hex key: %w", err)
	}
	copy(k[:], b)
	return k, nil
}

// ParseKeyRef decodes a manifest content key that may carry an optional
// "manifest://" scheme prefix (the form printed in URIs / passed on
// CLIs and env). "manifest://<hex>" and bare "<hex>" are equivalent;
// validation is delegated to ParseHexKey.
func ParseKeyRef(s string) (store.ContentKey, error) {
	return ParseHexKey(strings.TrimPrefix(s, "manifest://"))
}

// HexKey is the inverse of ParseHexKey — formats a ContentKey as 64
// lowercase hex chars suitable for CLI output, logs, or env passing.
func HexKey(k store.ContentKey) string {
	return hex.EncodeToString(k[:])
}

// CustomerKey decodes the YAML's manifest.key into a 32-byte array.
// Returns an error if the field is empty or malformed.
func (c *Config) CustomerKey() ([32]byte, error) {
	var k [32]byte
	if c.Manifest.Key == "" {
		return k, fmt.Errorf("manifest: customer key required (set manifest.key)")
	}
	raw, err := hex.DecodeString(c.Manifest.Key)
	if err != nil {
		return k, fmt.Errorf("manifest: decode key: %w", err)
	}
	if len(raw) != 32 {
		return k, fmt.Errorf("manifest: key must be 32 bytes, got %d", len(raw))
	}
	copy(k[:], raw)
	return k, nil
}

// IngestKeyFunc returns an ingest.CustomerKeyFunc that resolves the
// key lazily from the YAML's manifest.key field. Equivalent to
// passing func() ([32]byte, error) { return c.CustomerKey() }.
func (c *Config) IngestKeyFunc() ingest.CustomerKeyFunc { return c.CustomerKey }

// FetchKeyFunc returns a fetch.CustomerKeyFunc that resolves the key
// lazily from the YAML's manifest.key field.
func (c *Config) FetchKeyFunc() fetch.CustomerKeyFunc { return c.CustomerKey }
