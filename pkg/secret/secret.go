// Package secret resolves ordinary secret values and references to local Secret
// plugins. It returns bytes; consumers retain responsibility for key length,
// format, and lifetime. Resolving a reference never falls back to a native value.
package secret

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Env configures process-local plugins as whitespace-separated declarations:
// name::/absolute/socket:cache_ttl=30s:cache_size=4k.
const Env = "KUASAR_SECRET_PLUGINS"

const (
	defaultTTL  = 30 * time.Second
	defaultSize = 4096
)

// Decoder interprets a native, non-reference value. The default is hex decoding.
// os.ReadFile is suitable for private-key paths; Literal preserves credentials.
type Decoder func(string) ([]byte, error)

// Literal returns the bytes of a static credential without decoding its content.
func Literal(text string) ([]byte, error) { return []byte(text), nil }

// IsReference reports whether text uses the reserved plugin-reference namespace.
// It deliberately does not validate or resolve text on a forwarding path.
func IsReference(text string) bool { return strings.HasPrefix(text, "@") }

// Client owns independent caches and HTTP transports for its configured plugins.
// Configuration is immutable. Clients and their Resolve calls are concurrency-safe.
type Client struct{ plugins map[string]*plugin }

var processClient = sync.OnceValues(func() (*Client, error) { return New(os.Getenv(Env)) })

// Resolve uses the process-local client, initialized from Env at the first plugin
// reference. Native values do not initialize the client. The environment must be
// installed before that first reference; subsequent changes do not reload it.
// Omit native (or pass nil) for hex decoding. At most one decoder is accepted.
func Resolve(ctx context.Context, text string, native ...Decoder) ([]byte, error) {
	decoder, err := nativeDecoder(native)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !IsReference(text) {
		return decoder(text)
	}
	c, err := processClient()
	if err != nil {
		return nil, err
	}
	return c.resolveReference(ctx, text)
}

// New parses a fixed plugin configuration. An empty configuration enables no
// plugins. It does not connect to sockets until a reference is resolved.
func New(config string) (*Client, error) {
	c := &Client{plugins: make(map[string]*plugin)}
	for _, declaration := range strings.Fields(config) {
		name, rest, ok := strings.Cut(declaration, "::")
		if !ok || !validName(name) {
			return nil, errors.New("secret: invalid plugin declaration")
		}
		if _, ok := c.plugins[name]; ok {
			return nil, fmt.Errorf("secret: duplicate plugin %q", name)
		}
		parts := strings.Split(rest, ":")
		path := parts[0]
		if !filepath.IsAbs(path) || strings.ContainsFunc(path, unicode.IsSpace) {
			return nil, fmt.Errorf("secret: plugin %q requires an absolute socket path without whitespace or colons", name)
		}
		ttl, size := defaultTTL, defaultSize
		seen := make(map[string]bool)
		for _, param := range parts[1:] {
			key, value, ok := strings.Cut(param, "=")
			if !ok || seen[key] {
				return nil, fmt.Errorf("secret: invalid or duplicate parameter for plugin %q", name)
			}
			seen[key] = true
			switch key {
			case "cache_ttl":
				var err error
				ttl, err = time.ParseDuration(value)
				if err != nil || ttl < 0 {
					return nil, fmt.Errorf("secret: invalid cache_ttl for plugin %q", name)
				}
			case "cache_size":
				var err error
				size, err = parseSize(value)
				if err != nil {
					return nil, fmt.Errorf("secret: invalid cache_size for plugin %q", name)
				}
			default:
				return nil, fmt.Errorf("secret: unknown parameter for plugin %q", name)
			}
		}
		c.plugins[name] = newPlugin(path, ttl, size)
	}
	return c, nil
}

// Resolve uses this client's fixed configuration. Native values retain the
// supplied decoder's semantics; the default decoder is hex.DecodeString.
func (c *Client) Resolve(ctx context.Context, text string, native ...Decoder) ([]byte, error) {
	decoder, err := nativeDecoder(native)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !IsReference(text) {
		return decoder(text)
	}
	return c.resolveReference(ctx, text)
}

func (c *Client) resolveReference(ctx context.Context, text string) ([]byte, error) {
	name, key, ok := strings.Cut(strings.TrimPrefix(text, "@"), "/")
	if !ok || !validName(name) || key == "" || strings.ContainsRune(key, ':') || strings.ContainsFunc(key, unicode.IsSpace) {
		return nil, errors.New("secret: invalid plugin reference")
	}
	if c == nil || c.plugins[name] == nil {
		return nil, fmt.Errorf("secret: plugin %q is not configured", name)
	}
	b, err := c.plugins[name].resolve(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("secret: plugin %q: %w", name, err)
	}
	return b, nil
}

// CloseIdleConnections releases idle plugin connections without affecting active
// requests or cached values. The process-local client is owned by the process.
func (c *Client) CloseIdleConnections() {
	for _, p := range c.plugins {
		p.http.CloseIdleConnections()
	}
}

func nativeDecoder(native []Decoder) (Decoder, error) {
	if len(native) > 1 {
		return nil, errors.New("secret: at most one native decoder is supported")
	}
	if len(native) == 1 && native[0] != nil {
		return native[0], nil
	}
	return hex.DecodeString, nil
}

func validName(name string) bool {
	for i, ch := range []byte(name) {
		if ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' {
			continue
		}
		if i > 0 && (ch >= '0' && ch <= '9' || ch == '-') {
			continue
		}
		return false
	}
	return name != ""
}

func parseSize(value string) (int, error) {
	multiplier := uint64(1)
	if strings.HasSuffix(value, "k") {
		multiplier, value = 1024, strings.TrimSuffix(value, "k")
	}
	if value == "" {
		return 0, errors.New("empty size")
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid size")
		}
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n > uint64(^uint(0)>>1)/multiplier {
		return 0, errors.New("size overflow")
	}
	return int(n * multiplier), nil
}
