package manifest

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxRuntimeBasenameBytes is the filename limit of a runtime declaration.
	MaxRuntimeBasenameBytes = 255
	// MaxRuntimeAnnotationBytes includes @runtime:, :digest: and the content digest.
	MaxRuntimeAnnotationBytes = len("@runtime:") + MaxRuntimeBasenameBytes + len(":digest:") + 64
)

// RuntimeIdentity declares the guest runtime required by a snapshot. It is
// scheduling metadata, not part of the snapshot's content or carrier identity.
// The zero value represents a legacy reference with no runtime declaration.
type RuntimeIdentity struct {
	Basename string
	Digest   string
}

func (r RuntimeIdentity) IsZero() bool { return r == (RuntimeIdentity{}) }

// Validate checks a declaration without reading a runtime artifact.
func (r RuntimeIdentity) Validate() error {
	if r.IsZero() {
		return nil
	}
	if r.Basename == "" || r.Basename == "." || r.Basename == ".." ||
		len(r.Basename) > MaxRuntimeBasenameBytes || strings.ContainsAny(r.Basename, "/\\") ||
		!utf8.ValidString(r.Basename) || strings.IndexFunc(r.Basename, unicode.IsControl) >= 0 ||
		!strings.HasSuffix(r.Basename, ".bundle") || r.Basename == ".bundle" {
		return fmt.Errorf("runtime ref: runtime name must be a UTF-8 .bundle basename of at most %d bytes without control characters", MaxRuntimeBasenameBytes)
	}
	for _, marker := range []string{"@runtime", "@digest:", "@hmac:", "@manifest:", "@location:", "@sha256:"} {
		if strings.Contains(r.Basename, marker) {
			return fmt.Errorf("runtime ref: reserved qualifier in runtime basename")
		}
	}
	if len(r.Digest) != 64 || strings.ToLower(r.Digest) != r.Digest {
		return fmt.Errorf("runtime ref: digest must be 64 lowercase hex characters")
	}
	if _, err := ParseHexKey(r.Digest); err != nil {
		return fmt.Errorf("runtime ref: digest: %w", err)
	}
	return nil
}

func parseRuntimeAnnotation(raw string) (string, RuntimeIdentity, error) {
	var runtime RuntimeIdentity
	i := strings.Index(raw, "@runtime")
	if i < 0 {
		return raw, runtime, nil
	}
	value, ok := strings.CutPrefix(raw[i:], "@runtime:")
	if !ok || strings.Contains(value, "@runtime") {
		return "", runtime, fmt.Errorf("runtime ref: malformed or duplicate annotation")
	}
	separator := strings.LastIndex(value, ":digest:")
	if separator < 0 {
		return "", runtime, fmt.Errorf("runtime ref: basename and digest are required")
	}
	runtime = RuntimeIdentity{Basename: value[:separator], Digest: value[separator+len(":digest:"):]}
	if runtime.Basename == "" || runtime.Digest == "" {
		return "", RuntimeIdentity{}, fmt.Errorf("runtime ref: basename and digest are required")
	}
	if err := runtime.Validate(); err != nil {
		return "", RuntimeIdentity{}, err
	}
	return raw[:i], runtime, nil
}
