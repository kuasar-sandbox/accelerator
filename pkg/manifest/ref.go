package manifest

import (
	"fmt"
	"path/filepath"
	"strings"
)

const (
	RefSchemeManifest = "manifest"
	RefSchemeFile     = "file"
)

// Ref is the canonical logical reference shared by manifest-backed and local
// file-backed data. Digest is the lowercase hex payload of an optional
// @sha256 qualifier; Location is the logical name from an optional @location
// qualifier. Host paths for locations are deliberately not part of Ref.
type Ref struct {
	Scheme   string
	Path     string
	Digest   string
	Location string
}

// ParseRef parses one manifest or file reference. A manifest reference always
// contains exactly one content key. File qualifiers, when present, have the
// fixed order @sha256 then @location.
func ParseRef(raw string) (Ref, error) {
	var ref Ref
	switch {
	case strings.HasPrefix(raw, RefSchemeManifest+"://"):
		key, err := ParseHexKey(strings.TrimPrefix(raw, RefSchemeManifest+"://"))
		if err != nil {
			return ref, fmt.Errorf("manifest ref: %w", err)
		}
		ref = Ref{Scheme: RefSchemeManifest, Path: HexKey(key)}
	case strings.HasPrefix(raw, RefSchemeFile+"://"):
		value := strings.TrimPrefix(raw, RefSchemeFile+"://")
		if i := strings.LastIndex(value, "@location:"); i >= 0 {
			ref.Location = value[i+len("@location:"):]
			if ref.Location == "" {
				return ref, fmt.Errorf("file ref: location name is empty")
			}
			value = value[:i]
		}
		if i := strings.LastIndex(value, "@sha256:"); i >= 0 {
			ref.Digest = value[i+len("@sha256:"):]
			if ref.Digest == "" {
				return ref, fmt.Errorf("file ref: sha256 digest is empty")
			}
			value = value[:i]
		}
		if strings.Contains(value, "@sha256:") || strings.Contains(value, "@location:") {
			return ref, fmt.Errorf("file ref: duplicate or out-of-order qualifier")
		}
		ref.Scheme = RefSchemeFile
		ref.Path = value
	default:
		return ref, fmt.Errorf("ref: unsupported scheme in %q", raw)
	}
	if err := ref.Validate(); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

// Validate checks the canonical reference fields without consulting host
// configuration or the filesystem.
func (r Ref) Validate() error {
	switch r.Scheme {
	case RefSchemeManifest:
		if r.Digest != "" || r.Location != "" {
			return fmt.Errorf("manifest ref: qualifiers are not allowed")
		}
		if _, err := ParseHexKey(r.Path); err != nil {
			return fmt.Errorf("manifest ref: %w", err)
		}
	case RefSchemeFile:
		if r.Path == "" {
			return fmt.Errorf("file ref: path is required")
		}
		if r.Digest != "" {
			if len(r.Digest) != 64 || strings.ToLower(r.Digest) != r.Digest {
				return fmt.Errorf("file ref: sha256 digest must be 64 lowercase hex characters")
			}
			if _, err := ParseHexKey(r.Digest); err != nil {
				return fmt.Errorf("file ref: sha256 digest: %w", err)
			}
		}
		if r.Location != "" {
			if !validLocationName(r.Location) {
				return fmt.Errorf("file ref: invalid location name %q", r.Location)
			}
			if r.Path == "." || r.Path == ".." || filepath.Base(r.Path) != r.Path || strings.ContainsAny(r.Path, `/\\`) {
				return fmt.Errorf("file ref: located path %q must be a basename", r.Path)
			}
		}
	default:
		return fmt.Errorf("ref: unsupported scheme %q", r.Scheme)
	}
	return nil
}

// String formats a validated Ref in canonical qualifier order. Callers that
// construct Ref values directly should call Validate before persisting it.
func (r Ref) String() string {
	value := r.Scheme + "://" + r.Path
	if r.Digest != "" {
		value += "@sha256:" + r.Digest
	}
	if r.Location != "" {
		value += "@location:" + r.Location
	}
	return value
}

// Portable reports whether the reference contains no node-local file path.
func (r Ref) Portable() bool {
	return r.Scheme == RefSchemeManifest || (r.Scheme == RefSchemeFile && r.Location != "")
}

func validLocationName(name string) bool {
	if name == "" || !asciiAlphaNumeric(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !asciiAlphaNumeric(c) && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func asciiAlphaNumeric(c byte) bool {
	return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}
