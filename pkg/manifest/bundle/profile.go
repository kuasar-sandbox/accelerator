// Package bundle implements the indexed standard-ZIP64 container profile for
// local Manifest snapshots. It stores exact physical Manifest and Chunk
// objects and never provides object-level fallback to another source.
package bundle

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const (
	refsName              = "bundle/refs"
	indexName             = "bundle/index"
	admissionPrefix       = "bundle/admission/"
	legacyAdmissionPrefix = "admission/"
	manifestPrefix        = "manifest/"
	chunkPrefix           = "chunk/"

	maxBundleEntries = 100_000
	maxBundleRefs    = 1_024
	maxRefsPayload   = 1 << 20

	// The metadata prefix is refs (at most 1 MiB), admission, and their
	// canonical Local File Headers. Keep a fixed bound before allocating the
	// single Open-time range buffer.
	maxMetadataPrefixBytes = maxRefsPayload + 1_024
)

var (
	ErrClosed     = errors.New("manifest bundle: closed")
	ErrIncomplete = errors.New("manifest bundle: incomplete Manifest Chunk closure")
)

func admissionName(admission store.WriteAdmission) string {
	return admissionPrefix + string(admission.Generation) + "/" + hex.EncodeToString(admission.Salt[:])
}

// EncodeRefs validates and encodes one immutable ordered Bundle search path.
// An empty list is represented by omitting bundle/refs, so it encodes to nil.
func EncodeRefs(refs []string) ([]byte, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if len(refs) > maxBundleRefs {
		return nil, fmt.Errorf("manifest bundle: %d refs exceed limit %d", len(refs), maxBundleRefs)
	}
	seen := make(map[string]struct{}, len(refs))
	var payload bytes.Buffer
	for index, ref := range refs {
		if err := validateBundleRef(ref); err != nil {
			return nil, fmt.Errorf("manifest bundle: ref[%d]: %w", index, err)
		}
		if _, duplicate := seen[ref]; duplicate {
			return nil, fmt.Errorf("manifest bundle: duplicate ref %q", ref)
		}
		seen[ref] = struct{}{}
		if payload.Len()+len(ref)+1 > maxRefsPayload {
			return nil, fmt.Errorf("manifest bundle: refs payload exceeds %d bytes", maxRefsPayload)
		}
		payload.WriteString(ref)
		payload.WriteByte('\n')
	}
	return payload.Bytes(), nil
}

// ParseRefs parses the canonical bundle/refs payload without sorting it.
func ParseRefs(payload []byte) ([]string, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("manifest bundle: refs payload must be non-empty")
	}
	if len(payload) > maxRefsPayload {
		return nil, fmt.Errorf("manifest bundle: refs payload exceeds %d bytes", maxRefsPayload)
	}
	if !utf8.Valid(payload) {
		return nil, fmt.Errorf("manifest bundle: refs payload is not valid UTF-8")
	}
	if bytes.Contains(payload, []byte{0xef, 0xbb, 0xbf}) {
		return nil, fmt.Errorf("manifest bundle: refs payload must not contain a BOM")
	}
	if bytes.Contains(payload, []byte{'\r'}) {
		return nil, fmt.Errorf("manifest bundle: refs payload must use LF line endings")
	}
	if payload[len(payload)-1] != '\n' {
		return nil, fmt.Errorf("manifest bundle: refs payload must end with LF")
	}
	lines := strings.Split(string(payload[:len(payload)-1]), "\n")
	if len(lines) > maxBundleRefs {
		return nil, fmt.Errorf("manifest bundle: %d refs exceed limit %d", len(lines), maxBundleRefs)
	}
	seen := make(map[string]struct{}, len(lines))
	refs := make([]string, 0, len(lines))
	for index, ref := range lines {
		if ref == "" {
			return nil, fmt.Errorf("manifest bundle: ref[%d] is empty", index)
		}
		if strings.TrimSpace(ref) != ref {
			return nil, fmt.Errorf("manifest bundle: ref[%d] has leading or trailing whitespace", index)
		}
		if strings.HasPrefix(ref, "#") {
			return nil, fmt.Errorf("manifest bundle: ref[%d] is a comment", index)
		}
		if err := validateBundleRef(ref); err != nil {
			return nil, fmt.Errorf("manifest bundle: ref[%d]: %w", index, err)
		}
		if _, duplicate := seen[ref]; duplicate {
			return nil, fmt.Errorf("manifest bundle: duplicate ref %q", ref)
		}
		seen[ref] = struct{}{}
		refs = append(refs, ref)
	}
	return refs, nil
}

func validateBundleRef(raw string) error {
	if !utf8.ValidString(raw) {
		return fmt.Errorf("Bundle ref is not valid UTF-8")
	}
	if strings.Contains(raw, "\ufeff") {
		return fmt.Errorf("Bundle ref must not contain a BOM")
	}
	if strings.TrimSpace(raw) != raw {
		return fmt.Errorf("Bundle ref has leading or trailing whitespace")
	}
	for _, character := range raw {
		if unicode.IsControl(character) {
			return fmt.Errorf("Bundle ref contains a control character")
		}
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return fmt.Errorf("invalid Bundle file ref %q: %w", raw, err)
	}
	if ref.String() != raw {
		return fmt.Errorf("non-canonical Bundle file ref %q", raw)
	}
	if ref.Scheme != manifest.RefSchemeFile {
		return fmt.Errorf("Bundle ref %q must use file://", raw)
	}
	if ref.DigestScheme != "" || ref.Digest != "" {
		return fmt.Errorf("Bundle ref %q must not contain an identity selector", raw)
	}
	if ref.Path == "." || ref.Path == ".." || strings.ContainsAny(ref.Path, `/\\`) {
		return fmt.Errorf("Bundle ref %q path must be a basename", raw)
	}
	if !strings.HasSuffix(ref.Path, ".bundle") || ref.Path == ".bundle" {
		return fmt.Errorf("Bundle ref %q path must name a .bundle file", raw)
	}
	return nil
}

func objectName(partition store.Partition, key store.ContentKey) (string, error) {
	switch partition {
	case store.PartitionManifest:
		return manifestPrefix + hex.EncodeToString(key[:]), nil
	case store.PartitionChunk:
		return chunkPrefix + hex.EncodeToString(key[:]), nil
	default:
		return "", fmt.Errorf("manifest bundle: unsupported partition %q", partition)
	}
}

func parseLowerHexKey(raw string) (store.ContentKey, error) {
	var key store.ContentKey
	if len(raw) != hex.EncodedLen(len(key)) || raw != strings.ToLower(raw) {
		return key, fmt.Errorf("want %d lowercase hexadecimal characters", hex.EncodedLen(len(key)))
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return key, err
	}
	copy(key[:], decoded)
	return key, nil
}

func parseAdmissionName(name string) (store.WriteAdmission, error) {
	var admission store.WriteAdmission
	if !strings.HasPrefix(name, admissionPrefix) {
		return admission, fmt.Errorf("manifest bundle: malformed admission entry %q", name)
	}
	parts := strings.Split(strings.TrimPrefix(name, admissionPrefix), "/")
	if len(parts) != 2 {
		return admission, fmt.Errorf("manifest bundle: malformed admission entry %q", name)
	}
	admission.Generation = store.Generation(parts[0])
	if err := store.ValidateGeneration(admission.Generation); err != nil {
		return admission, fmt.Errorf("manifest bundle: admission generation: %w", err)
	}
	if len(parts[1]) != hex.EncodedLen(len(admission.Salt)) || parts[1] != strings.ToLower(parts[1]) {
		return admission, fmt.Errorf("manifest bundle: admission salt must be %d lowercase hexadecimal characters", hex.EncodedLen(len(admission.Salt)))
	}
	decoded, err := hex.DecodeString(parts[1])
	if err != nil {
		return admission, fmt.Errorf("manifest bundle: admission salt: %w", err)
	}
	copy(admission.Salt[:], decoded)
	return admission, nil
}
