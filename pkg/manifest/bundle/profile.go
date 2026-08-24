// Package bundle implements the standard ZIP64 container profile for local
// Manifest snapshots. It stores exact physical Manifest and Chunk objects and
// never provides object-level fallback to another source.
package bundle

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const (
	admissionPrefix = "admission/"
	manifestPrefix  = "manifest/"
	chunkPrefix     = "chunk/"

	maxBundleEntries = 100_000
)

var (
	ErrClosed     = errors.New("manifest bundle: closed")
	ErrIncomplete = errors.New("manifest bundle: incomplete Manifest Chunk closure")
)

func admissionName(admission store.WriteAdmission) string {
	return admissionPrefix + string(admission.Generation) + "/" + hex.EncodeToString(admission.Salt[:])
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
	parts := strings.Split(name, "/")
	if len(parts) != 3 || parts[0] != strings.TrimSuffix(admissionPrefix, "/") {
		return admission, fmt.Errorf("manifest bundle: malformed admission entry %q", name)
	}
	admission.Generation = store.Generation(parts[1])
	if err := store.ValidateGeneration(admission.Generation); err != nil {
		return admission, fmt.Errorf("manifest bundle: admission generation: %w", err)
	}
	if len(parts[2]) != hex.EncodedLen(len(admission.Salt)) || parts[2] != strings.ToLower(parts[2]) {
		return admission, fmt.Errorf("manifest bundle: admission salt must be %d lowercase hexadecimal characters", hex.EncodedLen(len(admission.Salt)))
	}
	decoded, err := hex.DecodeString(parts[2])
	if err != nil {
		return admission, fmt.Errorf("manifest bundle: admission salt: %w", err)
	}
	copy(admission.Salt[:], decoded)
	return admission, nil
}
