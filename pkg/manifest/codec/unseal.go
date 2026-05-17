package codec

import (
	"fmt"

	"github.com/fullof-work/mass-sandbox/pkg/manifest/crypto"
)

// UnsealKeys decrypts the manifest's sealed key table and expands it
// into a sparse N-length slice where N = m.ChunkCount(). Each non-zero
// entry receives its decrypted 32-byte key; IsZero entries get the
// zero key (they are never read by the fetch path because zero chunks
// are reconstructed by the reader, not decrypted from storage).
//
// Validates that the sealed table contains exactly 32 bytes per
// non-zero chunk; mismatch is reported as an error rather than a
// truncation, since both directions of mismatch indicate a corrupt
// or replayed manifest.
//
// Replaces the previously duplicated unseal helpers in
// pkg/sandbox/disks.go and cmd/manifest-ctl/main.go.
func UnsealKeys(m *Manifest, sealedKT []byte, customerKey [32]byte, dec crypto.Decryptor) ([][32]byte, error) {
	flat, err := dec.UnsealKeyTable(customerKey, sealedKT, BuildAAD(m))
	if err != nil {
		return nil, fmt.Errorf("unseal key table: %w", err)
	}
	count := int(m.ChunkCount())
	nonZero := 0
	for _, e := range m.Entries {
		if !e.IsZero {
			nonZero++
		}
	}
	if len(flat) != nonZero*32 {
		return nil, fmt.Errorf("key table size mismatch: got %d bytes, want %d (non-zero chunks: %d)",
			len(flat), nonZero*32, nonZero)
	}
	keys := make([][32]byte, count)
	pos := 0
	for i, e := range m.Entries {
		if e.IsZero {
			continue
		}
		copy(keys[i][:], flat[pos*32:(pos+1)*32])
		pos++
	}
	return keys, nil
}
