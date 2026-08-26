package crypto

import (
	"context"
	"crypto/sha256"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/internal/objectformat"
)

// DeriveKey derives a convergent encryption key from salt and plaintext.
// key = SHA256(salt || plaintext)
func DeriveKey(salt [32]byte, plaintext []byte) [32]byte {
	h := sha256.New()
	h.Write(salt[:])
	h.Write(plaintext)
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

// ChunkEncryptor encrypts/decrypts chunk data.
type ChunkEncryptor interface {
	// Mode returns the mode of the decryptor.
	Mode() string
	// EncryptChunk encrypts the plaintext chunk and returns the ciphertext.
	EncryptChunk(ctx context.Context, salt [32]byte, plaintext []byte) ([]byte, [32]byte, [32]byte, error)
	// DecryptChunk decrypts the ciphertext and returns the plaintext.
	DecryptChunkTo(ctx context.Context, key [32]byte, ciphertext, dst []byte) error
	// DecryptChunkRangeTo decrypts a range of the ciphertext and writes the plaintext to the destination.
	DecryptChunkRangeTo(ctx context.Context, key [32]byte, ciphertext []byte, plaintextSize, plaintextOffset int, dst []byte) error
}

// KeyTableEncryptor encrypts/decrypts the manifest key table.
type KeyTableEncryptor interface {
	// Seal encrypts the key table with the customer key.
	Seal(customerKey [32]byte, keys []byte, aad []byte) ([]byte, error)
	// Unseal decrypts the key table with the customer key.
	Unseal(customerKey [32]byte, sealed []byte, aad []byte) ([]byte, error)
}

// Chunk and key-table bytes live in separate physical formats. Their format
// constants intentionally do not share a generic flag name.
const (
	ChunkFormatAESRaw    byte = 0x01
	ChunkFormatAESSnappy byte = 0x02

	KeyTableFormatAESGCM byte = 0x01

	// MaxChunkDecodedSize is the fixed safety boundary for one logical
	// chunk. It is part of the canonical implementation policy, not config.
	MaxChunkDecodedSize = objectformat.MaxChunkDecodedSize
)

// compressionBeneficial is part of the canonical format. The division form
// of the 75% comparison avoids overflow even when called with adversarial
// uint64 values in boundary tests.
func compressionBeneficial(rawSize, compressedSize uint64) bool {
	return objectformat.CompressionBeneficial(rawSize, compressedSize)
}
