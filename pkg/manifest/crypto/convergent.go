package crypto

import (
	"crypto/sha256"
)

// DeriveSalt derives a 32-byte salt from a generation ID.
// salt = SHA256("accelerator-salt-v1" || generationID)
func DeriveSalt(generationID string) [32]byte {
	h := sha256.New()
	h.Write([]byte("accelerator-salt-v1"))
	h.Write([]byte(generationID))
	var salt [32]byte
	copy(salt[:], h.Sum(nil))
	return salt
}

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
	// Encrypt encrypts plaintext. Returns ciphertext (with prefix flag byte) and ciphertext hash.
	Encrypt(key [32]byte, plaintext []byte) (ciphertext []byte, hash [32]byte)

	// Decrypt decrypts ciphertext (with prefix flag byte) and returns
	// freshly-allocated plaintext. Use this when the caller does not own
	// the ciphertext storage or needs an independent plaintext lifetime.
	Decrypt(key [32]byte, ciphertext []byte) ([]byte, error)

	// DecryptInPlace decrypts buf in place and returns the plaintext slice
	// aliased into the same underlying storage as buf. The caller must
	// keep buf alive (or its backing buffer in a pool) for as long as the
	// returned plaintext is referenced.
	//
	// For AES-256-CTR: validates the flag byte and XORs the keystream
	// over buf[1:], returning buf[1:] (no allocation).
	//
	// For Fake (HMAC + plaintext): validates the tag and returns the
	// plaintext slice that already exists in buf — no XOR, no allocation.
	//
	// On error the buffer contents are unspecified; callers must discard.
	DecryptInPlace(key [32]byte, buf []byte) ([]byte, error)
}

// KeyTableEncryptor encrypts/decrypts the manifest key table.
type KeyTableEncryptor interface {
	// Seal encrypts the key table with the customer key.
	Seal(customerKey [32]byte, keys []byte, aad []byte) ([]byte, error)
	// Unseal decrypts the key table with the customer key.
	Unseal(customerKey [32]byte, sealed []byte, aad []byte) ([]byte, error)
}

// Flag bytes for ciphertext format identification
const (
	FlagAES  byte = 0x01
	FlagFake byte = 0x00
)
