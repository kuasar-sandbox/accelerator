package crypto

import (
	"crypto/sha256"
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
	// Encrypt derives the convergent key from (salt, plaintext) and encrypts
	// under it. Returns the ciphertext (with prefix flag byte), the ciphertext
	// hash, and the derived key (to record in the manifest key table).
	// Derivation is owned by the encryptor so callers cannot supply a
	// non-convergent key — see DeriveKey and the AES zero-IV invariant.
	Encrypt(salt [32]byte, plaintext []byte) (ciphertext []byte, hash [32]byte, key [32]byte)

	// Decrypt decrypts ciphertext (with prefix flag byte) and returns
	// freshly-allocated plaintext. Use this when the caller does not own
	// the ciphertext storage or needs an independent plaintext lifetime.
	Decrypt(key [32]byte, ciphertext []byte) ([]byte, error)

	// DecryptInPlace decrypts buf in place and returns the plaintext slice
	// aliased into the same underlying storage as buf. The caller must
	// keep buf alive (or its backing buffer in a pool) for as long as the
	// returned plaintext is referenced.
	//
	// Validates the flag byte and XORs the AES-256-CTR keystream over
	// buf[1:], returning buf[1:] (no allocation). The key comes from the
	// unsealed manifest key table (decryption cannot re-derive it).
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

// FlagAES is the format-identification flag byte prefixed to every chunk
// ciphertext (ciphertext[0]); the chunk hash and manifest addressing cover it.
const FlagAES byte = 0x01
