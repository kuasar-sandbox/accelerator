package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
)

// AESChunkEncryptor implements ChunkEncryptor using AES-256-CTR.
type AESChunkEncryptor struct{}

// Encrypt encrypts plaintext with AES-256-CTR.
// Format: [0x01] + AES-256-CTR(key, iv=0, plaintext)
// Hash is SHA256 of the full ciphertext including flag byte.
func (e *AESChunkEncryptor) Encrypt(key [32]byte, plaintext []byte) ([]byte, [32]byte) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic(fmt.Sprintf("crypto: aes.NewCipher: %v", err))
	}

	iv := make([]byte, aes.BlockSize) // zero IV
	stream := cipher.NewCTR(block, iv)

	ciphertext := make([]byte, 1+len(plaintext))
	ciphertext[0] = FlagAES
	stream.XORKeyStream(ciphertext[1:], plaintext)

	hash := sha256.Sum256(ciphertext)
	return ciphertext, hash
}

// Decrypt decrypts AES-256-CTR ciphertext into a fresh allocation.
// Expects format: [0x01] + encrypted_data. For hot paths that own the
// ciphertext buffer, prefer DecryptInPlace to avoid the per-call alloc.
func (e *AESChunkEncryptor) Decrypt(key [32]byte, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < 1 {
		return nil, errors.New("crypto: ciphertext too short")
	}
	if ciphertext[0] != FlagAES {
		return nil, fmt.Errorf("crypto: unexpected flag byte 0x%02x, want 0x%02x", ciphertext[0], FlagAES)
	}

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}

	iv := make([]byte, aes.BlockSize) // zero IV
	stream := cipher.NewCTR(block, iv)

	plaintext := make([]byte, len(ciphertext)-1)
	stream.XORKeyStream(plaintext, ciphertext[1:])
	return plaintext, nil
}

// DecryptInPlace XORs AES-256-CTR keystream over buf[1:] in place and
// returns buf[1:] as the plaintext. The flag byte at buf[0] is checked
// then ignored (caller's storage may keep it for re-encryption or pool
// return). XORKeyStream(dst, src) with dst==src is well-defined: each
// block reads src[i] then writes dst[i] sequentially.
func (e *AESChunkEncryptor) DecryptInPlace(key [32]byte, buf []byte) ([]byte, error) {
	if len(buf) < 1 {
		return nil, errors.New("crypto: ciphertext too short")
	}
	if buf[0] != FlagAES {
		return nil, fmt.Errorf("crypto: unexpected flag byte 0x%02x, want 0x%02x", buf[0], FlagAES)
	}

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}

	iv := make([]byte, aes.BlockSize) // zero IV
	stream := cipher.NewCTR(block, iv)

	plain := buf[1:]
	stream.XORKeyStream(plain, plain)
	return plain, nil
}

// AESKeyTableEncryptor implements KeyTableEncryptor using AES-256-GCM.
type AESKeyTableEncryptor struct{}

const gcmNonceSize = 12

// Seal encrypts the key table with AES-256-GCM.
// Format: [0x01] + nonce(12) + encrypted_data + tag(16)
//
// The 12-byte nonce is synthesized deterministically from
// (customerKey, aad, keys) via HMAC-SHA256, NOT drawn at random.
// Rationale: the manifest blob is published into a content-addressed
// store keyed by SHA256(blob); a random nonce would make the same
// logical manifest hash to a different key on every re-upload,
// breaking the project's convergent-encryption stance (chunk addresses
// are already a pure function of content). With this synthesis,
// re-uploading the same image under the same customerKey produces a
// byte-identical manifest and therefore the same manifest key.
//
// GCM safety: a (key, nonce) pair must never be reused under different
// plaintexts. HMAC-SHA256 truncated to 96 bits over (aad, keys) gives
// distinct nonces for distinct plaintexts with overwhelming probability
// (collision birthday bound ~2^48, far outside any realistic upload
// volume per customer key). Identical plaintexts under the same key
// reuse the nonce — and produce identical ciphertexts, which is the
// desired idempotency, not a leak: an attacker watching the store
// already sees the chunk-level dedup, so manifest-level dedup adds no
// new information channel.
//
// Domain-separation prefix prevents this HMAC use of the customer key
// from colliding with FakeKeyTableEncryptor's HMAC tag use of the
// same key.
func (e *AESKeyTableEncryptor) Seal(customerKey [32]byte, keys []byte, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(customerKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: cipher.NewGCM: %w", err)
	}

	mac := hmac.New(sha256.New, customerKey[:])
	mac.Write([]byte("ktable-nonce/v1\x00"))
	mac.Write(aad)
	mac.Write(keys)
	nonce := mac.Sum(nil)[:gcmNonceSize]

	encrypted := gcm.Seal(nil, nonce, keys, aad)

	// [flag(1)] + [nonce(12)] + [encrypted + tag]
	sealed := make([]byte, 0, 1+gcmNonceSize+len(encrypted))
	sealed = append(sealed, FlagAES)
	sealed = append(sealed, nonce...)
	sealed = append(sealed, encrypted...)
	return sealed, nil
}

// Unseal decrypts the key table with AES-256-GCM.
// Expects format: [0x01] + nonce(12) + encrypted_data + tag(16)
func (e *AESKeyTableEncryptor) Unseal(customerKey [32]byte, sealed []byte, aad []byte) ([]byte, error) {
	// minimum: flag(1) + nonce(12) + tag(16) = 29
	if len(sealed) < 1+gcmNonceSize+16 {
		return nil, errors.New("crypto: sealed data too short")
	}
	if sealed[0] != FlagAES {
		return nil, fmt.Errorf("crypto: unexpected flag byte 0x%02x, want 0x%02x", sealed[0], FlagAES)
	}

	block, err := aes.NewCipher(customerKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: cipher.NewGCM: %w", err)
	}

	nonce := sealed[1 : 1+gcmNonceSize]
	ciphertext := sealed[1+gcmNonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm.Open: %w", err)
	}
	return plaintext, nil
}
