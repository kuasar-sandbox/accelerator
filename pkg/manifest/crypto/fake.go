package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
)

const hmacTagSize = 32

// FakeChunkEncryptor implements ChunkEncryptor without real encryption.
// Data is stored in plaintext with an HMAC tag for integrity verification.
// Useful as a performance baseline to isolate encryption overhead.
type FakeChunkEncryptor struct{}

// Encrypt produces tagged plaintext.
// Format: [0x00] + HMAC-SHA256(key, plaintext)[:32] + plaintext
// Hash is SHA256 of the full output.
func (e *FakeChunkEncryptor) Encrypt(key [32]byte, plaintext []byte) ([]byte, [32]byte) {
	mac := hmac.New(sha256.New, key[:])
	mac.Write(plaintext)
	tag := mac.Sum(nil)

	out := make([]byte, 0, 1+hmacTagSize+len(plaintext))
	out = append(out, FlagFake)
	out = append(out, tag[:hmacTagSize]...)
	out = append(out, plaintext...)

	hash := sha256.Sum256(out)
	return out, hash
}

// Decrypt verifies the HMAC tag and returns the plaintext.
// Expects format: [0x00] + HMAC tag(32) + plaintext
func (e *FakeChunkEncryptor) Decrypt(key [32]byte, ciphertext []byte) ([]byte, error) {
	plain, err := e.DecryptInPlace(key, ciphertext)
	if err != nil {
		return nil, err
	}
	// Decrypt's contract returns plaintext that outlives the input buffer;
	// copy the in-place slice so the caller can free ciphertext.
	out := make([]byte, len(plain))
	copy(out, plain)
	return out, nil
}

// DecryptInPlace verifies the HMAC tag and returns buf[1+hmacTagSize:]
// as the plaintext. Fake mode stores plaintext verbatim, so there is
// no XOR step — just tag validation and a slice header. No allocation.
func (e *FakeChunkEncryptor) DecryptInPlace(key [32]byte, buf []byte) ([]byte, error) {
	// minimum: flag(1) + tag(32) = 33
	if len(buf) < 1+hmacTagSize {
		return nil, errors.New("crypto: fake ciphertext too short")
	}
	if buf[0] != FlagFake {
		return nil, fmt.Errorf("crypto: unexpected flag byte 0x%02x, want 0x%02x", buf[0], FlagFake)
	}

	tag := buf[1 : 1+hmacTagSize]
	plaintext := buf[1+hmacTagSize:]

	mac := hmac.New(sha256.New, key[:])
	mac.Write(plaintext)
	expected := mac.Sum(nil)

	if !hmac.Equal(tag, expected[:hmacTagSize]) {
		return nil, errors.New("crypto: fake HMAC verification failed")
	}
	return plaintext, nil
}

// FakeKeyTableEncryptor implements KeyTableEncryptor without real encryption.
// Data is stored in plaintext with an HMAC tag for integrity verification.
type FakeKeyTableEncryptor struct{}

// Seal produces tagged plaintext for the key table.
// Format: [0x00] + HMAC-SHA256(customerKey, aad || keys)[:32] + keys
func (e *FakeKeyTableEncryptor) Seal(customerKey [32]byte, keys []byte, aad []byte) ([]byte, error) {
	mac := hmac.New(sha256.New, customerKey[:])
	mac.Write(aad)
	mac.Write(keys)
	tag := mac.Sum(nil)

	sealed := make([]byte, 0, 1+hmacTagSize+len(keys))
	sealed = append(sealed, FlagFake)
	sealed = append(sealed, tag[:hmacTagSize]...)
	sealed = append(sealed, keys...)
	return sealed, nil
}

// Unseal verifies the HMAC tag and returns the plaintext keys.
// Expects format: [0x00] + HMAC tag(32) + keys
func (e *FakeKeyTableEncryptor) Unseal(customerKey [32]byte, sealed []byte, aad []byte) ([]byte, error) {
	// minimum: flag(1) + tag(32) = 33
	if len(sealed) < 1+hmacTagSize {
		return nil, errors.New("crypto: fake sealed data too short")
	}
	if sealed[0] != FlagFake {
		return nil, fmt.Errorf("crypto: unexpected flag byte 0x%02x, want 0x%02x", sealed[0], FlagFake)
	}

	tag := sealed[1 : 1+hmacTagSize]
	keys := sealed[1+hmacTagSize:]

	mac := hmac.New(sha256.New, customerKey[:])
	mac.Write(aad)
	mac.Write(keys)
	expected := mac.Sum(nil)

	if !hmac.Equal(tag, expected[:hmacTagSize]) {
		return nil, errors.New("crypto: fake HMAC verification failed")
	}
	return keys, nil
}
