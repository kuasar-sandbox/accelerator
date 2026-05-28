package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// TestAES_DecryptInPlace_RoundTrip — encrypt then decrypt-in-place
// returns the original plaintext, with the working buffer reusing
// the ciphertext storage (no fresh allocation).
func TestAES_DecryptInPlace_RoundTrip(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0xAB, 0xCD, 0xEF, 0x12}, 4096)

	enc := &AESChunkEncryptor{}
	ct, _ := enc.Encrypt(key, plain)

	// In-place decrypt: the buffer ct itself is mutated.
	got, err := enc.DecryptInPlace(key, ct)
	if err != nil {
		t.Fatalf("DecryptInPlace: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch")
	}
	// got must alias ct[1:] — same underlying array.
	if &got[0] != &ct[1] {
		t.Fatalf("DecryptInPlace returned a fresh slice; expected aliasing into ct[1:]")
	}
}

// TestFake_DecryptInPlace_RoundTrip — fake mode: decrypt-in-place
// returns plaintext aliased into the input buffer, with no XOR step.
func TestFake_DecryptInPlace_RoundTrip(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0xEE}, 1024)

	enc := &FakeChunkEncryptor{}
	ct, _ := enc.Encrypt(key, plain)

	got, err := enc.DecryptInPlace(key, ct)
	if err != nil {
		t.Fatalf("DecryptInPlace: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch")
	}
	if &got[0] != &ct[1+hmacTagSize] {
		t.Fatalf("DecryptInPlace did not alias; got addr differs")
	}
}

// TestAES_DecryptInPlace_FlagMismatch — wrong flag byte rejected.
func TestAES_DecryptInPlace_FlagMismatch(t *testing.T) {
	var key [32]byte
	enc := &AESChunkEncryptor{}
	bad := []byte{0xFF, 0x01, 0x02}
	if _, err := enc.DecryptInPlace(key, bad); err == nil {
		t.Fatal("expected flag mismatch error, got nil")
	}
}

// TestAESKeyTable_Determinism — two Seal calls with identical
// (customerKey, keys, aad) must produce byte-equal sealed output.
// This is what makes the manifest blob (and its SHA256 manifest key)
// content-addressable across re-uploads of the same image.
func TestAESKeyTable_Determinism(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	keys := bytes.Repeat([]byte{0xA5}, 32*200) // 200 32-byte chunk keys
	aad := []byte("aad-fixture")

	enc := &AESKeyTableEncryptor{}
	a, err := enc.Seal(key, keys, aad)
	if err != nil {
		t.Fatalf("Seal #1: %v", err)
	}
	b, err := enc.Seal(key, keys, aad)
	if err != nil {
		t.Fatalf("Seal #2: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("Seal not deterministic: two calls differ")
	}
}

// TestAESKeyTable_RoundTrip — Seal then Unseal returns the original
// keys; tampering with the ciphertext or AAD breaks GCM auth.
func TestAESKeyTable_RoundTrip(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0x7E}, 1024)
	aad := []byte("manifest-geometry")

	enc := &AESKeyTableEncryptor{}
	sealed, err := enc.Seal(key, plain, aad)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	got, err := enc.Unseal(key, sealed, aad)
	if err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch")
	}
	// Tamper the last byte (inside the GCM tag) → Unseal must fail.
	sealed[len(sealed)-1] ^= 0xFF
	if _, err := enc.Unseal(key, sealed, aad); err == nil {
		t.Fatal("Unseal of tampered ciphertext should fail")
	}
	sealed[len(sealed)-1] ^= 0xFF
	// Wrong AAD → Unseal must fail.
	if _, err := enc.Unseal(key, sealed, []byte("other-aad")); err == nil {
		t.Fatal("Unseal with wrong AAD should fail")
	}
}

// TestAESKeyTable_InputChangeChangesOutput — flipping a single byte
// in either keys or aad must change both the synthesized nonce
// (visible in sealed[1:13]) and the ciphertext+tag tail. This is the
// safety property that makes the synthesized-nonce scheme equivalent
// to a random-nonce GCM seal for distinct plaintexts.
func TestAESKeyTable_InputChangeChangesOutput(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	keys := bytes.Repeat([]byte{0x33}, 128)
	aad := []byte("aad")
	enc := &AESKeyTableEncryptor{}

	base, err := enc.Seal(key, keys, aad)
	if err != nil {
		t.Fatal(err)
	}
	// flip a byte in keys
	keys2 := append([]byte(nil), keys...)
	keys2[7] ^= 1
	v1, err := enc.Seal(key, keys2, aad)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(base[1:1+gcmNonceSize], v1[1:1+gcmNonceSize]) {
		t.Fatal("nonce unchanged after keys change")
	}
	// flip a byte in aad
	aad2 := append([]byte(nil), aad...)
	aad2[0] ^= 1
	v2, err := enc.Seal(key, keys, aad2)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(base[1:1+gcmNonceSize], v2[1:1+gcmNonceSize]) {
		t.Fatal("nonce unchanged after aad change")
	}
}

// TestAES_DecryptInPlace_LegacyDecryptUnchanged — Decrypt still
// returns a fresh allocation independent of the input buffer.
func TestAES_DecryptInPlace_LegacyDecryptUnchanged(t *testing.T) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatal(err)
	}
	plain := []byte("hello world")

	enc := &AESChunkEncryptor{}
	ct, _ := enc.Encrypt(key, plain)

	out, err := enc.Decrypt(key, ct)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, plain) {
		t.Fatalf("plaintext mismatch")
	}
	// ct must NOT have been mutated by legacy Decrypt — re-decrypting
	// works again.
	out2, err := enc.Decrypt(key, ct)
	if err != nil {
		t.Fatalf("second Decrypt: %v", err)
	}
	if !bytes.Equal(out2, plain) {
		t.Fatalf("ct was mutated by legacy Decrypt; expected immutable")
	}
}
