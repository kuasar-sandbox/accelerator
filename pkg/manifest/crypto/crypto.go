package crypto

import "fmt"

// Encryptor is the write-side narrow interface: encrypt a chunk, seal
// a key table. Returned by New together with a matching Decryptor; the
// two views over the same underlying codec keep ingest and fetch paths
// from accidentally touching each other's primitives.
type Encryptor interface {
	// EncryptChunk derives the convergent key from (salt, plaintext), encrypts
	// under it, and returns the ciphertext, its hash, and the derived key (to
	// record in the key table). See crypto.DeriveKey and the AES zero-IV invariant.
	EncryptChunk(salt [32]byte, plaintext []byte) (ciphertext []byte, hash [32]byte, key [32]byte)
	SealKeyTable(customerKey [32]byte, keys, aad []byte) ([]byte, error)
}

// Decryptor is the read-side narrow interface. Symmetric counterpart
// to Encryptor — same algorithms, opposite direction.
type Decryptor interface {
	DecryptChunk(key [32]byte, ciphertext []byte) (plaintext []byte, err error)
	DecryptChunkInPlace(key [32]byte, buf []byte) (plaintext []byte, err error)
	UnsealKeyTable(customerKey [32]byte, sealed, aad []byte) (keys []byte, err error)
}

// New constructs the Encryptor + Decryptor pair selected by cfg. Both
// views are backed by the same underlying impl so the algorithm choice
// is per-process atomic; New is idempotent within a process.
//
// The only supported mode is "aes" (AES-256-CTR convergent chunks +
// AES-256-GCM key table); any other value is rejected (no insecure fallback).
func New(cfg Config) (Encryptor, Decryptor, error) {
	if _, err := cfg.LocalPolicy(); err != nil {
		return nil, nil, err
	}
	chunk, err := NewChunkEncryptor(cfg.Chunk)
	if err != nil {
		return nil, nil, fmt.Errorf("crypto: chunk: %w", err)
	}
	kt, err := NewKeyTableEncryptor(cfg.Manifest)
	if err != nil {
		return nil, nil, fmt.Errorf("crypto: keytable: %w", err)
	}
	c := &codec{chunk: chunk, kt: kt}
	return c, c, nil
}

// codec adapts the legacy split-interface impls
// (ChunkEncryptor + KeyTableEncryptor) to the new narrow interfaces.
// Single struct so both views share state if any is ever added.
type codec struct {
	chunk ChunkEncryptor
	kt    KeyTableEncryptor
}

func (c *codec) EncryptChunk(salt [32]byte, plaintext []byte) ([]byte, [32]byte, [32]byte) {
	return c.chunk.Encrypt(salt, plaintext)
}

func (c *codec) DecryptChunk(key [32]byte, ciphertext []byte) ([]byte, error) {
	return c.chunk.Decrypt(key, ciphertext)
}

func (c *codec) DecryptChunkInPlace(key [32]byte, buf []byte) ([]byte, error) {
	return c.chunk.DecryptInPlace(key, buf)
}

func (c *codec) SealKeyTable(customerKey [32]byte, keys, aad []byte) ([]byte, error) {
	return c.kt.Seal(customerKey, keys, aad)
}

func (c *codec) UnsealKeyTable(customerKey [32]byte, sealed, aad []byte) ([]byte, error) {
	return c.kt.Unseal(customerKey, sealed, aad)
}
