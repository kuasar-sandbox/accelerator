package crypto

import (
	"context"
	"fmt"
)

// Encryptor is the write-side narrow interface: encrypt a chunk, seal
// a key table. Returned by New together with a matching Decryptor; the
// two views over the same underlying codec keep ingest and fetch paths
// from accidentally touching each other's primitives.
type Encryptor interface {
	// EncryptChunk derives the convergent key from (salt, original plaintext),
	// chooses the fixed canonical RAW/Snappy payload, encrypts it, and returns the
	// complete physical object, its hash, and the key recorded in the key table.
	EncryptChunk(ctx context.Context, salt [32]byte, plaintext []byte) (object []byte, hash [32]byte, key [32]byte, err error)
	SealKeyTable(customerKey [32]byte, keys, aad []byte) ([]byte, error)
}

// DeltaEncryptor is implemented by encryptors that can also produce
// ChunkFormatAESDelta objects. It is a separate, optional interface so that
// existing Encryptor implementations (and test doubles) are unaffected; callers
// type-assert for it and treat its absence as "delta unsupported".
type DeltaEncryptor interface {
	// EncryptDeltaChunk stores target as a delta against base. stream is the
	// VCDIFF encoding of target relative to base's plaintext. It returns the
	// complete physical object, its hash, and the key to record in the key table
	// (the delta key K_d, which is NOT the convergent key of target).
	EncryptDeltaChunk(ctx context.Context, salt [32]byte, base DeltaBase, target, stream []byte) (object []byte, hash [32]byte, key [32]byte, err error)
}

// DeltaDecryptor is the read-side counterpart of DeltaEncryptor.
type DeltaDecryptor interface {
	// OpenDeltaChunk parses and decrypts a ChunkFormatAESDelta object with its
	// key-table key, returning the base reference and the VCDIFF stream. The
	// caller fetches the base, decrypts it with the returned BaseKey, and applies
	// the stream.
	OpenDeltaChunk(ctx context.Context, key [32]byte, object []byte) (DeltaOpened, error)
}

// Decryptor is the read-side narrow interface. Symmetric counterpart
// to Encryptor — same algorithms, opposite direction.
type Decryptor interface {
	// DecryptChunkTo treats ciphertext as immutable and fills the exact-size
	// destination. The slices must not overlap. ctx controls bounded Snappy scratch
	// admission; on an error dst is unspecified.
	DecryptChunkTo(ctx context.Context, key [32]byte, ciphertext, dst []byte) error
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
	if _, err := cfg.RefPolicy(); err != nil {
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

// codec combines the chunk and key-table implementations behind the narrow
// read/write interfaces. Both views share the same bounded codec state.
type codec struct {
	chunk *AESChunkEncryptor
	kt    KeyTableEncryptor
}

func (c *codec) EncryptChunk(ctx context.Context, salt [32]byte, plaintext []byte) ([]byte, [32]byte, [32]byte, error) {
	return c.chunk.EncryptChunk(ctx, salt, plaintext)
}

func (c *codec) EncryptDeltaChunk(ctx context.Context, salt [32]byte, base DeltaBase, target, stream []byte) ([]byte, [32]byte, [32]byte, error) {
	return c.chunk.EncryptDeltaChunk(ctx, salt, base, target, stream)
}

func (c *codec) OpenDeltaChunk(ctx context.Context, key [32]byte, object []byte) (DeltaOpened, error) {
	return c.chunk.OpenDeltaChunk(ctx, key, object)
}

func (c *codec) DecryptChunkTo(ctx context.Context, key [32]byte, ciphertext, dst []byte) error {
	return c.chunk.DecryptChunkTo(ctx, key, ciphertext, dst)
}

func (c *codec) DecryptChunkRangeTo(ctx context.Context, key [32]byte, ciphertext []byte, plaintextSize, plaintextOffset int, dst []byte) error {
	return c.chunk.DecryptChunkRangeTo(ctx, key, ciphertext, plaintextSize, plaintextOffset, dst)
}

func (c *codec) SealKeyTable(customerKey [32]byte, keys, aad []byte) ([]byte, error) {
	return c.kt.Seal(customerKey, keys, aad)
}

func (c *codec) UnsealKeyTable(customerKey [32]byte, sealed, aad []byte) ([]byte, error) {
	return c.kt.Unseal(customerKey, sealed, aad)
}
