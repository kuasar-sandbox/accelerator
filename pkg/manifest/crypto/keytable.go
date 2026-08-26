package crypto

import (
	"encoding/hex"
	"fmt"
)

// NewChunkEncryptor creates the chunk codec for the given mode. The only
// supported mode is "aes"; any other value is rejected so a misconfiguration
// fails loudly at startup rather than silently storing unencrypted chunks.
func NewChunkEncryptor(mode string) (ChunkEncryptor, error) {
	switch mode {
	case "aes":
		return &AESChunkEncryptor{}, nil
	case "aes-gcm":
		return &AESGCMChunkEncryptor{}, nil
	default:
		return nil, fmt.Errorf("crypto: unknown chunk encryptor mode %q (only %q is supported)", mode, "aes")
	}
}

// NewKeyTableEncryptor creates a KeyTableEncryptor for the given mode. Only
// "aes" is supported; see NewChunkEncryptor.
func NewKeyTableEncryptor(mode string) (KeyTableEncryptor, error) {
	switch mode {
	case "aes":
		return &AESKeyTableEncryptor{}, nil
	default:
		return nil, fmt.Errorf("crypto: unknown key table encryptor mode %q (only %q is supported)", mode, "aes")
	}
}

// ChunkName returns the hex-encoded hash as a chunk storage name.
func ChunkName(hash [32]byte) string {
	return hex.EncodeToString(hash[:])
}
