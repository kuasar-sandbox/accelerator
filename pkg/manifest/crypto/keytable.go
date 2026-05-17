package crypto

import (
	"encoding/hex"
	"fmt"
)

// NewChunkEncryptor creates a ChunkEncryptor for the given mode.
// Supported modes: "aes", "fake".
func NewChunkEncryptor(mode string) (ChunkEncryptor, error) {
	switch mode {
	case "aes":
		return &AESChunkEncryptor{}, nil
	case "fake":
		return &FakeChunkEncryptor{}, nil
	default:
		return nil, fmt.Errorf("crypto: unknown chunk encryptor mode %q", mode)
	}
}

// NewKeyTableEncryptor creates a KeyTableEncryptor for the given mode.
// Supported modes: "aes", "fake".
func NewKeyTableEncryptor(mode string) (KeyTableEncryptor, error) {
	switch mode {
	case "aes":
		return &AESKeyTableEncryptor{}, nil
	case "fake":
		return &FakeKeyTableEncryptor{}, nil
	default:
		return nil, fmt.Errorf("crypto: unknown key table encryptor mode %q", mode)
	}
}

// ChunkName returns the hex-encoded hash as a chunk storage name.
func ChunkName(hash [32]byte) string {
	return hex.EncodeToString(hash[:])
}
