package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

const (
	// FlagAESGCM is the fixed encrypted tarstream v1 record encoding flag.
	FlagAESGCM byte = 0x01

	aesGCMOverhead = 1 + 16
)

const artifactKeyDomain = "kuasar/tarstream/aes-gcm/artifact-key/v1\x00"

// TarStreamCodec holds the customer key used to bind encrypted tarstream v1
// artifacts. It is immutable and safe for concurrent use.
type TarStreamCodec struct {
	customerKey [32]byte
}

// recordCodec is bound to one artifact salt. Its AES-256-GCM instance is
// constructed once and is safe for concurrent use.
type recordCodec struct {
	aead cipher.AEAD
}

// NewTarStreamCodec retains customerKey for artifact binding and logical
// identity. It does not read configuration, resolve credentials, or connect to
// storage.
func NewTarStreamCodec(customerKey [32]byte) (*TarStreamCodec, error) {
	return &TarStreamCodec{customerKey: customerKey}, nil
}

// BindArtifact derives an artifact-specific AES-256 key and constructs the GCM
// primitive once. Per-record operations only synthesize a counter nonce.
func (c *TarStreamCodec) BindArtifact(salt [32]byte) (tarstream.RecordCodec, error) {
	mac := hmac.New(sha256.New, c.customerKey[:])
	_, _ = mac.Write([]byte(artifactKeyDomain))
	_, _ = mac.Write(salt[:])
	key := mac.Sum(nil)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: tarstream artifact cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: tarstream artifact GCM: %w", err)
	}
	return &recordCodec{aead: aead}, nil
}

func keyedHMAC(key [32]byte, data []byte) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(data)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

// CiphertextSize returns flag + GCM ciphertext and tag length.
func (*recordCodec) CiphertextSize(plaintextSize int) int {
	if plaintextSize < 0 || plaintextSize > int(^uint(0)>>1)-aesGCMOverhead {
		return -1
	}
	return plaintextSize + aesGCMOverhead
}

// Encrypt appends one AES-256-GCM record to dst. The caller guarantees that
// sequence is unique within the bound artifact. Inputs are preserved even when
// dst capacity aliases plaintext or associatedData.
func (c *recordCodec) Encrypt(dst, plaintext, associatedData []byte, sequence uint64) ([]byte, error) {
	size := c.CiphertextSize(len(plaintext))
	if size < 0 {
		return nil, fmt.Errorf("crypto: tarstream: plaintext size overflow")
	}
	start := len(dst)
	if cap(dst)-len(dst) >= size {
		dst = dst[:len(dst)+size]
		if slicesOverlap(dst[start:], plaintext) || slicesOverlap(dst[start:], associatedData) {
			moved := make([]byte, len(dst))
			copy(moved, dst[:start])
			dst = moved
		}
	} else {
		dst = append(dst, make([]byte, size)...)
	}
	record := dst[start:]
	record[0] = FlagAESGCM
	nonce := recordNonce(sequence)
	sealed := c.aead.Seal(record[1:1], nonce[:], plaintext, associatedData)
	if len(sealed) != len(record)-1 {
		panic("crypto: tarstream GCM returned unexpected ciphertext size")
	}
	return dst, nil
}

func slicesOverlap(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	aStart := uintptr(unsafe.Pointer(unsafe.SliceData(a)))
	bStart := uintptr(unsafe.Pointer(unsafe.SliceData(b)))
	if aStart < bStart {
		return bStart-aStart < uintptr(len(a))
	}
	return aStart-bStart < uintptr(len(b))
}

// DecryptInPlace authenticates and decrypts one record into record[1:]'s
// backing storage. Authentication failure clears any tentative plaintext and
// never returns it.
func (c *recordCodec) DecryptInPlace(record, associatedData []byte, sequence uint64) ([]byte, error) {
	if len(record) < aesGCMOverhead || record[0] != FlagAESGCM {
		return nil, fmt.Errorf("%w: invalid AES-GCM record", tarstream.ErrAuthentication)
	}
	nonce := recordNonce(sequence)
	plaintextSize := len(record) - aesGCMOverhead
	plaintext, err := c.aead.Open(record[1:1], nonce[:], record[1:], associatedData)
	if err != nil {
		clear(record[1 : 1+plaintextSize])
		return nil, tarstream.ErrAuthentication
	}
	return plaintext, nil
}

func recordNonce(sequence uint64) [12]byte {
	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[4:], sequence)
	return nonce
}

// KeyedDigest computes HMAC-SHA256(customerKey, plainDigestRaw32Bytes).
func (c *TarStreamCodec) KeyedDigest(plainDigest [32]byte) [32]byte {
	return keyedHMAC(c.customerKey, plainDigest[:])
}

var (
	_ tarstream.Codec       = (*TarStreamCodec)(nil)
	_ tarstream.RecordCodec = (*recordCodec)(nil)
)
