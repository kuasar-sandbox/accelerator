package crypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/golang/snappy"
)

// AESChunkEncryptor implements the fixed adaptive RAW/Snappy chunk format. Its
// zero value uses process-wide bounded encode/decode scratch managers.
type AESChunkEncryptor struct {
	encodeScratch *scratchManager
	decodeScratch *scratchManager
	encodeBlock   func(dst, src []byte) ([]byte, error)
}

func (e *AESChunkEncryptor) Mode() string {
	return "aes"
}

func (e *AESChunkEncryptor) encode(dst, src []byte) ([]byte, error) {
	if e != nil && e.encodeBlock != nil {
		return e.encodeBlock(dst, src)
	}
	return encodeSnappyBlock(dst, src)
}

func (e *AESChunkEncryptor) encScratch() *scratchManager {
	if e != nil && e.encodeScratch != nil {
		return e.encodeScratch
	}
	return defaultEncodeScratch
}

func (e *AESChunkEncryptor) decScratch() *scratchManager {
	if e != nil && e.decodeScratch != nil {
		return e.decodeScratch
	}
	return defaultDecodeScratch
}

// EncryptChunk derives K=SHA256(salt||original plaintext), evaluates the
// canonical Snappy block candidate, encrypts the selected payload with AES-CTR
// and a zero IV, and hashes the complete physical object. The encoded scratch is
// released before this method returns and therefore before Store.Put begins.
func (e *AESChunkEncryptor) EncryptChunk(
	ctx context.Context,
	salt [32]byte,
	plaintext []byte,
) ([]byte, [32]byte, [32]byte, error) {
	var zero [32]byte
	if err := ctx.Err(); err != nil {
		return nil, zero, zero, err
	}
	if len(plaintext) > MaxChunkDecodedSize {
		return nil, zero, zero, fmt.Errorf("%w: %d > %d", ErrChunkTooLarge, len(plaintext), MaxChunkDecodedSize)
	}
	maxEncoded := snappy.MaxEncodedLen(len(plaintext))
	if maxEncoded < 0 {
		return nil, zero, zero, fmt.Errorf("crypto: snappy max encoded length for %d", len(plaintext))
	}
	lease, err := e.encScratch().acquire(ctx, maxEncoded)
	if err != nil {
		return nil, zero, zero, fmt.Errorf("crypto: acquire encode scratch: %w", err)
	}
	if err := ctx.Err(); err != nil {
		lease.release()
		return nil, zero, zero, err
	}
	// golang/snappy requires len(dst), rather than only cap(dst), to cover
	// MaxEncodedLen. Passing the complete lease prevents a fallback allocation
	// outside the process-wide scratch bound.
	encoded, err := e.encode(lease.buf, plaintext)
	if err != nil {
		lease.markTouched(cap(lease.buf))
		lease.release()
		return nil, zero, zero, fmt.Errorf("crypto: Snappy encode: %w", err)
	}
	if len(encoded) > maxEncoded || (len(encoded) > 0 && &encoded[0] != &lease.buf[0]) {
		lease.markTouched(cap(lease.buf))
		lease.release()
		return nil, zero, zero, fmt.Errorf("crypto: Snappy encoder did not reuse bounded scratch: encoded=%d max=%d", len(encoded), maxEncoded)
	}
	lease.markTouched(len(encoded))
	decodedLen, err := snappy.DecodedLen(encoded)
	if err != nil {
		lease.release()
		return nil, zero, zero, fmt.Errorf("crypto: invalid Snappy encoder output length: %w", err)
	}
	if decodedLen != len(plaintext) {
		lease.release()
		return nil, zero, zero, fmt.Errorf("crypto: invalid Snappy encoder output: encoded=%d decoded=%d, want %d", len(encoded), decodedLen, len(plaintext))
	}
	if err := ctx.Err(); err != nil {
		lease.release()
		return nil, zero, zero, err
	}

	format := ChunkFormatAESRaw
	payload := plaintext
	if compressionBeneficial(uint64(len(plaintext)), uint64(len(encoded))) {
		format = ChunkFormatAESSnappy
		payload = encoded
	}

	key := DeriveKey(salt, plaintext)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		lease.release()
		return nil, zero, zero, fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	object := make([]byte, 1+len(payload))
	object[0] = format
	var iv [aes.BlockSize]byte
	cipher.NewCTR(block, iv[:]).XORKeyStream(object[1:], payload)
	lease.release()

	hash := sha256.Sum256(object)
	return object, hash, key, nil
}

func encodeSnappyBlock(dst, src []byte) (encoded []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			encoded = nil
			err = fmt.Errorf("crypto: Snappy encode failed: %v", recovered)
		}
	}()
	return snappy.Encode(dst, src), nil
}

// DecryptChunkTo decrypts immutable ciphertext into an exact-size caller
// destination. RAW writes AES output straight into dst. Snappy uses bounded
// encoded scratch and asks snappy.Decode to reuse dst's backing array.
func (e *AESChunkEncryptor) DecryptChunkTo(
	ctx context.Context,
	key [32]byte,
	object []byte,
	dst []byte,
) error {
	if err := validateChunkDestination(ctx, object, dst); err != nil {
		return err
	}
	switch object[0] {
	case ChunkFormatAESRaw:
		if len(object)-1 != len(dst) {
			return fmt.Errorf("crypto: RAW payload length %d, want %d", len(object)-1, len(dst))
		}
		return xorCTR(key, dst, object[1:], 0)
	case ChunkFormatAESSnappy:
		return e.decryptSnappyTo(ctx, key, object[1:], dst)
	default:
		return fmt.Errorf("crypto: unknown chunk format 0x%02x", object[0])
	}
}

// DecryptChunkRangeTo serves the exceptional oversized partial-read path. RAW
// seeks the CTR stream and writes only the requested range. Snappy is a single
// block and therefore must decode the complete chunk into bounded temporary
// plaintext before copying the range.
func (e *AESChunkEncryptor) DecryptChunkRangeTo(
	ctx context.Context,
	key [32]byte,
	object []byte,
	plaintextSize int,
	plaintextOffset int,
	dst []byte,
) error {
	if plaintextSize < 0 || plaintextSize > MaxChunkDecodedSize {
		return fmt.Errorf("%w: %d", ErrChunkTooLarge, plaintextSize)
	}
	if plaintextOffset < 0 || plaintextOffset > plaintextSize || len(dst) > plaintextSize-plaintextOffset {
		return fmt.Errorf("crypto: range [%d,%d) outside plaintext size %d", plaintextOffset, plaintextOffset+len(dst), plaintextSize)
	}
	if err := validateChunkDestination(ctx, object, dst); err != nil {
		return err
	}
	switch object[0] {
	case ChunkFormatAESRaw:
		if len(object)-1 != plaintextSize {
			return fmt.Errorf("crypto: RAW payload length %d, want %d", len(object)-1, plaintextSize)
		}
		return xorCTR(key, dst, object[1+plaintextOffset:1+plaintextOffset+len(dst)], uint64(plaintextOffset))
	case ChunkFormatAESSnappy:
		maxEncoded := snappy.MaxEncodedLen(plaintextSize)
		if maxEncoded < 0 || len(object)-1 > maxEncoded {
			return fmt.Errorf("crypto: Snappy payload length %d exceeds maximum %d for %d decoded bytes", len(object)-1, maxEncoded, plaintextSize)
		}
		if !compressionBeneficial(uint64(plaintextSize), uint64(len(object)-1)) {
			return fmt.Errorf("crypto: non-canonical Snappy payload length %d for %d decoded bytes", len(object)-1, plaintextSize)
		}
		encodedSize := len(object) - 1
		combinedSize := encodedSize + plaintextSize
		if combinedSize < encodedSize {
			return errors.New("crypto: oversized Snappy scratch length overflows")
		}
		lease, err := e.decScratch().acquire(ctx, combinedSize)
		if err != nil {
			return fmt.Errorf("crypto: acquire oversized decode scratch: %w", err)
		}
		defer lease.release()
		if err := ctx.Err(); err != nil {
			return err
		}
		lease.markTouched(combinedSize)
		encoded := lease.buf[:encodedSize]
		plain := lease.buf[encodedSize:]
		if err := xorCTR(key, encoded, object[1:], 0); err != nil {
			return err
		}
		if err := decodeSnappyExact(encoded, plain); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		copy(dst, plain[plaintextOffset:plaintextOffset+len(dst)])
		return nil
	default:
		return fmt.Errorf("crypto: unknown chunk format 0x%02x", object[0])
	}
}

func (e *AESChunkEncryptor) decryptSnappyTo(ctx context.Context, key [32]byte, payload, dst []byte) error {
	maxEncoded := snappy.MaxEncodedLen(len(dst))
	if maxEncoded < 0 || len(payload) > maxEncoded {
		return fmt.Errorf("crypto: Snappy payload length %d exceeds maximum %d for %d decoded bytes", len(payload), maxEncoded, len(dst))
	}
	if !compressionBeneficial(uint64(len(dst)), uint64(len(payload))) {
		return fmt.Errorf("crypto: non-canonical Snappy payload length %d for %d decoded bytes", len(payload), len(dst))
	}
	lease, err := e.decScratch().acquire(ctx, len(payload))
	if err != nil {
		return fmt.Errorf("crypto: acquire decode scratch: %w", err)
	}
	defer lease.release()
	if err := ctx.Err(); err != nil {
		return err
	}
	lease.markTouched(len(lease.buf))
	if err := xorCTR(key, lease.buf, payload, 0); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := decodeSnappyExact(lease.buf, dst); err != nil {
		return err
	}
	return ctx.Err()
}

func decodeSnappyExact(encoded, dst []byte) error {
	decodedLen, err := snappy.DecodedLen(encoded)
	if err != nil {
		return fmt.Errorf("crypto: Snappy decoded length: %w", err)
	}
	if decodedLen != len(dst) {
		return fmt.Errorf("crypto: Snappy decoded length %d, want %d", decodedLen, len(dst))
	}
	decoded, err := snappy.Decode(dst, encoded)
	if err != nil {
		return fmt.Errorf("crypto: Snappy decode: %w", err)
	}
	if len(decoded) != len(dst) || (len(dst) > 0 && &decoded[0] != &dst[0]) {
		return errors.New("crypto: Snappy decode did not reuse the exact destination")
	}
	return nil
}

func validateChunkDestination(ctx context.Context, object, dst []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(object) == 0 {
		return errors.New("crypto: chunk object is empty")
	}
	if len(dst) > MaxChunkDecodedSize {
		return fmt.Errorf("%w: %d > %d", ErrChunkTooLarge, len(dst), MaxChunkDecodedSize)
	}
	if slicesOverlap(object, dst) {
		return errors.New("crypto: ciphertext and destination overlap")
	}
	return nil
}

func xorCTR(key [32]byte, dst, src []byte, plaintextOffset uint64) error {
	if len(dst) != len(src) {
		return fmt.Errorf("crypto: AES source length %d, destination length %d", len(src), len(dst))
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	blockIndex := plaintextOffset / aes.BlockSize
	var iv [aes.BlockSize]byte
	for i := len(iv) - 1; blockIndex > 0; i-- {
		iv[i] = byte(blockIndex)
		blockIndex >>= 8
	}
	stream := cipher.NewCTR(block, iv[:])
	if skip := int(plaintextOffset % aes.BlockSize); skip != 0 {
		var zeros, discard [aes.BlockSize]byte
		stream.XORKeyStream(discard[:skip], zeros[:skip])
	}
	stream.XORKeyStream(dst, src)
	return nil
}

// AESKeyTableEncryptor implements KeyTableEncryptor using AES-256-GCM.
type AESKeyTableEncryptor struct{}

const gcmNonceSize = 12

func GetGCMNonceSize() int {
	return gcmNonceSize
}

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
// The domain-separation prefix scopes this HMAC use of the customer key
// so it cannot collide with any other HMAC use of the same key.
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
	sealed = append(sealed, KeyTableFormatAESGCM)
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
	if sealed[0] != KeyTableFormatAESGCM {
		return nil, fmt.Errorf("crypto: unexpected key-table format byte 0x%02x, want 0x%02x", sealed[0], KeyTableFormatAESGCM)
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
