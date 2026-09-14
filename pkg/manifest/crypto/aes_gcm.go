package crypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"

	"github.com/golang/snappy"
)

const gcmAuthTagSize = 16

func GetGCMAuthTagSize() int {
	return gcmAuthTagSize
}

// AESGCMChunkEncryptor implements the fixed adaptive RAW/Snappy chunk format. Its
// zero value uses process-wide bounded encode/decode scratch managers.
type AESGCMChunkEncryptor struct {
	encodeScratch *scratchManager
	decodeScratch *scratchManager
	encodeBlock   func(dst, src []byte) ([]byte, error)
}

func (e *AESGCMChunkEncryptor) Mode() string {
	return "aes-gcm"
}

func (e *AESGCMChunkEncryptor) encode(dst, src []byte) ([]byte, error) {
	if e != nil && e.encodeBlock != nil {
		return e.encodeBlock(dst, src)
	}
	return encodeSnappyBlock(dst, src)
}

func (e *AESGCMChunkEncryptor) encScratch() *scratchManager {
	if e != nil && e.encodeScratch != nil {
		return e.encodeScratch
	}
	return defaultEncodeScratch
}

func (e *AESGCMChunkEncryptor) decScratch() *scratchManager {
	if e != nil && e.decodeScratch != nil {
		return e.decodeScratch
	}
	return defaultDecodeScratch
}

// EncryptChunk derives K=SHA256(salt||original plaintext), evaluates the
// canonical Snappy block candidate, encrypts the selected payload with AES-GCM and a
// deterministic nonce derived from the key used for encryption. The encoded scratch is
// released before this method returns and therefore before Store.Put begins.
func (e *AESGCMChunkEncryptor) EncryptChunk(
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

	nonce := []byte{format}
	nonce = append(nonce, key[:gcmNonceSize]...)

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(fmt.Sprintf("crypto: cipher.NewGCM: %v", err))
	}

	// Not including the nonce in the ciphertext as it is easily
	// derived from the key used for encryption.
	object := aesgcm.Seal(nonce, nonce[1:], payload, nil)

	lease.release()

	// TODO(CHARAN): Do we need to hash the object and return it for GCM ? return nil ?
	hash := sha256.Sum256(object)
	return object, hash, key, nil
}

// DecryptChunkTo decrypts immutable ciphertext into an exact-size caller
// destination. RAW writes AES output straight into dst. Snappy uses bounded
// encoded scratch and asks snappy.Decode to reuse dst's backing array.
func (e *AESGCMChunkEncryptor) DecryptChunkTo(
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
		actualObjectLength := len(object)-1-gcmNonceSize-gcmAuthTagSize
		if actualObjectLength != len(dst) {
			return fmt.Errorf("crypto: RAW payload length (without format, nonce and auth tag) %d, want %d", actualObjectLength, len(dst))
		}
		return openGCM(key, &dst, object[1:], 0)
	case ChunkFormatAESSnappy:
		return e.decryptSnappyTo(ctx, key, object[1:], dst)
	default:
		return fmt.Errorf("crypto: unknown chunk format 0x%02x", object[0])
	}
}

func (e *AESGCMChunkEncryptor) DecryptChunkRangeTo(
	ctx context.Context,
	key [32]byte,
	object []byte,
	plaintextSize int,
	plaintextOffset int,
	dst []byte,
) error {
	// Currently just a place holder to satisfy the interface.
	return e.DecryptChunkTo(ctx, key, object, dst)
}

func (e *AESGCMChunkEncryptor) decryptSnappyTo(ctx context.Context, key [32]byte, payload, dst []byte) error {
	maxEncoded := snappy.MaxEncodedLen(len(dst))
	if maxEncoded < 0 || len(payload) > maxEncoded {
		return fmt.Errorf("crypto: Snappy payload length %d exceeds maximum %d for %d decoded bytes", len(payload), maxEncoded, len(dst))
	}
	if !compressionBeneficial(uint64(len(dst)), uint64(len(payload))) {
		return fmt.Errorf("crypto: non-canonical Snappy payload length %d for %d decoded bytes", len(payload), len(dst))
	}
	actualPayloadLength := len(payload)-gcmNonceSize-gcmAuthTagSize
	lease, err := e.decScratch().acquire(ctx, actualPayloadLength)
	if err != nil {
		return fmt.Errorf("crypto: acquire decode scratch: %w", err)
	}
	defer lease.release()
	if err := ctx.Err(); err != nil {
		return err
	}
	lease.markTouched(len(lease.buf))
	if err := openGCM(key, &lease.buf, payload, 0); err != nil {
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

func openGCM(key [32]byte, dst *[]byte, src []byte, plaintextOffset uint64) error {
	actualSrcLength := len(src)-gcmNonceSize-gcmAuthTagSize
	if len(*dst) != actualSrcLength {
		return fmt.Errorf("crypto: AES source length (without nonce and auth tag) %d, destination length %d", actualSrcLength, len(*dst))
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return fmt.Errorf("crypto: aes.NewCipher: %w", err)
	}
	
	// As AES-GCM cannot decrypt a ciphertext range, we do not consider plaintextOffset.
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(fmt.Sprintf("crypto: cipher.NewGCM: %v", err))
	}

	nonce := src[:gcmNonceSize]
	_, err = aesgcm.Open((*dst)[:0], nonce, src[gcmNonceSize:], nil)
	if err != nil {
		return fmt.Errorf("crypto: ciphertext corrupt or tampered store/cache")
	}

	return nil
}