package crypto

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/golang/snappy"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/internal/objectformat"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
)

// ChunkFormatAESDelta object layout (all integers little endian):
//
//	[0]      format byte 0x03
//	[1:33]   base object key: address of the base chunk's object (cleartext, so a
//	         reader can start fetching the base before it decrypts anything)
//	[33:37]  base plaintext size   u32
//	[37:41]  target plaintext size u32 (equals ChunkEntry.Size)
//	[41:45]  VCDIFF stream size    u32 (the decoded stream, i.e. before Snappy)
//	[45]     payload encoding: 0x00 RAW | 0x01 Snappy
//	[46:]    AES-256-CTR(K_d, zero IV) over  K_base(32) || payload
//
// where payload is the VCDIFF stream, RAW or Snappy (same adaptive rule as
// ordinary chunks). The 78-byte overhead (46 header + 32 wrapped key) is
// charged against the acceptance rule, so it is never a hidden cost.
//
// Keys. K_d, the key the manifest's key table records for this chunk, is
//
//	K_d = SHA256(salt || "accelerator-delta-chunk/v1" || baseObjectKey || target)
//
// It is deliberately bound to the base. The CTR stream uses a zero IV, so one
// (key, IV) pair must only ever encrypt one plaintext. The same target stored as
// a delta against two different bases has two different plaintexts under the
// object, and with a base-independent key they would share a keystream (a
// two-time pad). Binding to baseObjectKey makes that impossible. K_base is the
// base's ordinary convergent key; it travels inside the encrypted stream, so
// the manifest key table stays one 32-byte key per chunk and needs no change.
//
// Integrity is unchanged: the manifest's AAD already covers the object's
// content hash, and the target's content is bound by K_d.
const (
	deltaHeaderSize   = 46
	deltaPayloadStart = deltaHeaderSize + 32

	// DeltaOverhead is the fixed number of bytes a delta object adds to its payload.
	DeltaOverhead = deltaPayloadStart

	deltaEncRaw    byte = 0x00
	deltaEncSnappy byte = 0x01

	// maxDeltaStreamSlack is how far a VCDIFF stream may exceed base+target
	// size. Real streams are far smaller; this only bounds what a reader will
	// allocate for a hostile header.
	maxDeltaStreamSlack = 1024

	deltaKeyLabel = "accelerator-delta-chunk/v1"
)

// ErrDeltaNotApplicable reports that a delta object cannot be built for these
// inputs; the caller should store the chunk independently.
var ErrDeltaNotApplicable = errors.New("crypto: delta not applicable")

// DeriveDeltaKey derives K_d (see the layout comment).
func DeriveDeltaKey(salt, baseObjectKey [32]byte, target []byte) [32]byte {
	h := sha256.New()
	h.Write(salt[:])
	h.Write([]byte(deltaKeyLabel))
	h.Write(baseObjectKey[:])
	h.Write(target)
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

// DeltaBase identifies the stored chunk a delta is relative to.
type DeltaBase struct {
	// ObjectKey is the base object's address (SHA-256 of its physical object).
	ObjectKey [32]byte
	// Key is the base chunk's ordinary convergent key K_B.
	Key [32]byte
	// PlaintextSize is the base's plaintext length.
	PlaintextSize int
}

// DeltaHeader is the cleartext header of a delta object.
type DeltaHeader struct {
	BaseObjectKey [32]byte
	BaseSize      uint32
	TargetSize    uint32
	StreamSize    uint32
	Snappy        bool
}

// DeltaOpened is a parsed and decrypted delta object.
type DeltaOpened struct {
	Header DeltaHeader
	// BaseKey is K_base: the key that decrypts the base object.
	BaseKey [32]byte
	// Stream is the decoded VCDIFF stream (exactly Header.StreamSize bytes).
	Stream []byte
}

func deltaMaxStream(baseSize, targetSize uint64) uint64 {
	return baseSize + targetSize + maxDeltaStreamSlack
}

// ParseDeltaHeader validates the structure of a delta object without any key:
// format byte, sizes, the declared payload length against the payload encoding,
// and the reader-side plausibility bound (a delta object must be at least 4 KiB
// smaller than the largest independent object its target could have produced).
// A header that fails these checks could not have been written by a conforming
// writer.
func ParseDeltaHeader(object []byte) (DeltaHeader, error) {
	bad := func(format string, args ...any) (DeltaHeader, error) {
		return DeltaHeader{}, readerr.Mark(fmt.Errorf("crypto: delta object: "+format, args...), false)
	}
	var h DeltaHeader
	if len(object) < deltaPayloadStart+1 {
		return bad("length %d below minimum %d", len(object), deltaPayloadStart+1)
	}
	if object[0] != ChunkFormatAESDelta {
		return bad("format byte 0x%02x", object[0])
	}
	copy(h.BaseObjectKey[:], object[1:33])
	h.BaseSize = binary.LittleEndian.Uint32(object[33:37])
	h.TargetSize = binary.LittleEndian.Uint32(object[37:41])
	h.StreamSize = binary.LittleEndian.Uint32(object[41:45])
	switch object[45] {
	case deltaEncRaw:
	case deltaEncSnappy:
		h.Snappy = true
	default:
		return bad("payload encoding 0x%02x", object[45])
	}
	if h.BaseSize == 0 || h.BaseSize > MaxChunkDecodedSize {
		return bad("base size %d out of range", h.BaseSize)
	}
	if h.TargetSize == 0 || h.TargetSize > MaxChunkDecodedSize {
		return bad("target size %d out of range", h.TargetSize)
	}
	if h.StreamSize == 0 || uint64(h.StreamSize) > deltaMaxStream(uint64(h.BaseSize), uint64(h.TargetSize)) {
		return bad("stream size %d out of range", h.StreamSize)
	}
	if !objectformat.DeltaPlausible(uint64(h.TargetSize), uint64(len(object))) {
		return bad("object size %d is not plausible for a %d byte target", len(object), h.TargetSize)
	}
	payloadLen := len(object) - deltaPayloadStart
	if h.Snappy {
		if max := snappy.MaxEncodedLen(int(h.StreamSize)); max < 0 || payloadLen > max {
			return bad("Snappy payload %d exceeds maximum %d", payloadLen, max)
		}
		if !compressionBeneficial(uint64(h.StreamSize), uint64(payloadLen)) {
			return bad("non-canonical Snappy payload %d for a %d byte stream", payloadLen, h.StreamSize)
		}
	} else if payloadLen != int(h.StreamSize) {
		return bad("RAW payload %d, header declares %d", payloadLen, h.StreamSize)
	}
	return h, nil
}

// EncryptDeltaChunk builds the delta object for target against base. stream is
// the VCDIFF encoding of target relative to base's plaintext. The result is a
// complete physical object; whether it is worth storing is the caller's
// decision (objectformat.DeltaBeneficial), because that needs the size of the
// independent object, which this method does not have.
func (e *AESChunkEncryptor) EncryptDeltaChunk(
	ctx context.Context,
	salt [32]byte,
	base DeltaBase,
	target, stream []byte,
) ([]byte, [32]byte, [32]byte, error) {
	var zero [32]byte
	if err := ctx.Err(); err != nil {
		return nil, zero, zero, err
	}
	if len(target) == 0 || len(target) > MaxChunkDecodedSize {
		return nil, zero, zero, fmt.Errorf("%w: target size %d", ErrDeltaNotApplicable, len(target))
	}
	if base.PlaintextSize <= 0 || base.PlaintextSize > MaxChunkDecodedSize {
		return nil, zero, zero, fmt.Errorf("%w: base size %d", ErrDeltaNotApplicable, base.PlaintextSize)
	}
	if len(stream) == 0 || uint64(len(stream)) > deltaMaxStream(uint64(base.PlaintextSize), uint64(len(target))) {
		return nil, zero, zero, fmt.Errorf("%w: stream size %d", ErrDeltaNotApplicable, len(stream))
	}

	maxEncoded := snappy.MaxEncodedLen(len(stream))
	if maxEncoded < 0 {
		return nil, zero, zero, fmt.Errorf("crypto: snappy max encoded length for %d", len(stream))
	}
	lease, err := e.encScratch().acquire(ctx, maxEncoded)
	if err != nil {
		return nil, zero, zero, fmt.Errorf("crypto: acquire encode scratch: %w", err)
	}
	encoded, err := e.encode(lease.buf, stream)
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
	if n, err := snappy.DecodedLen(encoded); err != nil || n != len(stream) {
		lease.release()
		return nil, zero, zero, fmt.Errorf("crypto: invalid Snappy encoder output for delta stream")
	}

	enc, payload := deltaEncRaw, stream
	if compressionBeneficial(uint64(len(stream)), uint64(len(encoded))) {
		enc, payload = deltaEncSnappy, encoded
	}

	object := make([]byte, deltaPayloadStart+len(payload))
	object[0] = ChunkFormatAESDelta
	copy(object[1:33], base.ObjectKey[:])
	binary.LittleEndian.PutUint32(object[33:37], uint32(base.PlaintextSize))
	binary.LittleEndian.PutUint32(object[37:41], uint32(len(target)))
	binary.LittleEndian.PutUint32(object[41:45], uint32(len(stream)))
	object[45] = enc
	copy(object[deltaHeaderSize:deltaPayloadStart], base.Key[:])
	copy(object[deltaPayloadStart:], payload)
	lease.release()

	key := DeriveDeltaKey(salt, base.ObjectKey, target)
	tail := object[deltaHeaderSize:]
	if err := xorCTR(key, tail, tail, 0); err != nil { // exact overlap is permitted by XORKeyStream
		return nil, zero, zero, err
	}
	// The object we just built must satisfy the reader's structural rules; a
	// writer that could emit something its own reader rejects would be a bug.
	if _, err := ParseDeltaHeader(object); err != nil {
		return nil, zero, zero, fmt.Errorf("%w: %v", ErrDeltaNotApplicable, err)
	}
	return object, sha256.Sum256(object), key, nil
}

// OpenDeltaChunk parses object, decrypts it with key (the key-table key K_d) and
// returns the base reference and the VCDIFF stream. It does not fetch or apply
// anything. A wrong key is not detected here (CTR has no tag): it surfaces as a
// malformed stream when the caller applies it, and in practice earlier as a
// content-hash failure because the manifest AAD covers the object hash.
func (e *AESChunkEncryptor) OpenDeltaChunk(ctx context.Context, key [32]byte, object []byte) (DeltaOpened, error) {
	if err := ctx.Err(); err != nil {
		return DeltaOpened{}, err
	}
	h, err := ParseDeltaHeader(object)
	if err != nil {
		return DeltaOpened{}, err
	}
	tail := object[deltaHeaderSize:]
	plain := make([]byte, len(tail))
	defer clear(plain)
	if err := xorCTR(key, plain, tail, 0); err != nil {
		return DeltaOpened{}, err
	}
	out := DeltaOpened{Header: h}
	copy(out.BaseKey[:], plain[:32])
	payload := plain[32:]
	if h.Snappy {
		out.Stream = make([]byte, h.StreamSize)
		if err := decodeSnappyExact(payload, out.Stream); err != nil {
			clear(out.Stream)
			return DeltaOpened{}, err
		}
	} else {
		out.Stream = append([]byte(nil), payload...)
	}
	return out, ctx.Err()
}
