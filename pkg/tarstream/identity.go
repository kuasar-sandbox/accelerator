package tarstream

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

var (
	payloadCommitmentDomain = []byte("kuasar/tarstream/payload\x00")
	tailDigestDomain        = []byte("kuasar/tarstream/tail\x00")
	carrierDigestDomain     = []byte("kuasar/tarstream/carrier\x00")
)

func newPayloadHasher(size uint64, extents []extent) hash.Hash {
	h := sha256.New()
	_, _ = h.Write(payloadCommitmentDomain)
	writeHashUint64(h, size)
	writeHashUint64(h, uint64(len(extents)))
	for _, extent := range extents {
		writeHashUint64(h, uint64(extent.Offset))
		writeHashUint64(h, uint64(extent.Size))
	}
	return h
}

func newTailHasher(size uint64) hash.Hash {
	h := sha256.New()
	_, _ = h.Write(tailDigestDomain)
	writeHashUint64(h, size)
	return h
}

func composeDigest(name string, totalSize, payloadSize uint64, payload, tail [32]byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write(carrierDigestDomain)
	writeHashUint64(h, uint64(len(name)))
	_, _ = h.Write([]byte(name))
	writeHashUint64(h, totalSize)
	writeHashUint64(h, payloadSize)
	_, _ = h.Write(payload[:])
	_, _ = h.Write(tail[:])
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func writeHashUint64(h hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = h.Write(encoded[:])
}

func prefixExtents(extents []extent, boundary, total int64) ([]extent, int64, bool) {
	result := make([]extent, 0, len(extents))
	var packed int64
	for _, current := range extents {
		if current.Offset >= boundary {
			break
		}
		end := current.Offset + current.Size
		if end > boundary {
			end = boundary
		}
		if end > current.Offset {
			result = append(result, extent{Offset: current.Offset, Size: end - current.Offset})
			packed += end - current.Offset
		}
	}
	coveredTail := boundary
	for _, current := range extents {
		start := current.Offset
		end := current.Offset + current.Size
		if end <= boundary {
			continue
		}
		if start < boundary {
			start = boundary
		}
		if start != coveredTail {
			return nil, 0, false
		}
		coveredTail = end
	}
	return result, packed, coveredTail == total
}

type identityAccumulator struct {
	payload          hash.Hash
	tail             hash.Hash
	payloadRemaining int64
}

func newIdentityAccumulator(payloadSize uint64, payloadExtents []extent, payloadPacked, tailSize int64) *identityAccumulator {
	return &identityAccumulator{
		payload:          newPayloadHasher(payloadSize, payloadExtents),
		tail:             newTailHasher(uint64(tailSize)),
		payloadRemaining: payloadPacked,
	}
}

func (a *identityAccumulator) Write(data []byte) (int, error) {
	original := len(data)
	if a.payloadRemaining > 0 {
		count := min(int64(len(data)), a.payloadRemaining)
		_, _ = a.payload.Write(data[:count])
		a.payloadRemaining -= count
		data = data[count:]
	}
	if len(data) > 0 {
		_, _ = a.tail.Write(data)
	}
	return original, nil
}

func (a *identityAccumulator) finish(name string, totalSize, payloadSize uint64) carrierIdentity {
	var payload, tail [32]byte
	copy(payload[:], a.payload.Sum(nil))
	copy(tail[:], a.tail.Sum(nil))
	return carrierIdentity{
		digest:      composeDigest(name, totalSize, payloadSize, payload, tail),
		payload:     payload,
		payloadSize: payloadSize,
	}
}
