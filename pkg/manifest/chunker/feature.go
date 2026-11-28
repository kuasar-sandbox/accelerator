package chunker

import (
	"errors"
	"fmt"
)

// FeatureCutter splits an in-memory buffer into small content-defined pieces.
//
// It exists for similarity detection (see pkg/manifest/delta), not for storage:
// the pieces are hashed and discarded, never stored or referenced by a manifest.
// It uses the same gear hash and two-phase normalized masks as the storage
// chunker so the two behave consistently, but differs in two deliberate ways:
//
//   - Cut points are NOT page-aligned. Storage chunks are 4 KiB aligned so that
//     they map onto restore pages; feature pieces are 256 B..4 KiB, far below
//     that granularity, and alignment would defeat the purpose.
//   - It is a pure function of (data, min, avg, max). It never reads from a
//     stream and never allocates per piece.
//
// The storage chunker's behavior is untouched; manifest determinism does not
// depend on this type.
type FeatureCutter struct {
	min, avg, max uint32
	masks         cdcMasks
}

// NewFeatureCutter validates bounds and derives masks. Unlike chunker.New it
// does not require page-aligned sizes.
func NewFeatureCutter(minSize, avgSize, maxSize uint32) (*FeatureCutter, error) {
	switch {
	case minSize == 0:
		return nil, errors.New("chunker: feature min size must be > 0")
	case !(minSize < avgSize && avgSize < maxSize):
		return nil, fmt.Errorf("chunker: feature sizes must satisfy min < avg < max (got %d/%d/%d)", minSize, avgSize, maxSize)
	case avgSize < 2:
		return nil, errors.New("chunker: feature avg size too small")
	}
	return &FeatureCutter{
		min:   minSize,
		avg:   avgSize,
		max:   maxSize,
		masks: deriveMasks(minSize, avgSize, maxSize),
	}, nil
}

// Split calls fn once per piece, in order. The slices alias data and are valid
// only for the duration of the call. The concatenation of all pieces is exactly
// data. fn must not retain or modify the slice.
func (c *FeatureCutter) Split(data []byte, fn func(piece []byte)) {
	for len(data) > 0 {
		n := c.cut(data)
		fn(data[:n])
		data = data[n:]
	}
}

// cut mirrors cdcCutPoint without alignDown.
func (c *FeatureCutter) cut(data []byte) int {
	n := len(data)
	if n > int(c.max) {
		n = int(c.max)
		data = data[:n]
	}
	if n <= int(c.min) {
		return n
	}

	phase1End := int(c.avg)
	if phase1End > n {
		phase1End = n
	}
	var hash uint64
	for i := int(c.min); i < phase1End; i++ {
		hash = gearHash(hash, data[i])
		if hash&c.masks.small == 0 {
			return i + 1
		}
	}
	for i := phase1End; i < n; i++ {
		hash = gearHash(hash, data[i])
		if hash&c.masks.large == 0 {
			return i + 1
		}
	}
	return n
}
