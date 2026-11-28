// Package objectformat holds the fixed rules shared by the canonical chunk
// and manifest physical encoders. It intentionally has no configuration or
// runtime policy surface.
package objectformat

const (
	CompressionMinSavings  uint64 = 4 << 10
	DeltaMinSavings        uint64 = 4 << 10
	MaxChunkDecodedSize           = 64 << 20
	MaxManifestDecodedSize        = 64 << 20
)

// CompressionBeneficial applies the canonical adaptive encoding rule without
// multiplying attacker-controlled lengths: save at least 4 KiB and encode to
// no more than 75% of the original size.
func CompressionBeneficial(rawSize, encodedSize uint64) bool {
	if encodedSize > rawSize || rawSize-encodedSize < CompressionMinSavings {
		return false
	}
	quotient, remainder := rawSize/4, rawSize%4
	return encodedSize <= quotient*3+(remainder*3)/4
}

// DeltaBeneficial is the canonical acceptance rule for storing a chunk as a
// delta object instead of an independent object: the delta object must save at
// least 4 KiB AND at least 20% relative to the independent object the same
// chunk would otherwise have produced (independentSize is that object's full
// size, format byte included; deltaSize is the delta object's full size).
//
// It is deliberately not configurable. A writer cannot loosen it, and readers
// enforce the weaker DeltaPlausible bound so a stored object that no conforming
// writer could have produced is rejected.
//
// The 80% comparison uses the division form so adversarial uint64 inputs
// cannot overflow: floor(n*4/5) = (n/5)*4 + ((n%5)*4)/5.
func DeltaBeneficial(independentSize, deltaSize uint64) bool {
	if deltaSize > independentSize || independentSize-deltaSize < DeltaMinSavings {
		return false
	}
	quotient, remainder := independentSize/5, independentSize%5
	return deltaSize <= quotient*4+(remainder*4)/5
}

// DeltaPlausible is the reader-side sanity bound for a delta object. The
// largest independent object a chunk can produce is a RAW object of
// plaintextSize+1 bytes, so any conforming delta object is at least
// DeltaMinSavings smaller than that. objectSize is the full delta object size
// including its header.
func DeltaPlausible(plaintextSize, objectSize uint64) bool {
	rawObject := plaintextSize + 1
	return objectSize <= rawObject && rawObject-objectSize >= DeltaMinSavings
}
