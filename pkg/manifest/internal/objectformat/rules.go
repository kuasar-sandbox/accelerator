// Package objectformat holds the fixed rules shared by the canonical chunk
// and manifest physical encoders. It intentionally has no configuration or
// runtime policy surface.
package objectformat

const (
	CompressionMinSavings  uint64 = 4 << 10
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
