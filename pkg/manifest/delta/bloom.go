package delta

import (
	"encoding/binary"
	"math"
	"sync/atomic"
)

// bloom is a fixed-size Bloom filter over 32-byte chunk hashes, safe for
// concurrent Add and Has. It answers one question: has this chunk probably been
// stored as an independent object? Has always returns true for a key that Add
// recorded. It can also return true for a key never added, which is harmless
// (see below).
//
// Why a Bloom filter: the known set can hold millions of chunks, and each
// candidate chunk checks it. At the default capacity of 8,000,000 keys, an exact
// set of 32-byte hashes would take about 256 MB per domain. The filter uses 10
// bits per key, about 10 MB, and each check reads a handful of bits with no hash
// table.
//
// Keys are already uniformly random hashes, so the k probe positions are sliced
// straight out of the key (double hashing) with no extra hashing.
//
// Its only job is the "known independent" set. A false positive makes the ingest
// path skip a delta attempt for a chunk that was not actually stored
// independently, costing a missed saving. A false negative, which happens only
// when the filter has lost its state, costs one wasted attempt. Neither affects
// correctness: the store's Resolve decides the representation.
type bloom struct {
	words []atomic.Uint64
	nbits uint64
	k     uint32
	count atomic.Uint64
}

// bloomBitsPerKey sets the filter's size. Ten bits per key gives about a 0.8%
// false-positive rate with k=7 probes.
const bloomBitsPerKey = 10

// newBloom sizes a filter for capacity keys. The bit count is capacity times
// bloomBitsPerKey, rounded up to a multiple of 64 and at least one word.
func newBloom(capacity int) *bloom {
	nbits := uint64(capacity) * bloomBitsPerKey
	nbits = (nbits + 63) &^ 63
	if nbits < 64 {
		nbits = 64
	}
	// k = bits per key × ln 2, rounded. For this ratio, that many probes gives the
	// lowest false-positive rate.
	return &bloom{
		words: make([]atomic.Uint64, nbits/64),
		nbits: nbits,
		k:     uint32(math.Round(bloomBitsPerKey * math.Ln2)),
	}
}

// probes derives the key's probe stride. h1 comes from the first 8 bytes of the
// key and h2 from the next 8. h2 is forced odd. nbits is a multiple of 64, so
// gcd(h2, nbits) is odd and the probe stride cycles through at least 64
// positions, well above k. Overflow past 2^64 breaks the strict progression for
// most keys, but a collision among one key's probes stays rare, about
// k²/(2·nbits) per key.
func (b *bloom) probes(key [32]byte) (h1, h2 uint64) {
	h1 = binary.LittleEndian.Uint64(key[0:8])
	h2 = binary.LittleEndian.Uint64(key[8:16]) | 1
	return
}

// Add records key. After Add, Has(key) returns true.
func (b *bloom) Add(key [32]byte) {
	h1, h2 := b.probes(key)
	for i := uint32(0); i < b.k; i++ {
		bit := (h1 + uint64(i)*h2) % b.nbits
		b.words[bit>>6].Or(1 << (bit & 63))
	}
	b.count.Add(1)
}

// Has reports whether key was probably added. false means it was definitely not
// added. true means it was added or is a false positive.
func (b *bloom) Has(key [32]byte) bool {
	h1, h2 := b.probes(key)
	for i := uint32(0); i < b.k; i++ {
		bit := (h1 + uint64(i)*h2) % b.nbits
		if b.words[bit>>6].Load()&(1<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

// Count is the number of Add calls (not distinct keys).
func (b *bloom) Count() uint64 { return b.count.Load() }
