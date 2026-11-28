package delta

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
)

// Sketch is a one-permutation min-hash of a chunk's feature pieces.
//
// The chunk is cut into small content-defined pieces; each piece is hashed to a
// uint64; the top log2(k) bits of that hash pick one of k slots and each slot
// keeps the smallest hash it saw. Two chunks agree in a slot with probability
// equal to the Jaccard similarity of their piece sets, so the fraction of
// agreeing slots estimates similarity from k numbers instead of the full sets.
type Sketch struct {
	K int
	// Min holds each slot's smallest hash, or emptySlot if no piece fell there.
	Min [MaxSlots]uint64
}

const emptySlot = ^uint64(0)

// Sketcher computes sketches. It is stateless after construction and safe for
// concurrent use.
type Sketcher struct {
	cutter    *chunker.FeatureCutter
	k         int
	slotShift uint // 64 - log2(k); a shift of 64 yields slot 0 for k == 1
	g         int
}

// NewSketcher builds a Sketcher from validated Params.
func NewSketcher(p Params) (*Sketcher, error) {
	c, err := chunker.NewFeatureCutter(p.FeatMin, p.FeatAvg, p.FeatMax)
	if err != nil {
		return nil, err
	}
	return &Sketcher{
		cutter:    c,
		k:         p.Slots,
		slotShift: uint(64 - bits.TrailingZeros(uint(p.Slots))),
		g:         p.GroupSize,
	}, nil
}

// Sketch computes the min-hash sketch of data.
func (s *Sketcher) Sketch(data []byte) Sketch {
	sk := Sketch{K: s.k}
	for i := 0; i < s.k; i++ {
		sk.Min[i] = emptySlot
	}
	s.cutter.Split(data, func(piece []byte) {
		sum := sha256.Sum256(piece)
		h := binary.BigEndian.Uint64(sum[:8])
		slot := int(h >> s.slotShift)
		if h < sk.Min[slot] {
			sk.Min[slot] = h
		}
	})
	return sk
}

// SuperFeatures folds the sketch into s = k/g super-features, each a hash of g
// consecutive slot minima. A chunk pair shares a super-feature iff all g slots
// agree, so the chance two chunks of Jaccard similarity J share at least one is
// 1-(1-J^g)^s. A group containing an empty slot cannot be trusted and yields
// no feature (ok=false for that index).
func (s *Sketcher) SuperFeatures(sk Sketch, out []uint64) []uint64 {
	out = out[:0]
	n := sk.K / s.g
	var buf [len("accelerator-sf/v1") + 8 + 8*MaxSlots]byte
	for j := 0; j < n; j++ {
		b := buf[:0]
		b = append(b, "accelerator-sf/v1"...)
		b = binary.LittleEndian.AppendUint64(b, uint64(j))
		empty := false
		for t := 0; t < s.g; t++ {
			v := sk.Min[j*s.g+t]
			if v == emptySlot {
				empty = true
				break
			}
			b = binary.LittleEndian.AppendUint64(b, v)
		}
		if empty {
			out = append(out, 0)
			continue
		}
		sum := sha256.Sum256(b)
		f := binary.LittleEndian.Uint64(sum[:8])
		if f == 0 {
			f = 1 // 0 is reserved for "no feature"
		}
		out = append(out, f)
	}
	return out
}

// Compact is the per-base form kept in the similarity index: one 16-bit
// fingerprint per slot (0 reserved for empty is avoided by tracking emptiness
// in a bitmask), 34 bytes for k=16 instead of 128.
type Compact struct {
	FP    [MaxSlots]uint16
	Empty uint64 // bit i set => slot i empty
	K     uint8
}

// Compact reduces a sketch for storage in the index.
func (sk Sketch) Compact() Compact {
	c := Compact{K: uint8(sk.K)}
	for i := 0; i < sk.K; i++ {
		if sk.Min[i] == emptySlot {
			c.Empty |= 1 << i
			continue
		}
		c.FP[i] = uint16(sk.Min[i])
	}
	return c
}

// Similarity estimates Jaccard similarity between a full sketch and a stored
// compact sketch. A slot empty in both is uninformative and excluded; a slot
// empty in only one counts as a mismatch. The estimator's standard error is
// about sqrt(J(1-J)/k), +-0.125 at J=0.5, k=16, which is why candidates are
// ranked by it and then confirmed by actually encoding, never trusted alone.
func (c Compact) Similarity(sk Sketch) float64 {
	if int(c.K) != sk.K {
		return 0
	}
	match, denom := 0, 0
	for i := 0; i < sk.K; i++ {
		ce := c.Empty&(1<<i) != 0
		se := sk.Min[i] == emptySlot
		switch {
		case ce && se:
			continue
		case ce || se:
			denom++
		default:
			denom++
			if c.FP[i] == uint16(sk.Min[i]) {
				match++
			}
		}
	}
	if denom == 0 {
		return 0
	}
	return float64(match) / float64(denom)
}
