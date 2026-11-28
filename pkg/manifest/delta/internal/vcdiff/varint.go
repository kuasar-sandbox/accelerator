package vcdiff

import (
	"fmt"
	"io"
	"math"
)

// VCDIFF integers are unsigned, big-endian base-128: the value is split into
// 7-bit groups emitted most-significant group first, and every byte except the
// last has its high bit (0x80) set. For example 123456789 -> BA EF 9A 15.

// appendVarint appends v to dst in the VCDIFF integer encoding.
func appendVarint(dst []byte, v uint64) []byte {
	var tmp [10]byte
	i := len(tmp) - 1
	tmp[i] = byte(v & 0x7f) // least-significant group, no continuation bit
	for v >>= 7; v != 0; v >>= 7 {
		i--
		tmp[i] = byte(v&0x7f) | 0x80
	}
	return append(dst, tmp[i:]...)
}

// varintLen reports how many bytes appendVarint would emit for v.
func varintLen(v uint64) int {
	n := 1
	for v >>= 7; v != 0; v >>= 7 {
		n++
	}
	return n
}

// readVarint reads one VCDIFF integer from r.
func readVarint(r io.ByteReader) (uint64, error) {
	var v uint64
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if v > math.MaxUint64>>7 {
			return 0, fmt.Errorf("vcdiff: varint overflow")
		}
		v = (v << 7) | uint64(b&0x7f)
		if b&0x80 == 0 {
			return v, nil
		}
	}
}

// byteCursor reads sequentially from an in-memory section, satisfying
// io.ByteReader for readVarint and single-byte address reads.
type byteCursor struct {
	b []byte
	i int
}

func (c *byteCursor) ReadByte() (byte, error) {
	if c.i >= len(c.b) {
		return 0, io.EOF
	}
	v := c.b[c.i]
	c.i++
	return v, nil
}

// remaining reports how many unread bytes are left in the cursor.
func (c *byteCursor) remaining() int { return len(c.b) - c.i }
