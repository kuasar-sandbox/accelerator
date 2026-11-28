package vcdiff

import (
	"fmt"
	"io"
)

// COPY address modes that are always present (RFC 3284 §5.3).
const (
	modeSelf byte = 0 // address is an absolute integer in U
	modeHere byte = 1 // address is encoded as here-addr
)

// addressCache implements the VCDIFF near/same address caches (RFC 3284 §5.1).
// Modes 2..2+sNear-1 are the near cache; modes 2+sNear..2+sNear+sSame-1 are the
// same cache. The cache must be reset at the start of every window and updated
// after every COPY address, in the same order by the encoder and the decoder.
type addressCache struct {
	sNear, sSame int
	near         []int
	same         []int
	nextSlot     int
}

func newAddressCache(sNear, sSame int) *addressCache {
	return &addressCache{
		sNear: sNear,
		sSame: sSame,
		near:  make([]int, sNear),
		same:  make([]int, sSame*256),
	}
}

// reset clears the cache for a new window.
func (c *addressCache) reset() {
	c.nextSlot = 0
	for i := range c.near {
		c.near[i] = 0
	}
	for i := range c.same {
		c.same[i] = 0
	}
}

// update records addr after a COPY, advancing both caches.
func (c *addressCache) update(addr int) {
	if c.sNear > 0 {
		c.near[c.nextSlot] = addr
		c.nextSlot = (c.nextSlot + 1) % c.sNear
	}
	if c.sSame > 0 {
		c.same[addr%(c.sSame*256)] = addr
	}
}

// decode reads one COPY address of the given mode from r (the addresses
// section), using here as the current position in U, and updates the cache.
func (c *addressCache) decode(here int, mode byte, r io.ByteReader) (int, error) {
	var addr int
	switch {
	case mode == modeSelf:
		v, err := readVarint(r)
		if err != nil {
			return 0, err
		}
		addr = int(v)
	case mode == modeHere:
		v, err := readVarint(r)
		if err != nil {
			return 0, err
		}
		addr = here - int(v)
	case int(mode)-2 < c.sNear:
		v, err := readVarint(r)
		if err != nil {
			return 0, err
		}
		addr = c.near[int(mode)-2] + int(v)
	default: // same cache: a single raw byte indexes the sub-table
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		m := int(mode) - (2 + c.sNear)
		addr = c.same[m*256+int(b)]
	}
	if addr < 0 {
		return 0, fmt.Errorf("vcdiff: negative COPY address %d", addr)
	}
	c.update(addr)
	return addr, nil
}

// addrEncoding is the encoder's chosen representation of a COPY address: a mode
// plus the varint to emit in the addresses section.
//
// Only the SELF/HERE/near modes are produced. The same-cache modes are never
// emitted because this encoder's greedy matcher re-indexes every copied region,
// so a repeated reference always resolves to the most recent occurrence (a fresh
// target position that was never itself a COPY source address) rather than an
// older, same-cached address. SELF/HERE/near always suffice, and the result is
// fully standard VCDIFF — open-vcdiff (which does emit same-mode) decodes it,
// and our decoder handles same-mode for the reverse direction.
type addrEncoding struct {
	mode byte
	val  uint64
}

// encode picks the cheapest of SELF/HERE/near for addr (given the current here)
// and updates the cache, mirroring decode's update order. addr is always < here.
func (c *addressCache) encode(addr, here int) addrEncoding {
	best := addrEncoding{mode: modeSelf, val: uint64(addr)}
	bestCost := varintLen(uint64(addr))

	if d := here - addr; varintLen(uint64(d)) < bestCost {
		best = addrEncoding{mode: modeHere, val: uint64(d)}
		bestCost = varintLen(uint64(d))
	}

	for j := 0; j < c.sNear; j++ {
		if addr >= c.near[j] {
			d := addr - c.near[j]
			if varintLen(uint64(d)) < bestCost {
				best = addrEncoding{mode: byte(2 + j), val: uint64(d)}
				bestCost = varintLen(uint64(d))
			}
		}
	}

	c.update(addr)
	return best
}
