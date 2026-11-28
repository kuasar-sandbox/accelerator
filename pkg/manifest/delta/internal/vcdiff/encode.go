package vcdiff

import (
	"encoding/binary"
	"io"
	"math/bits"
)

const (
	minMatch     = 4  // shortest COPY the encoder will emit (and the seed length)
	runThreshold = 4  // shortest byte run emitted as a RUN instead of an ADD
	maxChain     = 32 // cap on hash-chain candidates inspected per position
)

// Encoder reuses its internal buffers across calls to avoid per-call
// allocation. Use it to encode many source/target pairs in one process (batch
// or server workloads); for a one-off delta the package-level Encode /
// EncodeBytes are simpler. An Encoder must not be used concurrently.
type Encoder struct {
	u    []byte  // U = source ‖ target
	head []int32 // hash bucket -> newest position
	prev []int32 // position -> previous position in the same bucket
}

// NewEncoder returns a reusable Encoder.
func NewEncoder() *Encoder { return &Encoder{} }

// EncodeBytes returns a VCDIFF delta that reconstructs target from source,
// reusing the Encoder's buffers. A nil/empty source produces a self-referential
// ("pure compression") delta.
func (e *Encoder) EncodeBytes(source, target []byte) []byte {
	out := appendHeader(nil)
	if len(target) > 0 {
		out = append(out, e.encodeWindow(source, target)...)
	}
	return out
}

// Encode writes EncodeBytes(source, target) to out.
func (e *Encoder) Encode(source, target []byte, out io.Writer) error {
	_, err := out.Write(e.EncodeBytes(source, target))
	return err
}

// EncodeBytes returns a VCDIFF delta that reconstructs target from source.
// A nil/empty source produces a self-referential ("pure compression") delta.
func EncodeBytes(source, target []byte) []byte {
	return new(Encoder).EncodeBytes(source, target)
}

// Encode writes EncodeBytes(source, target) to out.
func Encode(source, target []byte, out io.Writer) error {
	return new(Encoder).Encode(source, target, out)
}

// encodeWindow builds a single VCDIFF window covering the whole target. The
// address space U = source ‖ target is searched with a greedy longest-match
// hash index; matches become COPY instructions, byte runs become RUN, and the
// rest becomes ADD literals. The u/head/prev buffers are reused from the
// Encoder (grown only when a larger input needs more room).
func (e *Encoder) encodeWindow(source, target []byte) []byte {
	sLen := len(source)
	n := sLen + len(target)

	if cap(e.u) < n {
		e.u = make([]byte, n)
	} else {
		e.u = e.u[:n]
	}
	u := e.u
	copy(u, source)
	copy(u[sLen:], target)

	// Match index, zlib-style: head[hash(seed)] is the newest position with that
	// hash and prev[pos] chains to the previous one. This replaces a
	// map[uint32][]int (one allocation per position) with two flat slices, which
	// is far faster while producing identical output — chains are walked
	// newest-first and capped at maxChain exact-seed matches, exactly as before
	// (hash collisions are skipped via the seed comparison in findMatch). head is
	// cleared each call; prev needs no clearing since a position's prev is always
	// written (by indexAt) before that position can appear in a chain.
	bits := hashChainBits(n)
	hsize := 1 << bits
	if cap(e.head) < hsize {
		e.head = make([]int32, hsize)
	} else {
		e.head = e.head[:hsize]
	}
	head := e.head
	for i := range head {
		head[i] = -1
	}
	if cap(e.prev) < n {
		e.prev = make([]int32, n)
	} else {
		e.prev = e.prev[:n]
	}
	prev := e.prev
	indexAt := func(pos int) {
		if pos+minMatch <= n {
			h := seedHash(binary.LittleEndian.Uint32(u[pos:pos+minMatch]), bits)
			prev[pos] = head[h]
			head[h] = int32(pos)
		}
	}
	for p := 0; p < sLen; p++ {
		indexAt(p)
	}

	cache := newAddressCache(defaultNearCache, defaultSameCache)
	var dataS, instS, addrS []byte

	litStart := sLen
	flush := func(end int) {
		if end > litStart {
			instS = append(instS, opcodeADD0)
			instS = appendVarint(instS, uint64(end-litStart))
			dataS = append(dataS, u[litStart:end]...)
		}
	}

	i := sLen
	for i < n {
		if mLen, mPos := findMatch(u, head, prev, bits, i, n, sLen); mLen >= minMatch {
			flush(i)
			enc := cache.encode(mPos, i)
			instS = append(instS, opcodeCOPY0(enc.mode))
			instS = appendVarint(instS, uint64(mLen))
			addrS = appendVarint(addrS, enc.val)
			// The copied span is intentionally not re-indexed: identical content
			// is already reachable through the source index (or the literal that
			// first introduced it), so indexing every copied byte roughly doubles
			// the hashing work for no practical gain in match quality.
			i += mLen
			litStart = i
			continue
		}
		if r := runLength(u, i, n); r >= runThreshold {
			flush(i)
			instS = append(instS, opcodeRUN0)
			instS = appendVarint(instS, uint64(r))
			dataS = append(dataS, u[i])
			// A run is rediscovered by runLength directly, so the run span needs
			// no indexing either.
			i += r
			litStart = i
			continue
		}
		indexAt(i)
		i++
	}
	flush(n)

	return assembleWindow(sLen, len(target), dataS, instS, addrS)
}

// hashChainBits picks a power-of-two hash-table size roughly matching the input
// length (capped), keeping collision chains short without wasting memory on
// small inputs. It only affects bucketing/speed, never the encoded output.
func hashChainBits(n int) uint {
	bits := uint(8)
	for (1<<bits) < n && bits < 21 {
		bits++
	}
	return bits
}

// seedHash maps a 4-byte seed to a hash-table bucket (Knuth multiplicative hash,
// high bits).
func seedHash(seed uint32, bits uint) uint32 {
	return (seed * 2654435761) >> (32 - bits)
}

// findMatch returns the longest match (length, U-position) for the seed at i,
// or (0, -1) if none reaches minMatch's seed length. The hash chain is walked
// newest-first; hash collisions (a different seed in the same bucket) are
// skipped, and at most maxChain exact-seed candidates are inspected.
func findMatch(u []byte, head, prev []int32, bits uint, i, n, sLen int) (int, int) {
	if i+minMatch > n {
		return 0, -1
	}
	seed := binary.LittleEndian.Uint32(u[i : i+minMatch])
	bestLen, bestPos := 0, -1
	count := 0
	for p := head[seedHash(seed, bits)]; p >= 0 && count < maxChain; p = prev[p] {
		pos := int(p)
		if binary.LittleEndian.Uint32(u[pos:pos+minMatch]) != seed {
			continue // hash collision: different seed in this bucket
		}
		count++
		if l := matchLen(u, pos, i, n, sLen); l > bestLen {
			bestLen, bestPos = l, pos
		}
	}
	return bestLen, bestPos
}

// matchLen returns how many bytes of u match starting at p versus i (p < i),
// stopping at the end of u. A match that begins in the source window is capped
// at the source/target boundary: COPY addresses that start in the source must
// stay within it, because xdelta3 (the reference decoder) reads a source-window
// copy from the source buffer and rejects one that runs past its end. Matches
// that begin in the target window may overlap forward freely (the standard
// run-length self-reference), which xdelta3 supports.
func matchLen(u []byte, p, i, n, sLen int) int {
	limit := n
	if p < sLen {
		limit = sLen
	}
	// maxL caps the comparison so a source-window match stays in the source
	// window and neither side reads past the end of u.
	maxL := n - i
	if limit-p < maxL {
		maxL = limit - p
	}
	l := 0
	// Compare 8 bytes at a time; the first non-zero XOR locates the mismatch.
	for l+8 <= maxL {
		if x := binary.LittleEndian.Uint64(u[p+l:]) ^ binary.LittleEndian.Uint64(u[i+l:]); x != 0 {
			return l + bits.TrailingZeros64(x)/8
		}
		l += 8
	}
	for l < maxL && u[p+l] == u[i+l] {
		l++
	}
	return l
}

// runLength returns the count of bytes equal to u[i] starting at i.
func runLength(u []byte, i, n int) int {
	r := 1
	for i+r < n && u[i+r] == u[i] {
		r++
	}
	return r
}

// assembleWindow serializes a VCD_SOURCE (or sourceless) window from the three
// prepared sections.
func assembleWindow(sLen, targetLen int, dataS, instS, addrS []byte) []byte {
	// The delta-encoding body: target-window size, Delta_Indicator, the three
	// section lengths, then the three sections.
	body := appendVarint(nil, uint64(targetLen))
	body = append(body, 0x00) // Delta_Indicator: no per-section compression
	body = appendVarint(body, uint64(len(dataS)))
	body = appendVarint(body, uint64(len(instS)))
	body = appendVarint(body, uint64(len(addrS)))
	body = append(body, dataS...)
	body = append(body, instS...)
	body = append(body, addrS...)

	var win []byte
	if sLen > 0 {
		win = append(win, winSource)
		win = appendVarint(win, uint64(sLen)) // source segment length
		win = appendVarint(win, 0)            // source segment position
	} else {
		win = append(win, 0x00)
	}
	win = appendVarint(win, uint64(len(body))) // length of the delta encoding
	win = append(win, body...)
	return win
}
