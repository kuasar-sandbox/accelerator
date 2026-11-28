package vcdiff

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
)

// Decode reads a VCDIFF delta from delta, reconstructs the target using source,
// writes it to out, and returns the number of target bytes produced.
func Decode(source []byte, delta io.Reader, out io.Writer) (int64, error) {
	return decode(source, delta, out, Limits{MaxTarget: -1, MaxSection: -1})
}

// Limits bounds the work and memory a decode may demand of an untrusted delta.
// A negative field means unlimited.
type Limits struct {
	// MaxTarget caps the total number of target bytes across all windows.
	MaxTarget int64
	// MaxSection caps the declared size of any one window section (data,
	// instructions, addresses). A section cannot legitimately exceed the
	// delta stream that carries it, so callers pass the stream length.
	MaxSection int64
}

// DecodeLimited is Decode with explicit resource limits. LOCAL MODIFICATION:
// not present in upstream; see NOTICE.md.
func DecodeLimited(source []byte, delta io.Reader, out io.Writer, lim Limits) (int64, error) {
	return decode(source, delta, out, lim)
}

func decode(source []byte, delta io.Reader, out io.Writer, lim Limits) (int64, error) {
	br := bufio.NewReader(delta)
	if err := readHeader(br); err != nil {
		return 0, err
	}
	table := defaultCodeTable()
	cache := newAddressCache(defaultNearCache, defaultSameCache)

	var total int64
	var targetSoFar []byte
	for {
		winInd, err := br.ReadByte()
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, fmt.Errorf("vcdiff: read window indicator: %w", err)
		}
		win, err := decodeWindow(br, winInd, source, targetSoFar, table, cache, total, lim)
		if err != nil {
			return total, err
		}
		if _, err := out.Write(win); err != nil {
			return total, err
		}
		targetSoFar = append(targetSoFar, win...)
		total += int64(len(win))
	}
}

// decodeWindow decodes one window and returns the target bytes it produces.
func decodeWindow(br *bufio.Reader, winInd byte, source, targetSoFar []byte,
	table *[256]codeTableEntry, cache *addressCache, totalSoFar int64, lim Limits) ([]byte, error) {
	if winInd&^(winSource|winTarget|winChecksum) != 0 {
		return nil, fmt.Errorf("vcdiff: unsupported Win_Indicator 0x%02x", winInd)
	}
	if winInd&winSource != 0 && winInd&winTarget != 0 {
		return nil, fmt.Errorf("vcdiff: both VCD_SOURCE and VCD_TARGET set")
	}

	var s []byte
	if winInd&(winSource|winTarget) != 0 {
		segLen, err := readVarint(br)
		if err != nil {
			return nil, fmt.Errorf("vcdiff: read source segment length: %w", err)
		}
		segPos, err := readVarint(br)
		if err != nil {
			return nil, fmt.Errorf("vcdiff: read source segment position: %w", err)
		}
		base := source
		if winInd&winTarget != 0 {
			base = targetSoFar
		}
		if s, err = sliceSegment(base, segPos, segLen); err != nil {
			return nil, err
		}
	}

	if _, err := readVarint(br); err != nil { // length of the delta encoding (skip hint)
		return nil, fmt.Errorf("vcdiff: read delta-encoding length: %w", err)
	}
	targetLen, err := readVarint(br)
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read target window size: %w", err)
	}
	if lim.MaxTarget >= 0 && (targetLen > uint64(lim.MaxTarget) || int64(targetLen) > lim.MaxTarget-totalSoFar) {
		return nil, fmt.Errorf("vcdiff: target window %d exceeds limit %d", targetLen, lim.MaxTarget)
	}
	deltaInd, err := br.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read delta indicator: %w", err)
	}
	if deltaInd&deltaCompMask != 0 {
		return nil, fmt.Errorf("vcdiff: secondary compression not supported (Delta_Indicator 0x%02x)", deltaInd)
	}
	dataLen, err := readVarint(br)
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read data length: %w", err)
	}
	instLen, err := readVarint(br)
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read instructions length: %w", err)
	}
	addrLen, err := readVarint(br)
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read addresses length: %w", err)
	}
	if lim.MaxSection >= 0 {
		for _, n := range [3]uint64{dataLen, instLen, addrLen} {
			if n > uint64(lim.MaxSection) {
				return nil, fmt.Errorf("vcdiff: section length %d exceeds limit %d", n, lim.MaxSection)
			}
		}
	}

	var checksum uint32
	if winInd&winChecksum != 0 {
		var cb [4]byte
		if _, err := io.ReadFull(br, cb[:]); err != nil {
			return nil, fmt.Errorf("vcdiff: read checksum: %w", err)
		}
		checksum = binary.BigEndian.Uint32(cb[:])
	}

	data, err := readSection(br, dataLen)
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read data section: %w", err)
	}
	inst, err := readSection(br, instLen)
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read instructions section: %w", err)
	}
	addr, err := readSection(br, addrLen)
	if err != nil {
		return nil, fmt.Errorf("vcdiff: read addresses section: %w", err)
	}

	t, err := runInstructions(s, int(targetLen), table, cache, data, inst, addr)
	if err != nil {
		return nil, err
	}
	if winInd&winChecksum != 0 && adler32.Checksum(t) != checksum {
		return nil, fmt.Errorf("vcdiff: window checksum mismatch")
	}
	return t, nil
}

// readSection reads exactly n bytes into a fresh slice.
func readSection(br *bufio.Reader, n uint64) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(br, b)
	return b, err
}

// runInstructions executes a window's instruction stream against U = s ‖ t,
// returning the produced target window t.
func runInstructions(s []byte, targetLen int, table *[256]codeTableEntry,
	cache *addressCache, dataB, instB, addrB []byte) ([]byte, error) {
	cache.reset()
	data := &byteCursor{b: dataB}
	inst := &byteCursor{b: instB}
	addr := &byteCursor{b: addrB}
	t := make([]byte, 0, targetLen)

	for len(t) < targetLen {
		op, err := inst.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("vcdiff: truncated instructions")
		}
		e := table[op]
		for _, ins := range [2]struct{ inst, size, mode byte }{
			{e.inst1, e.size1, e.mode1},
			{e.inst2, e.size2, e.mode2},
		} {
			if ins.inst == instNOOP {
				continue
			}
			size := int(ins.size)
			if size == 0 {
				v, err := readVarint(inst)
				if err != nil {
					return nil, fmt.Errorf("vcdiff: read instruction size: %w", err)
				}
				size = int(v)
			}
			// LOCAL MODIFICATION: an instruction may never grow the window past
			// its declared size. Without this a hostile RUN/COPY length drives
			// unbounded allocation before the final length check.
			if size < 0 || size > targetLen-len(t) {
				return nil, fmt.Errorf("vcdiff: instruction size %d overruns window (%d of %d bytes produced)", size, len(t), targetLen)
			}
			if t, err = execInstruction(t, s, ins.inst, ins.mode, size, cache, data, addr); err != nil {
				return nil, err
			}
		}
	}
	if len(t) != targetLen {
		return nil, fmt.Errorf("vcdiff: window produced %d bytes, want %d", len(t), targetLen)
	}
	if data.remaining() != 0 || inst.remaining() != 0 || addr.remaining() != 0 {
		return nil, fmt.Errorf("vcdiff: window sections not fully consumed")
	}
	return t, nil
}

// execInstruction applies one ADD/RUN/COPY instruction to t.
func execInstruction(t, s []byte, inst, mode byte, size int,
	cache *addressCache, data, addr *byteCursor) ([]byte, error) {
	switch inst {
	case instADD:
		if data.remaining() < size {
			return nil, fmt.Errorf("vcdiff: ADD past end of data section")
		}
		t = append(t, data.b[data.i:data.i+size]...)
		data.i += size
	case instRUN:
		b, err := data.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("vcdiff: RUN past end of data section")
		}
		for k := 0; k < size; k++ {
			t = append(t, b)
		}
	default: // instCOPY
		here := len(s) + len(t)
		a, err := cache.decode(here, mode, addr)
		if err != nil {
			return nil, fmt.Errorf("vcdiff: read COPY address: %w", err)
		}
		switch {
		case a+size <= len(s):
			// Entirely within the source window: bulk copy.
			t = append(t, s[a:a+size]...)
		case a >= len(s) && a+size <= here:
			// Entirely within already-produced target, no overlap: bulk copy.
			ti := a - len(s)
			t = append(t, t[ti:ti+size]...)
		default:
			// Overlapping self-reference (run-length style) or a span crossing
			// the source/target boundary: copy byte by byte so bytes produced
			// during this instruction become visible to it.
			for k := 0; k < size; k++ {
				x := a + k
				var b byte
				if x < len(s) {
					b = s[x]
				} else {
					ti := x - len(s)
					if ti >= len(t) {
						return nil, fmt.Errorf("vcdiff: COPY address %d out of range", x)
					}
					b = t[ti]
				}
				t = append(t, b)
			}
		}
	}
	return t, nil
}
