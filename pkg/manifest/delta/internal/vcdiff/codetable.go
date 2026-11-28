package vcdiff

// VCDIFF instruction types (RFC 3284 §5.4).
const (
	instNOOP byte = 0
	instADD  byte = 1
	instRUN  byte = 2
	instCOPY byte = 3
)

// codeTableEntry maps a single instruction opcode to one or two instructions.
// A size of 0 means the size is not baked into the opcode and must be read as a
// separate integer from the instructions section (inst1's first, then inst2's).
type codeTableEntry struct {
	inst1, size1, mode1 byte
	inst2, size2, mode2 byte
}

// defaultCodeTable builds the RFC 3284 §5.6 default instruction code table,
// which assumes near_cache_size = 4 and same_cache_size = 3 (nine COPY modes,
// 0..8). The construction walks an index counter through the canonical blocks,
// producing the same 256 entries as open-vcdiff's kDefaultCodeTableData.
func defaultCodeTable() *[256]codeTableEntry {
	var t [256]codeTableEntry
	i := 0
	set := func(e codeTableEntry) {
		t[i] = e
		i++
	}

	// 0: RUN with a separate size.
	set(codeTableEntry{instRUN, 0, 0, instNOOP, 0, 0})

	// 1..18: single ADD, sizes 0 then 1..17.
	for s := 0; s <= 17; s++ {
		set(codeTableEntry{instADD, byte(s), 0, instNOOP, 0, 0})
	}

	// 19..162: single COPY for each of the 9 modes, sizes 0 then 4..18.
	for m := 0; m < 9; m++ {
		set(codeTableEntry{instCOPY, 0, byte(m), instNOOP, 0, 0})
		for s := 4; s <= 18; s++ {
			set(codeTableEntry{instCOPY, byte(s), byte(m), instNOOP, 0, 0})
		}
	}

	// 163..234: ADD(1..4) + COPY(4..6) for modes 0..5.
	for m := 0; m < 6; m++ {
		for a := 1; a <= 4; a++ {
			for cs := 4; cs <= 6; cs++ {
				set(codeTableEntry{instADD, byte(a), 0, instCOPY, byte(cs), byte(m)})
			}
		}
	}

	// 235..246: ADD(1..4) + COPY(4) for modes 6,7,8.
	for m := 6; m < 9; m++ {
		for a := 1; a <= 4; a++ {
			set(codeTableEntry{instADD, byte(a), 0, instCOPY, 4, byte(m)})
		}
	}

	// 247..255: COPY(4, mode) + ADD(1) for modes 0..8.
	for m := 0; m < 9; m++ {
		set(codeTableEntry{instCOPY, 4, byte(m), instADD, 1, 0})
	}

	return &t
}

// Opcodes the encoder emits: single instructions with an explicit size integer.
const (
	opcodeRUN0 byte = 0 // RUN, size read separately
	opcodeADD0 byte = 1 // ADD, size read separately
)

// opcodeCOPY0 is the size-0 COPY opcode for the given address mode.
func opcodeCOPY0(mode byte) byte { return 19 + 16*mode }
