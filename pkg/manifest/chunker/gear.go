package chunker

// gearTable is a 256-entry lookup table for the Gear rolling hash.
// Generated deterministically from seed 0x12345678 using an LCG
// (multiplier 6364136223846793005, increment 1442695040888963407).
var gearTable [256]uint64

func init() {
	var state uint64 = 0x12345678
	for i := range gearTable {
		state = state*6364136223846793005 + 1442695040888963407
		gearTable[i] = state
	}
}

// gearHash computes one step of the Gear rolling hash.
//
//	hash = (hash << 1) + gearTable[b]
func gearHash(hash uint64, b byte) uint64 {
	return (hash << 1) + gearTable[b]
}
