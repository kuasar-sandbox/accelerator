package ec

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// defaultTableSize is the Maglev lookup table size. Must be prime.
// 65537 supports up to ~500 nodes at <1% balance variance.
const defaultTableSize = 65537

// maglevTable is a Maglev consistent-hash lookup table that maps keys
// to nodes with near-perfect balance and minimal disruption on node
// changes. buildMaglev constructs the table; LocateN selects N distinct
// nodes for a key by walking the table from a hash-derived start
// position.
type maglevTable struct {
	table []int    // len = tableSize; table[i] = index into nodes
	nodes []string // sorted node IDs
	size  int      // tableSize (prime)
}

// buildMaglev constructs a Maglev lookup table from the given node IDs.
// tableSize must be a prime number significantly larger than len(nodes).
//
// Nodes are sorted internally so the table depends only on the SET of
// node IDs, not the caller's slice order. This prevents YAML peer-array
// reordering from triggering a full remap.
func buildMaglev(nodes []string, tableSize int) *maglevTable {
	sorted := make([]string, len(nodes))
	copy(sorted, nodes)
	sort.Strings(sorted)
	n := len(sorted)

	if n == 0 {
		return &maglevTable{table: nil, nodes: nil, size: tableSize}
	}

	// Two independent hashes per node derive a permutation of table
	// positions:
	//   preference[j] = (offset + j*skip) % tableSize
	//
	// h1 = FNV-1a(name)          → offset in [0, tableSize)
	// h2 = FNV-1a(0xFF || name)  → skip in [1, tableSize-1]
	//
	// skip is always coprime with tableSize because tableSize is prime
	// and 1 ≤ skip < tableSize → gcd(skip, tableSize) = 1. This
	// guarantees each node visits all table positions.
	offset := make([]int, n)
	skip := make([]int, n)
	for i, name := range sorted {
		offset[i] = int(fnvHash(name) % uint64(tableSize))
		skip[i] = int(fnvHashSalted(name) % uint64(tableSize-1)) + 1
	}

	table := make([]int, tableSize)
	for i := range table {
		table[i] = -1
	}

	// Round-robin: each node takes turns claiming the next empty slot
	// in its preference list. Terminates when the table is full.
	next := make([]int, n)
	filled := 0
	for filled < tableSize {
		for i := 0; i < n; i++ {
			c := (offset[i] + next[i]*skip[i]) % tableSize
			for table[c] != -1 {
				next[i]++
				c = (offset[i] + next[i]*skip[i]) % tableSize
			}
			table[c] = i
			next[i]++
			filled++
			if filled == tableSize {
				break
			}
		}
	}

	return &maglevTable{table: table, nodes: sorted, size: tableSize}
}

// LocateN returns N distinct node IDs for the given key by walking the
// lookup table from a hash-derived start position. The walk collects
// nodes in encounter order, producing a key-dependent permutation when
// N == len(nodes). Returns error if n > len(nodes).
func (m *maglevTable) LocateN(key []byte, n int) ([]string, error) {
	if n > len(m.nodes) {
		return nil, fmt.Errorf("maglev: need %d nodes but only %d in table", n, len(m.nodes))
	}
	result := make([]string, 0, n)
	seen := make([]bool, len(m.nodes))
	idx := int(fnvHashBytes(key) % uint64(m.size))
	for len(result) < n {
		nodeIdx := m.table[idx]
		if !seen[nodeIdx] {
			seen[nodeIdx] = true
			result = append(result, m.nodes[nodeIdx])
		}
		idx = (idx + 1) % m.size
	}
	return result, nil
}

// fnvHash returns a 64-bit FNV-1a hash of s (used by buildMaglev for node names).
func fnvHash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// fnvHashBytes returns a 64-bit FNV-1a hash of b (used by LocateN for content keys).
func fnvHashBytes(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// fnvHashSalted returns a 64-bit FNV-1a hash of s with a 0xFF prefix
// salt, producing an independent hash from fnvHash for the same input.
func fnvHashSalted(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte{0xff})
	h.Write([]byte(s))
	return h.Sum64()
}
