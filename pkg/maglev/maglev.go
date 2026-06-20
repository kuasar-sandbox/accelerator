// Package maglev implements Maglev consistent hashing: it maps a key to
// N distinct members of a set with near-perfect balance and minimal
// disruption when the member set changes.
//
// It is used both by the L2 cache (locating the N erasure-coded shard
// peers for a chunk, pkg/cache/ec) and by the cluster scaler
// (shuffle-sharding a sandbox-group onto N slots). Build a Table once
// from a fixed member set and call its LocateN method repeatedly, or use
// the package-level LocateN convenience when the member set is supplied
// per call.
package maglev

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// defaultTableSize is the Maglev lookup table size. Must be prime.
// 65537 supports up to ~500 members at <1% balance variance.
const defaultTableSize = 65537

// Table is a Maglev consistent-hash lookup table that maps keys to
// members with near-perfect balance and minimal disruption on member
// changes. New constructs the table; LocateN selects N distinct members
// for a key by walking the table from a hash-derived start position. A
// Table is immutable after construction and safe for concurrent use.
type Table struct {
	table []int    // len = size; table[i] = index into nodes
	nodes []string // sorted member IDs
	size  int      // table size (prime)
}

// New builds a Maglev table from the given member IDs. Members are
// sorted internally, so the table depends only on the SET of members,
// not the caller's slice order. This keeps the table stable against
// config/slice reordering.
func New(members []string) *Table {
	return buildMaglev(members, defaultTableSize)
}

// LocateN builds a Maglev table from members and returns the n distinct
// members responsible for key. It is a convenience for callers whose
// member set varies per call (e.g. shuffle-sharding a group onto a
// changing slot set); when the member set is stable, build a Table once
// with New and call its LocateN method to avoid rebuilding per lookup.
func LocateN(key []byte, members []string, n int) ([]string, error) {
	return New(members).LocateN(key, n)
}

// buildMaglev constructs a Maglev lookup table from the given member IDs.
// tableSize must be a prime number significantly larger than len(nodes).
//
// Nodes are sorted internally so the table depends only on the SET of
// member IDs, not the caller's slice order. This prevents array
// reordering from triggering a full remap.
func buildMaglev(nodes []string, tableSize int) *Table {
	sorted := make([]string, len(nodes))
	copy(sorted, nodes)
	sort.Strings(sorted)
	n := len(sorted)

	if n == 0 {
		return &Table{table: nil, nodes: nil, size: tableSize}
	}

	// Two independent hashes per member derive a permutation of table
	// positions:
	//   preference[j] = (offset + j*skip) % tableSize
	//
	// h1 = FNV-1a(name)          → offset in [0, tableSize)
	// h2 = FNV-1a(0xFF || name)  → skip in [1, tableSize-1]
	//
	// skip is always coprime with tableSize because tableSize is prime
	// and 1 ≤ skip < tableSize → gcd(skip, tableSize) = 1. This
	// guarantees each member visits all table positions.
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

	// Round-robin: each member takes turns claiming the next empty slot
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

	return &Table{table: table, nodes: sorted, size: tableSize}
}

// LocateN returns n distinct member IDs for the given key by walking the
// lookup table from a hash-derived start position. The walk collects
// members in encounter order, producing a key-dependent permutation when
// n == len(members). Returns an error if n exceeds the member count.
func (m *Table) LocateN(key []byte, n int) ([]string, error) {
	if n > len(m.nodes) {
		return nil, fmt.Errorf("maglev: need %d members but only %d in table", n, len(m.nodes))
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

// fnvHash returns a 64-bit FNV-1a hash of s (used by buildMaglev for member names).
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
