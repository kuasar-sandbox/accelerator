package ec

import (
	"fmt"
	"math"
	"testing"
)

func testNodes(n int) []string {
	nodes := make([]string, n)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("node-%02d", i)
	}
	return nodes
}

// TestBuildMaglev_TableBalance verifies that each node owns exactly
// TableSize/M ± 1 positions in the lookup table.
func TestBuildMaglev_TableBalance(t *testing.T) {
	for _, m := range []int{3, 5, 7, 10, 20} {
		t.Run(fmt.Sprintf("M=%d", m), func(t *testing.T) {
			tbl := buildMaglev(testNodes(m), defaultTableSize)
			counts := make([]int, m)
			for _, idx := range tbl.table {
				counts[idx]++
			}
			expected := defaultTableSize / m
			for i, c := range counts {
				diff := c - expected
				if diff < -1 || diff > 1 {
					t.Errorf("node %d: %d positions (expected %d ± 1)", i, c, expected)
				}
			}
		})
	}
}

// TestLocateN_Distinctness verifies that LocateN returns N distinct nodes.
func TestLocateN_Distinctness(t *testing.T) {
	tbl := buildMaglev(testNodes(5), defaultTableSize)
	for n := 1; n <= 5; n++ {
		ids, err := tbl.LocateN([]byte("test-key"), n)
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != n {
			t.Fatalf("LocateN(%d): got %d nodes", n, len(ids))
		}
		seen := make(map[string]bool)
		for _, id := range ids {
			if seen[id] {
				t.Fatalf("LocateN(%d): duplicate node %s", n, id)
			}
			seen[id] = true
		}
	}
}

// TestLocateN_Deterministic verifies the same key always produces the
// same result with the same node set.
func TestLocateN_Deterministic(t *testing.T) {
	tbl := buildMaglev(testNodes(5), defaultTableSize)
	ids1, _ := tbl.LocateN([]byte("stable-key"), 5)
	ids2, _ := tbl.LocateN([]byte("stable-key"), 5)
	for i := range ids1 {
		if ids1[i] != ids2[i] {
			t.Fatalf("unstable at index %d: %s vs %s", i, ids1[i], ids2[i])
		}
	}
}

// TestLocateN_NEqualsM verifies that when N == M, LocateN returns all
// M nodes (as a key-dependent permutation, not a fixed order).
func TestLocateN_NEqualsM(t *testing.T) {
	tbl := buildMaglev(testNodes(5), defaultTableSize)
	ids, _ := tbl.LocateN([]byte("any-key"), 5)
	if len(ids) != 5 {
		t.Fatalf("expected 5, got %d", len(ids))
	}
	seen := make(map[string]bool)
	for _, id := range ids {
		seen[id] = true
	}
	if len(seen) != 5 {
		t.Fatal("not all 5 nodes present in result")
	}
}

// TestLocateN_NGreaterThanM verifies that LocateN returns an error
// when N exceeds the number of nodes.
func TestLocateN_NGreaterThanM(t *testing.T) {
	tbl := buildMaglev(testNodes(5), defaultTableSize)
	_, err := tbl.LocateN([]byte("key"), 6)
	if err == nil {
		t.Fatal("expected error for N > M")
	}
}

// TestLocateN_Shard0Balance verifies that shard 0 (the first node in
// the LocateN result) is approximately uniformly distributed across
// all nodes. This validates that different keys get different primary
// nodes, preventing hotspots.
func TestLocateN_Shard0Balance(t *testing.T) {
	tbl := buildMaglev(testNodes(5), defaultTableSize)
	const numKeys = 100_000
	counts := make(map[string]int)
	for i := 0; i < numKeys; i++ {
		ids, _ := tbl.LocateN([]byte(fmt.Sprintf("key-%d", i)), 5)
		counts[ids[0]]++
	}
	expected := float64(numKeys) / 5.0
	var sumSqDiff float64
	for _, c := range counts {
		diff := float64(c) - expected
		sumSqDiff += diff * diff
	}
	stddev := math.Sqrt(sumSqDiff / 5.0)
	relStddev := stddev / expected
	if relStddev > 0.01 {
		t.Errorf("shard-0 relative stddev %.4f > 1%%: %v", relStddev, counts)
	}
}

// TestBuildMaglev_InputOrderIndependent verifies that the table is the
// same regardless of input order (because buildMaglev sorts internally).
func TestBuildMaglev_InputOrderIndependent(t *testing.T) {
	tbl1 := buildMaglev([]string{"c", "a", "b"}, defaultTableSize)
	tbl2 := buildMaglev([]string{"a", "b", "c"}, defaultTableSize)
	tbl3 := buildMaglev([]string{"b", "c", "a"}, defaultTableSize)
	for i := 0; i < tbl1.size; i++ {
		if tbl1.table[i] != tbl2.table[i] || tbl2.table[i] != tbl3.table[i] {
			t.Fatalf("table differs at index %d: %d vs %d vs %d",
				i, tbl1.table[i], tbl2.table[i], tbl3.table[i])
		}
	}
}

// TestBuildMaglev_MinimalDisruption verifies that adding one node to a
// cluster remaps approximately 1/(M+1) of keys (close to the
// theoretical minimum).
func TestBuildMaglev_MinimalDisruption(t *testing.T) {
	const numKeys = 100_000
	nodes5 := testNodes(5)
	nodes6 := append(testNodes(5), "node-05")
	tbl5 := buildMaglev(nodes5, defaultTableSize)
	tbl6 := buildMaglev(nodes6, defaultTableSize)

	changed := 0
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		ids5, _ := tbl5.LocateN(key, 1)
		ids6, _ := tbl6.LocateN(key, 1)
		if ids5[0] != ids6[0] {
			changed++
		}
	}
	ratio := float64(changed) / float64(numKeys)
	expected := 1.0 / 6.0
	if math.Abs(ratio-expected) > 0.05 {
		t.Errorf("remap ratio %.4f too far from expected %.4f", ratio, expected)
	}
}

func BenchmarkLocateN_5_5(b *testing.B) {
	tbl := buildMaglev(testNodes(5), defaultTableSize)
	key := []byte("bench-key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.LocateN(key, 5)
	}
}

func BenchmarkLocateN_5_20(b *testing.B) {
	tbl := buildMaglev(testNodes(20), defaultTableSize)
	key := []byte("bench-key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.LocateN(key, 5)
	}
}

func BenchmarkBuildMaglev_5(b *testing.B) {
	nodes := testNodes(5)
	for i := 0; i < b.N; i++ {
		buildMaglev(nodes, defaultTableSize)
	}
}
