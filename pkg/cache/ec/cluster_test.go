package ec

import "testing"

func TestClampShardsToPeers(t *testing.T) {
	tests := []struct {
		name                 string
		data, parity, n      int
		wantData, wantParity int
		wantClamped          bool
	}{
		// data+parity <= n : untouched (smaller fan-out is legitimate).
		{"fits exactly", 4, 1, 5, 4, 1, false},
		{"smaller fan-out kept", 2, 1, 5, 2, 1, false},
		{"single data no parity fits", 1, 0, 5, 1, 0, false},

		// data+parity > n, parity < n : preserve parity, shrink data.
		{"too many data shards", 6, 1, 5, 4, 1, true},
		{"high parity preserved", 4, 3, 5, 2, 3, true},
		{"default 4+1 on 3 peers", 4, 1, 3, 2, 1, true},

		// parity >= n : fall back to 1 + (n-1).
		{"parity exceeds peers", 2, 5, 3, 1, 2, true},
		{"parity equals peers", 1, 3, 3, 1, 2, true},
		{"default 4+1 on 1 peer -> 1+0", 4, 1, 1, 1, 0, true},

		// n == 0 : left to the router's "no peers" error.
		{"no peers untouched", 4, 1, 0, 4, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, p, c := clampShardsToPeers(tt.data, tt.parity, tt.n)
			if d != tt.wantData || p != tt.wantParity || c != tt.wantClamped {
				t.Fatalf("clampShardsToPeers(%d,%d,%d) = (%d,%d,%v), want (%d,%d,%v)",
					tt.data, tt.parity, tt.n, d, p, c, tt.wantData, tt.wantParity, tt.wantClamped)
			}
			// When clamped, the result must be a valid RS scheme that fits n
			// peers exactly: at least one data shard, non-negative parity, and
			// data+parity == n.
			if c {
				if d < 1 || p < 0 || d+p != tt.n {
					t.Fatalf("clamped (%d,%d) invalid for n=%d: want d>=1, p>=0, d+p==n", d, p, tt.n)
				}
			}
		})
	}
}
