package ec

// ClusterConfig holds the static cluster configuration loaded from YAML.
type ClusterConfig struct {
	DataShards   int    `yaml:"data_shards"`
	ParityShards int    `yaml:"parity_shards"`
	Peers        []Peer `yaml:"peers"`
	Pool         int    `yaml:"pool"`
	Timeout      string `yaml:"timeout"`
}

// clampShardsToPeers fits an EC (data, parity) scheme into the available peer
// count. Reed-Solomon places exactly data+parity distinct chunks per object
// (one per peer via Maglev), so data+parity must not exceed len(peers) —
// otherwise every Get fails at routing time ("need N nodes but only M").
//
// When data+parity > n the scheme is clamped to use all n peers, preferring to
// PRESERVE parity (fault tolerance) and shrink data:
//
//   - parity < n   → data = n - parity         (keep the configured parity)
//   - parity >= n  → data = 1, parity = n - 1   (parity alone won't fit; fall
//     back to maximal redundancy. n == 1 → 1+0, a no-redundancy single-peer
//     passthrough that still reads/writes cleanly; a missing shard is just a
//     miss.)
//
// data+parity <= n is left untouched: a per-object fan-out smaller than the
// peer count is legitimate and possibly deliberate (objects spread across peer
// subsets for load balancing; fewer reads per Get → less EC tail latency
// amplification). n == 0 is left untouched too, so the router reports the
// clearer "no peers configured" error rather than a confusing parity<0 one.
//
// Returns the (possibly unchanged) data, parity and whether a clamp occurred.
func clampShardsToPeers(data, parity, n int) (int, int, bool) {
	if n < 1 || data+parity <= n {
		return data, parity, false
	}
	if parity < n {
		data = n - parity
	} else {
		data, parity = 1, n-1
	}
	return data, parity, true
}
