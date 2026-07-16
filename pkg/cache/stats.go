package cache

// DaemonStats is the top-level snapshot returned by the Info gRPC
// service. Pure Go — no proto dependency. The server package owns the
// Go ↔ proto conversion.
type DaemonStats struct {
	Mode        string         `json:"mode"`
	BackendType string         `json:"backend_type,omitempty"`
	UptimeSec   int64          `json:"uptime_sec"`
	Server      ServerStats    `json:"server"`
	Tiered      *TieredStats   `json:"tiered,omitempty"`
	Rocks       []RocksCFStats `json:"rocks,omitempty"`
	Redis       *RedisStats    `json:"redis,omitempty"`
}

// ServerStats is the wire-handler view of the daemon's external RPC
// service — a single Get/GetShard RPC bumps exactly one of Hits/Misses,
// and a successful Fill/FillShard bumps Fills. Unlike per-tier counts,
// a single RPC never double-counts here.
type ServerStats struct {
	Hits   uint64 `json:"hits"`
	Misses uint64 `json:"misses"`
	Fills  uint64 `json:"fills"`
}

// TieredStats carries per-tier cascade counts plus the origin. Only
// populated when the daemon runs in tiered mode.
type TieredStats struct {
	Tiers  []TierStats `json:"tiers"`
	Origin OriginStats `json:"origin"`
}

// TierStats is one tier's cascade-view counts from TieredCache. A
// single Get RPC may touch several tiers, incrementing each of their
// Hits/Misses/Errors according to the tier's answer. Errors counts
// tryTier calls that returned a non-nil error (wire failure, decode
// failure, etc.) — kept separate from Misses so observability shows
// "peer confirmed absent" distinctly from "we failed to ask".
type TierStats struct {
	Type          string `json:"type"`
	Hits          uint64 `json:"hits"`
	Misses        uint64 `json:"misses"`
	Fills         uint64 `json:"fills"`
	Errors        uint64 `json:"errors"`
	FillsInflight uint64 `json:"fills_inflight"`

	Rocks    []RocksCFStats `json:"rocks,omitempty"`
	Redis    *RedisStats    `json:"redis,omitempty"`
	Peers    []PeerStats    `json:"peers,omitempty"`
	Endpoint string         `json:"endpoint,omitempty"`
}

// OriginStats is the final cascade layer (store or upstream). No Fills
// — origin is a Getter, not a Filler. Errors mirrors TierStats.Errors.
type OriginStats struct {
	Type   string `json:"type"`
	Hits   uint64 `json:"hits"`
	Misses uint64 `json:"misses"`
	Errors uint64 `json:"errors"`

	StorePath string `json:"store_path,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
}

// PeerStats is per-peer counts inside an EC tier. Distinct from the
// tier-level counts: tier-level Hits requires ≥ dataShards peers
// returning data; each PeerStats row counts that single peer's
// shard-level answers.
//
// Misses counts only peer-confirmed StatusMiss. Errors counts wire
// failures (Acquire/Write/Read error, StatusError). Cancelled counts
// StatusCancelled replies (peer honoured a CancelRequest, e.g., after
// EC.Get early-exited with enough hits). Keeping these separate avoids
// inflating the Miss rate with transport / cancellation noise.
type PeerStats struct {
	ID        string `json:"id"`
	Endpoint  string `json:"endpoint"`
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Errors    uint64 `json:"errors"`
	Cancelled uint64 `json:"cancelled"`
	Fills     uint64 `json:"fills"`
}

// RocksCFStats mirrors rocks.CFStats with JSON tags; the server
// package converts from rocks.CFStats at snapshot time.
type RocksCFStats struct {
	Name      string `json:"name"`
	NumKeys   string `json:"num_keys"`
	DiskUsage string `json:"disk_usage"`
	MemUsage  string `json:"mem_usage"`
}

// RedisStats is the bounded backend worker/pool view exported by a redisstore.
// Latencies are cumulative backend percentiles in nanoseconds.
type RedisStats struct {
	Endpoint  string `json:"endpoint"`
	Transport string `json:"transport"`

	GetPoolSize  int64 `json:"get_pool_size"`
	SetPoolSize  int64 `json:"set_pool_size"`
	GetConnected int64 `json:"get_connected"`
	SetConnected int64 `json:"set_connected"`
	GetInflight  int64 `json:"get_inflight"`
	SetInflight  int64 `json:"set_inflight"`
	PoolWaiters  int64 `json:"pool_waiters"`
	Draining     int64 `json:"draining"`

	GetHits          uint64 `json:"get_hits"`
	GetMisses        uint64 `json:"get_misses"`
	Sets             uint64 `json:"sets"`
	Cancelled        uint64 `json:"cancelled"`
	LateBytesDrained uint64 `json:"late_bytes_drained"`
	Reconnects       uint64 `json:"reconnects"`
	ProtocolErrors   uint64 `json:"protocol_errors"`
	BackendErrors    uint64 `json:"backend_errors"`

	GetP50Ns      uint64 `json:"get_p50_ns"`
	GetP99Ns      uint64 `json:"get_p99_ns"`
	GetP999Ns     uint64 `json:"get_p999_ns"`
	SetP50Ns      uint64 `json:"set_p50_ns"`
	SetP99Ns      uint64 `json:"set_p99_ns"`
	SetP999Ns     uint64 `json:"set_p999_ns"`
	PoolWaitP99Ns uint64 `json:"pool_wait_p99_ns"`
}
