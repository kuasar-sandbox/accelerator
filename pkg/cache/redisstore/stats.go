package redisstore

import "github.com/kuasar-sandbox/accelerator/internal/util/obstat"

// Stats is a point-in-time snapshot of the Redis backend. Inflight includes
// requests whose caller was cancelled while the worker drains the RESP reply.
type Stats struct {
	Endpoint  string
	Transport string

	GetPoolSize  int64
	SetPoolSize  int64
	GetConnected int64
	SetConnected int64
	GetInflight  int64
	SetInflight  int64
	PoolWaiters  int64
	Draining     int64

	GetHits          uint64
	GetMisses        uint64
	Sets             uint64
	Cancelled        uint64
	LateBytesDrained uint64
	Reconnects       uint64
	ProtocolErrors   uint64
	BackendErrors    uint64

	GetLatency      obstat.HistSnapshot
	SetLatency      obstat.HistSnapshot
	PoolWaitLatency obstat.HistSnapshot
}
