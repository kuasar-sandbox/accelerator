package server

import (
	"sync/atomic"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
)

// Stats accumulates store-server request counters that feed the daemon's
// periodic, adaptive stats line. Every field is updated lock-free from the
// request handlers (one Add per request); read a consistent window with
// Snapshot. Counters are cumulative — the printer diffs successive snapshots.
type Stats struct {
	getN     atomic.Uint64 // Get attempts
	getHits  atomic.Uint64 // Get attempts that found the key
	getBytes atomic.Uint64 // bytes streamed OUT on Get hits
	putN     atomic.Uint64 // Put attempts
	putBytes atomic.Uint64 // bytes received + stored on Put (excludes dedup hits)
	putDedup atomic.Uint64 // Put that short-circuited on an existing key (no store)
	admitN   atomic.Uint64 // AdmitWrite calls
	errN     atomic.Uint64 // requests that returned a gRPC error
	inflight atomic.Int64  // gauge: Get+Put currently executing

	getHist obstat.Hist // Get wall-clock latency
	putHist obstat.Hist // Put wall-clock latency
}

// StatsSnapshot is an immutable read of Stats.
type StatsSnapshot struct {
	GetN, GetHits, GetBytes  uint64
	PutN, PutBytes, PutDedup uint64
	AdmitN, ErrN             uint64
	Inflight                 int64
	GetHist, PutHist         obstat.HistSnapshot
}

// Snapshot reads the counters. Safe to call concurrently with request handling.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		GetN: s.getN.Load(), GetHits: s.getHits.Load(), GetBytes: s.getBytes.Load(),
		PutN: s.putN.Load(), PutBytes: s.putBytes.Load(), PutDedup: s.putDedup.Load(),
		AdmitN: s.admitN.Load(), ErrN: s.errN.Load(),
		Inflight: s.inflight.Load(),
		GetHist:  s.getHist.Snapshot(),
		PutHist:  s.putHist.Snapshot(),
	}
}

// Stats returns a snapshot of the server's request counters for the periodic
// stats printer.
func (s *Server) Stats() StatsSnapshot { return s.stats.Snapshot() }
