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
	resolveN atomic.Uint64 // Resolve calls

	// Alias outcomes of Puts that asked for representation (cross-node delta).
	aliasWon   atomic.Uint64 // this Put's object became the chunk's representation
	aliasLost  atomic.Uint64 // another delta object already represented the chunk
	aliasIndep atomic.Uint64 // an independent object already existed (no longer produced; kept for the stats line)
	aliasErr   atomic.Uint64 // the alias step failed or is unsupported; Put still succeeded
	// Losing objects this server created in the same Put and deleted again.
	discarded      atomic.Uint64
	discardedBytes atomic.Uint64
	discardErr     atomic.Uint64 // a losing object could not be deleted and stays as an orphan
	errN           atomic.Uint64 // requests that returned a gRPC error
	inflight       atomic.Int64  // gauge: Get+Put currently executing

	getHist obstat.Hist // Get wall-clock latency
	putHist obstat.Hist // Put wall-clock latency
}

// StatsSnapshot is an immutable read of Stats.
type StatsSnapshot struct {
	GetN, GetHits, GetBytes  uint64
	PutN, PutBytes, PutDedup uint64
	AdmitN, ErrN             uint64
	ResolveN                 uint64
	AliasWon, AliasLost      uint64
	AliasIndep, AliasErr     uint64
	Discarded, DiscardErr    uint64
	DiscardedBytes           uint64
	Inflight                 int64
	GetHist, PutHist         obstat.HistSnapshot
}

// Snapshot reads the counters. Safe to call concurrently with request handling.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		GetN: s.getN.Load(), GetHits: s.getHits.Load(), GetBytes: s.getBytes.Load(),
		PutN: s.putN.Load(), PutBytes: s.putBytes.Load(), PutDedup: s.putDedup.Load(),
		AdmitN: s.admitN.Load(), ErrN: s.errN.Load(),
		ResolveN: s.resolveN.Load(),
		AliasWon: s.aliasWon.Load(), AliasLost: s.aliasLost.Load(),
		AliasIndep: s.aliasIndep.Load(), AliasErr: s.aliasErr.Load(),
		Discarded: s.discarded.Load(), DiscardErr: s.discardErr.Load(),
		DiscardedBytes: s.discardedBytes.Load(),
		Inflight:       s.inflight.Load(),
		GetHist:        s.getHist.Snapshot(),
		PutHist:        s.putHist.Snapshot(),
	}
}

// Stats returns a snapshot of the server's request counters for the periodic
// stats printer.
func (s *Server) Stats() StatsSnapshot { return s.stats.Snapshot() }
