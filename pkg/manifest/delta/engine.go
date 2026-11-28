package delta

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Engine owns all delta soft state for one process: the shared base pool and,
// per dedup domain (salt), a similarity index, a representation index and a
// known-independent filter. One Engine is shared by every ingester in the
// process, because that is the only way chunks from different images can find
// each other as bases.
type Engine struct {
	p    Params
	sk   *Sketcher
	pool *BasePool

	mu      sync.Mutex
	domains map[[32]byte]*Domain
	tick    uint64

	dirty    atomic.Bool
	aliasOff atomic.Bool
	snapMu   sync.Mutex
	lastSnap time.Time
	now      func() time.Time
	// snapKey seals snapshots (seal.go). Derived from the customer key; zero
	// when no snapshot path is configured.
	snapKey [32]byte

	loadErr error
}

// SnapshotKeyFunc returns the customer key. The engine derives its snapshot
// sealing key from it (seal.go); the customer key itself is not retained.
type SnapshotKeyFunc func() ([32]byte, error)

// NewEngine builds an engine from validated Params and, if a snapshot path is
// configured, tries to restore the persisted state. A missing, stale or corrupt
// snapshot is not an error for the caller: the state is soft, so the engine
// starts empty and reports why through LoadErr.
//
// A snapshot holds chunk decryption keys, so a configured snapshot path
// requires keyFn: the file is sealed under a key derived from the customer
// key, and a snapshot written under one customer key is unreadable under
// another. keyFn may be nil when no snapshot path is configured.
func NewEngine(p Params, keyFn SnapshotKeyFunc) (*Engine, error) {
	sk, err := NewSketcher(p)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		p:       p,
		sk:      sk,
		pool:    NewBasePool(p.PoolMaxBytes),
		domains: make(map[[32]byte]*Domain),
		now:     time.Now,
	}
	// Pool eviction does not forget the base. Phase 2 can read the object back
	// from the store, so the similarity index outlives the plaintext pool.
	if p.SnapshotPath != "" {
		if keyFn == nil {
			return nil, errors.New("delta: index.snapshot_path requires the customer key to seal the snapshot")
		}
		ck, err := keyFn()
		if err != nil {
			return nil, fmt.Errorf("delta: snapshot key: %w", err)
		}
		e.snapKey = deriveSnapshotKey(ck)
		clear(ck[:])
		e.loadErr = e.loadSnapshot(p.SnapshotPath)
	}
	return e, nil
}

// Params returns the resolved parameters.
func (e *Engine) Params() Params { return e.p }

// Sketcher returns the engine's sketcher.
func (e *Engine) Sketcher() *Sketcher { return e.sk }

// AliasUnsupported reports whether the store was found unable to answer
// Resolve. Delta objects are only written when replicas can agree on one
// representation per chunk, which needs that RPC, so once this is set no chunk
// is delta-encoded or taken from the representation index. It is process-wide
// and sticky: a store upgraded underneath a running process is picked up on
// restart.
func (e *Engine) AliasUnsupported() bool { return e.aliasOff.Load() }

// MarkAliasUnsupported records that the store cannot resolve aliases and
// reports whether this call was the first to do so, so the caller can log once.
func (e *Engine) MarkAliasUnsupported() (first bool) { return e.aliasOff.CompareAndSwap(false, true) }

// LoadErr reports why the configured snapshot was not restored; nil if it was
// restored or no snapshot existed.
func (e *Engine) LoadErr() error { return e.loadErr }

// Domain returns the state for one dedup domain, creating it (and evicting the
// least recently used domain if over the cap) on first use. The salt already
// encodes generation and any tenant salt, so two ingests that cannot dedup
// against each other can never see each other's entries.
func (e *Engine) Domain(salt [32]byte) *Domain {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tick++
	if d, ok := e.domains[salt]; ok {
		d.used = e.tick
		return d
	}
	d := e.newDomainLocked(salt)
	e.evictDomainsLocked()
	return d
}

func (e *Engine) newDomainLocked(salt [32]byte) *Domain {
	d := &Domain{
		e:     e,
		salt:  salt,
		sim:   newSimIndex(e.sk, e.p.MaxEntries, e.p.MaxPostings),
		reps:  newRepIndex(e.p.MaxRepresentations),
		known: newBloom(e.p.KnownCapacity),
		used:  e.tick,
	}
	e.domains[salt] = d
	return d
}

func (e *Engine) evictDomainsLocked() {
	for len(e.domains) > e.p.MaxDomains {
		var victim *Domain
		for _, d := range e.domains {
			if victim == nil || d.used < victim.used {
				victim = d
			}
		}
		delete(e.domains, victim.salt)
		victim.closed.Store(true)
		e.pool.RemoveOwner(victim)
		e.dirty.Store(true)
	}
}

// Stats is a point-in-time view of the engine's sizes.
type Stats struct {
	Domains         int
	PoolEntries     int
	PoolBytes       int64
	SimilarityBases int
	Representations int
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	doms := make([]*Domain, 0, len(e.domains))
	for _, d := range e.domains {
		doms = append(doms, d)
	}
	e.mu.Unlock()
	var s Stats
	s.Domains = len(doms)
	s.PoolEntries, s.PoolBytes = e.pool.Stats()
	for _, d := range doms {
		s.SimilarityBases += d.sim.Len()
		s.Representations += d.reps.Len()
	}
	return s
}

// Domain is the delta state for one dedup domain.
type Domain struct {
	e      *Engine
	salt   [32]byte
	sim    *simIndex
	reps   *repIndex
	known  *bloom
	used   uint64 // guarded by Engine.mu
	closed atomic.Bool
}

// Salt returns the domain's salt.
func (d *Domain) Salt() [32]byte { return d.salt }

// Known reports whether the chunk with independent object hash h is believed to
// be stored as an independent object already (a Bloom filter: false positives
// possible, false negatives only after a restart without a snapshot).
func (d *Domain) Known(h [32]byte) bool { return d.known.Has(h) }

// MarkKnown records that h is stored independently.
func (d *Domain) MarkKnown(h [32]byte) {
	d.known.Add(h)
	d.e.dirty.Store(true)
}

// LookupRep returns where the chunk with independent hash h is stored as a delta.
func (d *Domain) LookupRep(h [32]byte) (RepEntry, bool) { return d.reps.Lookup(h) }

// RecordRep records a successfully stored delta object. Call it only after the
// delta object's Put has succeeded: an entry for an object that was never
// stored is exactly the stale entry the design forbids.
func (d *Domain) RecordRep(h [32]byte, e RepEntry) {
	d.reps.Put(h, e)
	d.e.dirty.Store(true)
}

// ForgetRep drops a representation entry, e.g. after a failed existence probe.
func (d *Domain) ForgetRep(h [32]byte) {
	d.reps.Delete(h)
	d.e.dirty.Store(true)
}

// Candidates returns up to the configured number of bases whose estimated
// similarity to sk meets the configured threshold, best first. A candidate
// whose plaintext is not in the pool is still returned: the caller may read it
// back from the store. notResident counts how many of the returned candidates
// need that read-back.
func (d *Domain) Candidates(sk Sketch) (cands []Candidate, notResident int) {
	p := d.e.p
	hits := d.sim.Lookup(sk, p.MinSimilarity, p.MaxCandidates)
	for _, c := range hits {
		if !d.e.pool.Has(c.ObjectKey) {
			notResident++
		}
	}
	return hits, notResident
}

// EvictBase drops a base from the similarity index and the pool, for example
// after a read-back finds the object gone. The plaintext, if still pooled, is
// zeroed.
func (d *Domain) EvictBase(key [32]byte) {
	if d.sim.Evict(key) {
		d.e.dirty.Store(true)
	}
	d.e.pool.Remove(key)
}

// PutLeased admits plaintext fetched by read-back and returns a lease on it.
// The similarity index is not touched: the base is already registered.
func (d *Domain) PutLeased(key [32]byte, plain []byte) (leased []byte, release func(), ok bool) {
	return d.e.pool.PutLeased(d, key, plain)
}

// AcquireBase leases a candidate's plaintext from the pool.
func (d *Domain) AcquireBase(objectKey [32]byte) (plain []byte, release func(), ok bool) {
	return d.e.pool.Acquire(objectKey)
}

// RegisterBase makes an independently stored chunk available as a future base.
// It TAKES OWNERSHIP of plain. Only independent chunks may be registered:
// registering a delta would create chains, which the design forbids (depth one).
func (d *Domain) RegisterBase(info BaseInfo, plain []byte, sk Sketch) bool {
	if d.closed.Load() {
		return false
	}
	if !d.e.pool.Put(d, info.ObjectKey, plain) {
		return false
	}
	d.e.dirty.Store(true)
	// Register returns the keys of bases it displaced from the full similarity
	// index when full. Their plaintext is removed from the pool, since nothing can
	// reach them as candidates any more.
	for _, k := range d.sim.Register(info, sk) {
		d.e.pool.Remove(k)
	}
	return true
}
