package delta

import (
	"slices"
	"sort"
	"sync"
)

// Candidate is a stored base that may be similar to a new chunk.
type Candidate struct {
	// ObjectKey is the base chunk's object address (what the delta header names).
	ObjectKey [32]byte
	// BaseKey is the base chunk's ordinary convergent key K_B.
	BaseKey [32]byte
	// Size is the base plaintext size.
	Size uint32
	// Similarity is the estimated Jaccard similarity to the query.
	Similarity float64
}

// BaseInfo is what the similarity index remembers about one base.
type BaseInfo struct {
	ObjectKey [32]byte
	BaseKey   [32]byte
	Size      uint32
}

const nilID = ^uint32(0)

type simEntry struct {
	info    BaseInfo
	compact Compact
	sfs     [MaxSlots]uint64 // super-features this entry is posted under (0 = none)
	nsf     uint8
	seq     uint64 // monotonically increasing insertion number
	prev    uint32 // insertion-order list, oldest at head
	next    uint32
	live    bool
}

// simIndex maps super-features to bases. Candidate generation is:
//
//	super-feature -> posting list of entry ids -> compact-sketch similarity
//
// Entries live in a slab addressed by uint32 id; a free list recycles ids.
// Postings are removed eagerly on eviction (the entry remembers which lists it
// is in), so a posting always names a live entry and there is no lazy cleanup
// or generation counter. Each posting list is capped; on overflow the oldest
// posting is dropped, which bounds both memory and per-lookup work against hot
// features that thousands of unrelated chunks share.
type simIndex struct {
	sk *Sketcher

	mu          sync.RWMutex
	maxEntries  int
	maxPostings int

	slab     []simEntry
	free     []uint32
	byKey    map[[32]byte]uint32
	postings map[uint64][]uint32
	head     uint32 // oldest
	tail     uint32 // newest
	live     int
	nextSeq  uint64
}

func newSimIndex(sk *Sketcher, maxEntries, maxPostings int) *simIndex {
	return &simIndex{
		sk:          sk,
		maxEntries:  maxEntries,
		maxPostings: maxPostings,
		byKey:       make(map[[32]byte]uint32),
		postings:    make(map[uint64][]uint32),
		head:        nilID,
		tail:        nilID,
	}
}

// Register adds a base and returns the keys of any bases it displaced to stay
// within maxEntries. A key already present is refreshed to newest.
func (x *simIndex) Register(info BaseInfo, sk Sketch) (displaced [][32]byte) {
	var sfBuf [MaxSlots]uint64
	sfs := x.sk.SuperFeatures(sk, sfBuf[:0])

	x.mu.Lock()
	defer x.mu.Unlock()

	if id, ok := x.byKey[info.ObjectKey]; ok {
		x.removeLocked(id)
	}
	for x.live >= x.maxEntries && x.head != nilID {
		old := x.head
		displaced = append(displaced, x.slab[old].info.ObjectKey)
		x.removeLocked(old)
	}

	var id uint32
	if n := len(x.free); n > 0 {
		id = x.free[n-1]
		x.free = x.free[:n-1]
	} else {
		id = uint32(len(x.slab))
		x.slab = append(x.slab, simEntry{})
	}
	e := &x.slab[id]
	x.nextSeq++
	*e = simEntry{info: info, compact: sk.Compact(), live: true, seq: x.nextSeq, prev: x.tail, next: nilID}
	if x.tail != nilID {
		x.slab[x.tail].next = id
	} else {
		x.head = id
	}
	x.tail = id
	x.byKey[info.ObjectKey] = id
	x.live++

	for _, f := range sfs {
		if f == 0 {
			continue
		}
		list := x.postings[f]
		if len(list) >= x.maxPostings {
			// Drop the oldest posting. Also forget it on the owning entry so a
			// later eviction does not look for it.
			drop := list[0]
			x.slab[drop].dropSF(f)
			list = list[1:]
		}
		x.postings[f] = append(list, id)
		e.sfs[e.nsf] = f
		e.nsf++
	}
	return displaced
}

func (e *simEntry) dropSF(f uint64) {
	for i := uint8(0); i < e.nsf; i++ {
		if e.sfs[i] == f {
			e.nsf--
			e.sfs[i] = e.sfs[e.nsf]
			e.sfs[e.nsf] = 0
			return
		}
	}
}

// Evict removes a base by object key. It reports whether it was present.
func (x *simIndex) Evict(objectKey [32]byte) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	id, ok := x.byKey[objectKey]
	if !ok {
		return false
	}
	x.removeLocked(id)
	return true
}

func (x *simIndex) removeLocked(id uint32) {
	e := &x.slab[id]
	for i := uint8(0); i < e.nsf; i++ {
		f := e.sfs[i]
		list := x.postings[f]
		if j := slices.Index(list, id); j >= 0 {
			list = slices.Delete(list, j, j+1)
		}
		if len(list) == 0 {
			delete(x.postings, f)
		} else {
			x.postings[f] = list
		}
	}
	if e.prev != nilID {
		x.slab[e.prev].next = e.next
	} else {
		x.head = e.next
	}
	if e.next != nilID {
		x.slab[e.next].prev = e.prev
	} else {
		x.tail = e.prev
	}
	delete(x.byKey, e.info.ObjectKey)
	*e = simEntry{}
	x.free = append(x.free, id)
	x.live--
}

// Lookup returns up to max candidates whose estimated similarity to sk is at
// least minSim, best first (ties: most recently registered first). It never
// returns a base whose posting was evicted.
func (x *simIndex) Lookup(sk Sketch, minSim float64, max int) []Candidate {
	var sfBuf [MaxSlots]uint64
	sfs := x.sk.SuperFeatures(sk, sfBuf[:0])

	type scored struct {
		c   Candidate
		seq uint64
	}
	var found []scored
	seen := make(map[uint32]struct{}, 8)

	x.mu.RLock()
	for _, f := range sfs {
		if f == 0 {
			continue
		}
		for _, id := range x.postings[f] {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			e := &x.slab[id]
			if sim := e.compact.Similarity(sk); sim >= minSim {
				found = append(found, scored{Candidate{
					ObjectKey: e.info.ObjectKey, BaseKey: e.info.BaseKey,
					Size: e.info.Size, Similarity: sim,
				}, e.seq})
			}
		}
	}
	x.mu.RUnlock()

	sort.SliceStable(found, func(i, j int) bool {
		if found[i].c.Similarity != found[j].c.Similarity {
			return found[i].c.Similarity > found[j].c.Similarity
		}
		return found[i].seq > found[j].seq
	})
	if len(found) > max {
		found = found[:max]
	}
	out := make([]Candidate, len(found))
	for i := range found {
		out[i] = found[i].c
	}
	return out
}

// storedBase is the snapshot form of one similarity-index entry. Super-features
// are stored explicitly: the compact sketch keeps only 16-bit fingerprints, which
// are not enough to recompute them.
type storedBase struct {
	info    BaseInfo
	compact Compact
	sfs     []uint64
}

// export returns live bases oldest first, so a reload that restores them in
// order reproduces recency.
func (x *simIndex) export() []storedBase {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make([]storedBase, 0, x.live)
	for id := x.head; id != nilID; id = x.slab[id].next {
		e := &x.slab[id]
		sfs := append([]uint64(nil), e.sfs[:e.nsf]...)
		out = append(out, storedBase{info: e.info, compact: e.compact, sfs: sfs})
	}
	return out
}

// restore inserts a previously exported base and drops the oldest entries if
// the index is over its cap. It does not recompute super-features.
func (x *simIndex) restore(b storedBase) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if id, ok := x.byKey[b.info.ObjectKey]; ok {
		x.removeLocked(id)
	}
	for x.live >= x.maxEntries && x.head != nilID {
		x.removeLocked(x.head)
	}
	var id uint32
	if n := len(x.free); n > 0 {
		id = x.free[n-1]
		x.free = x.free[:n-1]
	} else {
		id = uint32(len(x.slab))
		x.slab = append(x.slab, simEntry{})
	}
	e := &x.slab[id]
	x.nextSeq++
	*e = simEntry{info: b.info, compact: b.compact, live: true, seq: x.nextSeq, prev: x.tail, next: nilID}
	if x.tail != nilID {
		x.slab[x.tail].next = id
	} else {
		x.head = id
	}
	x.tail = id
	x.byKey[b.info.ObjectKey] = id
	x.live++
	for _, f := range b.sfs {
		if f == 0 || e.nsf == MaxSlots {
			continue
		}
		list := x.postings[f]
		if len(list) >= x.maxPostings {
			drop := list[0]
			x.slab[drop].dropSF(f)
			list = list[1:]
		}
		x.postings[f] = append(list, id)
		e.sfs[e.nsf] = f
		e.nsf++
	}
}

// Len is the number of live bases.
func (x *simIndex) Len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.live
}
