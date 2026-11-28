package delta

import (
	"container/list"
	"sync"
)

// RepEntry records that the chunk whose independent object would be H_ind is
// already stored as a delta object.
type RepEntry struct {
	// DeltaObjectKey is the delta object's address (SHA-256 of the object).
	DeltaObjectKey [32]byte
	// BaseObjectKey is the base chunk's object address.
	BaseObjectKey [32]byte
	// ObjectSize is the delta object's full size, for accounting on a hit
	// without a store round trip.
	ObjectSize uint32
	// BaseSize is the base chunk's plaintext size, recorded in the manifest's
	// delta table so readers can size the base without opening the delta
	// object. Zero only for entries loaded from a pre-Version3 snapshot, which
	// predate the field; such an entry is not trusted as a hit.
	BaseSize uint32
}

// Complete reports whether the entry carries everything a manifest needs.
func (e RepEntry) Complete() bool { return e.BaseSize != 0 }

type repItem struct {
	hInd [32]byte
	RepEntry
}

// repIndex is the representation index: H_ind -> where the chunk actually
// lives. A bounded LRU; evicting an entry only forfeits a future shortcut.
type repIndex struct {
	mu  sync.Mutex
	max int
	m   map[[32]byte]*list.Element
	lru *list.List // front = most recently used
}

func newRepIndex(max int) *repIndex {
	return &repIndex{max: max, m: make(map[[32]byte]*list.Element), lru: list.New()}
}

func (r *repIndex) Lookup(h [32]byte) (RepEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	el, ok := r.m[h]
	if !ok {
		return RepEntry{}, false
	}
	r.lru.MoveToFront(el)
	return el.Value.(*repItem).RepEntry, true
}

func (r *repIndex) Put(h [32]byte, e RepEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if el, ok := r.m[h]; ok {
		el.Value.(*repItem).RepEntry = e
		r.lru.MoveToFront(el)
		return
	}
	r.m[h] = r.lru.PushFront(&repItem{hInd: h, RepEntry: e})
	for r.lru.Len() > r.max {
		back := r.lru.Back()
		delete(r.m, back.Value.(*repItem).hInd)
		r.lru.Remove(back)
	}
}

func (r *repIndex) Delete(h [32]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if el, ok := r.m[h]; ok {
		delete(r.m, h)
		r.lru.Remove(el)
	}
}

func (r *repIndex) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lru.Len()
}

// items returns entries oldest first, so a reload that replays them with Put
// reproduces the recency order.
func (r *repIndex) items() []repItem {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]repItem, 0, r.lru.Len())
	for el := r.lru.Back(); el != nil; el = el.Prev() {
		out = append(out, *el.Value.(*repItem))
	}
	return out
}
