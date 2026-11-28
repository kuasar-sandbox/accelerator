package delta

import (
	"container/list"
	"sync"
)

// BasePool keeps recently stored independent chunks' PLAINTEXT in memory so a
// new chunk can be delta-encoded against them without reading and decrypting
// the base back from the store.
//
// It is bounded by bytes and evicts least-recently-used. A candidate that has
// left the pool can still be used: the ingest path reads the object back from
// the store. Plaintext is sensitive, so evicted buffers are zeroed; a buffer
// still leased to an in-flight encode is zeroed when its last lease is released.
type BasePool struct {
	mu      sync.Mutex
	max     int64
	used    int64
	m       map[[32]byte]*poolEntry
	lru     *list.List // front = most recently used
	onEvict func(owner *Domain, key [32]byte)
}

type poolEntry struct {
	key   [32]byte
	owner *Domain
	data  []byte
	refs  int
	dead  bool
	elem  *list.Element
}

// NewBasePool returns a pool bounded to maxBytes of resident plaintext.
func NewBasePool(maxBytes int64) *BasePool {
	return &BasePool{max: maxBytes, m: make(map[[32]byte]*poolEntry), lru: list.New()}
}

// Put admits a base and TAKES OWNERSHIP of data: the caller must not use or
// modify it afterwards, and the pool zeroes it when it leaves. It reports
// false (and leaves data untouched) if the base alone exceeds the pool.
func (p *BasePool) Put(owner *Domain, key [32]byte, data []byte) bool {
	size := int64(len(data))
	if size == 0 || size > p.max {
		return false
	}
	var evicted []*poolEntry

	p.mu.Lock()
	if el, ok := p.m[key]; ok {
		// Already resident (same content): refresh recency, keep the old buffer.
		p.lru.MoveToFront(el.elem)
		p.mu.Unlock()
		return true
	}
	for p.used+size > p.max {
		back := p.lru.Back()
		if back == nil {
			break
		}
		evicted = append(evicted, p.removeLocked(back.Value.(*poolEntry)))
	}
	e := &poolEntry{key: key, owner: owner, data: data}
	e.elem = p.lru.PushFront(e)
	p.m[key] = e
	p.used += size
	cb := p.onEvict
	p.mu.Unlock()

	p.finishEvictions(evicted, cb)
	return true
}

// PutLeased is Put that also returns a lease on the admitted buffer, held
// before the lock is released so a concurrent eviction cannot zero it first.
// If key is already resident, data is left to the caller and the existing
// buffer is leased instead. ok is false when data cannot fit in the pool at
// all; data is untouched in that case.
func (p *BasePool) PutLeased(owner *Domain, key [32]byte, data []byte) (leased []byte, release func(), ok bool) {
	size := int64(len(data))
	if size == 0 || size > p.max {
		return nil, nil, false
	}
	var evicted []*poolEntry
	p.mu.Lock()
	if el, found := p.m[key]; found {
		p.lru.MoveToFront(el.elem)
		el.refs++
		rel := p.releaseFn(el)
		got := el.data
		p.mu.Unlock()
		return got, rel, true
	}
	for p.used+size > p.max {
		back := p.lru.Back()
		if back == nil {
			break
		}
		evicted = append(evicted, p.removeLocked(back.Value.(*poolEntry)))
	}
	e := &poolEntry{key: key, owner: owner, data: data, refs: 1}
	e.elem = p.lru.PushFront(e)
	p.m[key] = e
	p.used += size
	rel := p.releaseFn(e)
	cb := p.onEvict
	p.mu.Unlock()
	p.finishEvictions(evicted, cb)
	return data, rel, true
}

func (p *BasePool) releaseFn(e *poolEntry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			e.refs--
			if e.dead && e.refs == 0 {
				clear(e.data)
				e.data = nil
			}
		})
	}
}

// removeLocked unlinks e. The caller zeroes it after releasing the lock if no
// lease is outstanding.
func (p *BasePool) removeLocked(e *poolEntry) *poolEntry {
	p.lru.Remove(e.elem)
	delete(p.m, e.key)
	p.used -= int64(len(e.data))
	e.dead = true
	return e
}

func (p *BasePool) finishEvictions(evicted []*poolEntry, cb func(*Domain, [32]byte)) {
	for _, e := range evicted {
		p.mu.Lock()
		if e.refs == 0 {
			clear(e.data)
			e.data = nil
		}
		p.mu.Unlock()
		if cb != nil {
			cb(e.owner, e.key)
		}
	}
}

// Acquire leases a base's plaintext. The returned slice is read-only and valid
// until release is called exactly once. ok is false if the base is not resident.
func (p *BasePool) Acquire(key [32]byte) (data []byte, release func(), ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, found := p.m[key]
	if !found {
		return nil, nil, false
	}
	p.lru.MoveToFront(e.elem)
	e.refs++
	var once sync.Once
	return e.data, func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			e.refs--
			if e.dead && e.refs == 0 {
				clear(e.data)
				e.data = nil
			}
		})
	}, true
}

// Has reports residency without leasing.
func (p *BasePool) Has(key [32]byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.m[key]
	return ok
}

// Remove drops one base. It does not invoke the eviction callback: the caller
// already knows and is keeping its own index in step.
func (p *BasePool) Remove(key [32]byte) {
	p.mu.Lock()
	e, ok := p.m[key]
	if !ok {
		p.mu.Unlock()
		return
	}
	p.removeLocked(e)
	if e.refs == 0 {
		clear(e.data)
		e.data = nil
	}
	p.mu.Unlock()
}

// RemoveOwner drops every base belonging to a dedup domain, e.g. when the
// domain itself is evicted. No eviction callback is invoked.
func (p *BasePool) RemoveOwner(owner *Domain) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.m {
		if e.owner != owner {
			continue
		}
		p.removeLocked(e)
		if e.refs == 0 {
			clear(e.data)
			e.data = nil
		}
	}
}

// Stats reports resident entries and bytes.
func (p *BasePool) Stats() (entries int, bytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.m), p.used
}
