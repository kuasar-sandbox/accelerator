package delta

import (
	"bytes"
	"sync"
	"testing"
)

// Checks that Put and Acquire work, and that the least recently used entry is the
// one evicted when the pool is full. Acquiring an entry makes it recent. With a
// 300-byte pool and three 100-byte entries, adding a fourth must evict the
// untouched entry, and the eviction callback must report it.
func TestPoolPutAcquireAndLRUEviction(t *testing.T) {
	p := NewBasePool(300)
	var evicted [][32]byte
	p.onEvict = func(_ *Domain, k [32]byte) { evicted = append(evicted, k) }
	buf := func(b byte) []byte { return bytes.Repeat([]byte{b}, 100) }

	for i := byte(1); i <= 3; i++ {
		if !p.Put(nil, key(i), buf(i)) {
			t.Fatalf("put %d", i)
		}
	}
	if n, used := p.Stats(); n != 3 || used != 300 {
		t.Fatalf("stats %d %d", n, used)
	}
	// Touch 1 so that 2 is the LRU victim.
	d, rel, ok := p.Acquire(key(1))
	if !ok || !bytes.Equal(d, buf(1)) {
		t.Fatal("acquire 1")
	}
	rel()
	p.Put(nil, key(4), buf(4))
	if p.Has(key(2)) || !p.Has(key(1)) || !p.Has(key(3)) || !p.Has(key(4)) {
		t.Fatal("wrong eviction victim")
	}
	if len(evicted) != 1 || evicted[0] != key(2) {
		t.Fatalf("callback %v", evicted)
	}
	if _, used := p.Stats(); used != 300 {
		t.Fatalf("used %d", used)
	}
}

// Checks that Put refuses an entry larger than the whole pool, and an empty entry,
// and that neither is stored.
func TestPoolRejectsOversizeAndEmpty(t *testing.T) {
	p := NewBasePool(100)
	if p.Put(nil, key(1), make([]byte, 101)) || p.Put(nil, key(2), nil) {
		t.Fatal("oversize/empty must be rejected")
	}
	if n, _ := p.Stats(); n != 0 {
		t.Fatal("rejected entry resident")
	}
}

// Checks that an evicted entry's plaintext is zeroed, since the pool owns the
// buffer it was given and that buffer must not keep sensitive data in memory.
func TestPoolZeroesOnEviction(t *testing.T) {
	p := NewBasePool(100)
	secret := bytes.Repeat([]byte{0xAA}, 100)
	p.Put(nil, key(1), secret)
	p.Put(nil, key(2), bytes.Repeat([]byte{0xBB}, 100)) // evicts 1
	if !bytes.Equal(secret, make([]byte, 100)) {
		t.Fatal("evicted plaintext was not zeroed")
	}
}

// Checks lease handling. An entry evicted while leased can no longer be acquired,
// but its data stays intact until the lease is released, and is zeroed on the
// last release. Calling release a second time is harmless.
func TestPoolLeasedBufferSurvivesEvictionAndZeroesOnRelease(t *testing.T) {
	p := NewBasePool(100)
	orig := bytes.Repeat([]byte{0xAA}, 100)
	p.Put(nil, key(1), orig)
	data, release, ok := p.Acquire(key(1))
	if !ok {
		t.Fatal("acquire")
	}
	p.Put(nil, key(2), bytes.Repeat([]byte{0xBB}, 100)) // evicts 1 while leased
	if p.Has(key(1)) {
		t.Fatal("evicted entry still acquirable")
	}
	if _, _, ok := p.Acquire(key(1)); ok {
		t.Fatal("must not lease an evicted entry")
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{0xAA}, 100)) {
		t.Fatal("leased data was zeroed while in use")
	}
	release()
	release() // idempotent
	if !bytes.Equal(orig, make([]byte, 100)) {
		t.Fatal("leased data not zeroed after last release")
	}
}

// Checks that putting a key that is already resident reports success, keeps the
// original buffer, and does not count the bytes twice. The second buffer is left
// untouched, since the pool did not take it.
func TestPoolPutDuplicateKeepsOriginal(t *testing.T) {
	p := NewBasePool(1000)
	a := bytes.Repeat([]byte{1}, 10)
	b := bytes.Repeat([]byte{1}, 10)
	p.Put(nil, key(1), a)
	if !p.Put(nil, key(1), b) {
		t.Fatal("duplicate put should report success")
	}
	if n, used := p.Stats(); n != 1 || used != 10 {
		t.Fatalf("duplicate double counted: %d %d", n, used)
	}
	if !bytes.Equal(b, bytes.Repeat([]byte{1}, 10)) {
		t.Fatal("pool must not touch a buffer it did not take")
	}
}

// Checks that Remove drops one key and RemoveOwner drops every key belonging to
// one domain, while other domains' entries stay. Neither removal may invoke the
// eviction callback, since the caller already handles those entries.
func TestPoolRemoveAndRemoveOwner(t *testing.T) {
	p := NewBasePool(1000)
	d1, d2 := &Domain{}, &Domain{}
	called := false
	p.onEvict = func(*Domain, [32]byte) { called = true }
	p.Put(d1, key(1), make([]byte, 10))
	p.Put(d1, key(2), make([]byte, 10))
	p.Put(d2, key(3), make([]byte, 10))
	p.Remove(key(1))
	p.RemoveOwner(d1)
	if p.Has(key(1)) || p.Has(key(2)) || !p.Has(key(3)) {
		t.Fatal("wrong removal")
	}
	if n, used := p.Stats(); n != 1 || used != 10 {
		t.Fatalf("stats %d %d", n, used)
	}
	if called {
		t.Fatal("explicit removal must not invoke the eviction callback")
	}
}

// Checks that concurrent Put, Acquire, and Remove calls from eight goroutines keep
// the byte count within the pool's bounds. It asserts little beyond that, so its
// main value is under the race detector (go test -race), which catches data races.
func TestPoolConcurrent(t *testing.T) {
	p := NewBasePool(2000)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := key(byte((g*13 + i) % 64))
				switch i % 3 {
				case 0:
					p.Put(nil, k, bytes.Repeat([]byte{byte(i)}, 50))
				case 1:
					if d, rel, ok := p.Acquire(k); ok {
						_ = d[0]
						rel()
					}
				default:
					p.Remove(k)
				}
			}
		}(g)
	}
	wg.Wait()
	if _, used := p.Stats(); used > 2000 || used < 0 {
		t.Fatalf("used %d out of bounds", used)
	}
}
