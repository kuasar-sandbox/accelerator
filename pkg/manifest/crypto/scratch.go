package crypto

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

const (
	// Active and retained allowances together cap codec-owned scratch at
	// 256 MiB per direction. The active portion is a weighted semaphore;
	// the retained portion is the explicit free-list ceiling.
	chunkScratchByteBudget    = 192 << 20
	chunkScratchRetainedBytes = 64 << 20
	chunkScratchMaxRetained   = 2 << 20
	minScratchClass           = 4 << 10
)

var (
	ErrChunkTooLarge = errors.New("crypto: chunk exceeds decoded size limit")
	ErrScratchLimit  = errors.New("crypto: scratch request exceeds byte budget")
)

// Encoding and decoding have independent admission so a store-heavy ingest
// cannot starve restore. Both are process-wide: constructing more Fetchers or
// Ingesters does not multiply the peak scratch allowance.
var (
	defaultEncodeScratch = newScratchManager(
		max(1, runtime.GOMAXPROCS(0)),
		chunkScratchByteBudget,
		chunkScratchMaxRetained,
		chunkScratchRetainedBytes,
	)
	defaultDecodeScratch = newScratchManager(
		max(1, runtime.GOMAXPROCS(0)),
		chunkScratchByteBudget,
		chunkScratchMaxRetained,
		chunkScratchRetainedBytes,
	)
)

// scratchManager combines a cancellable CPU slot limit, a weighted in-flight
// byte limit, and an explicitly capped size-classed free list. A sync.Pool on
// its own would provide neither a peak bound nor a retained-memory bound.
type scratchManager struct {
	slots      *semaphore.Weighted
	bytes      *semaphore.Weighted
	byteBudget int64
	slotLimit  int

	maxRetainedBuffer int
	retainedBudget    int64

	mu            sync.Mutex
	pools         map[int][][]byte
	retainedBytes int64

	currentBytes atomic.Int64
	peakBytes    atomic.Int64
	poolMisses   atomic.Uint64
}

func newScratchManager(slots int, byteBudget, maxRetainedBuffer, retainedBudget int64) *scratchManager {
	if slots < 1 {
		slots = 1
	}
	if byteBudget < 1 {
		byteBudget = 1
	}
	if maxRetainedBuffer < 0 {
		maxRetainedBuffer = 0
	}
	if retainedBudget < 0 {
		retainedBudget = 0
	}
	return &scratchManager{
		slots:             semaphore.NewWeighted(int64(slots)),
		bytes:             semaphore.NewWeighted(byteBudget),
		byteBudget:        byteBudget,
		slotLimit:         slots,
		maxRetainedBuffer: int(maxRetainedBuffer),
		retainedBudget:    retainedBudget,
		pools:             make(map[int][][]byte),
	}
}

// acquire reserves one CPU slot and one scratch buffer. Waiting for either
// reservation observes ctx. The returned buffer has len=size and may be
// resliced up to its capacity by the codec.
func (m *scratchManager) acquire(ctx context.Context, size int) (scratchLease, error) {
	if err := m.slots.Acquire(ctx, 1); err != nil {
		return scratchLease{}, err
	}
	lease, err := m.acquireBuffer(ctx, size)
	if err != nil {
		m.slots.Release(1)
		return scratchLease{}, err
	}
	lease.hasSlot = true
	return lease, nil
}

// acquireBuffer reserves a size-classed buffer under the weighted byte budget.
// Callers that need multiple simultaneous regions request one combined buffer
// through acquire, so they cannot deadlock by incrementally reserving bytes.
func (m *scratchManager) acquireBuffer(ctx context.Context, size int) (scratchLease, error) {
	charge, err := m.classFor(size)
	if err != nil {
		return scratchLease{}, err
	}
	if err := m.bytes.Acquire(ctx, int64(charge)); err != nil {
		return scratchLease{}, err
	}
	current := m.currentBytes.Add(int64(charge))
	for {
		peak := m.peakBytes.Load()
		if current <= peak || m.peakBytes.CompareAndSwap(peak, current) {
			break
		}
	}

	var buf []byte
	if charge <= m.maxRetainedBuffer {
		m.mu.Lock()
		pool := m.pools[charge]
		if n := len(pool); n > 0 {
			buf = pool[n-1]
			m.pools[charge] = pool[:n-1]
			m.retainedBytes -= int64(cap(buf))
		}
		m.mu.Unlock()
	}
	if buf == nil {
		m.poolMisses.Add(1)
		buf = make([]byte, charge)
	}
	return scratchLease{
		manager: m,
		buf:     buf[:size],
		charge:  charge,
	}, nil
}

func (m *scratchManager) classFor(size int) (int, error) {
	if size < 0 {
		return 0, fmt.Errorf("%w: negative size %d", ErrScratchLimit, size)
	}
	charge := size
	if charge == 0 {
		charge = 1
	}
	if charge <= m.maxRetainedBuffer {
		class := minScratchClass
		for class < charge {
			if class > m.maxRetainedBuffer/2 {
				class = charge
				break
			}
			class *= 2
		}
		charge = class
	}
	if int64(charge) > m.byteBudget {
		return 0, fmt.Errorf("%w: need %d bytes, budget %d", ErrScratchLimit, charge, m.byteBudget)
	}
	return charge, nil
}

type scratchLease struct {
	manager  *scratchManager
	buf      []byte
	charge   int
	touched  int
	hasSlot  bool
	released bool
}

func (l *scratchLease) markTouched(n int) {
	if n > l.touched {
		l.touched = min(n, cap(l.buf))
	}
}

func (l *scratchLease) release() {
	if l == nil || l.released {
		return
	}
	l.released = true
	if l.touched > 0 {
		clear(l.buf[:l.touched])
	}

	m := l.manager
	if l.charge <= m.maxRetainedBuffer {
		whole := l.buf[:cap(l.buf)]
		m.mu.Lock()
		pool := m.pools[l.charge]
		if len(pool) < m.slotLimit && m.retainedBytes+int64(cap(whole)) <= m.retainedBudget {
			m.pools[l.charge] = append(pool, whole)
			m.retainedBytes += int64(cap(whole))
		}
		m.mu.Unlock()
	}

	l.buf = nil
	m.currentBytes.Add(-int64(l.charge))
	m.bytes.Release(int64(l.charge))
	if l.hasSlot {
		m.slots.Release(1)
	}
}
