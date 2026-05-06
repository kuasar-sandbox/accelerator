package freq

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Sketch is a lock-free Count-Min Sketch for TinyLFU-style frequency tracking.
//
// Design highlights:
//   - Touch / Estimate / Reset are all lock-free. Touch uses CAS on packed
//     uint64 words (16 × 4-bit counters per word); Reset swaps an
//     atomic.Pointer[cms]. None of the three paths ever block the others.
//   - Reset preserves rolling history: the old active CMS becomes prev, and
//     Estimate returns active + prev/2. Two consecutive Resets discard the
//     oldest generation.
//   - The CMS eats the full key []byte; there is no upstream key→uint64
//     projection. 4 independent row hashes are derived from FNV-1a with
//     row-specific starting state.
//
// Usage:
//   - Call Touch on every Get/Put to record access.
//   - CompactionFilter calls ShouldEvict to decide whether to remove a key.
//   - Halving is triggered by whichever of resetAfter (count) or
//     resetInterval (time) fires first.
type Sketch struct {
	// Two rolling generations. Touch writes to active; Estimate reads
	// active+prev/2; Reset swaps active into prev and installs a fresh active.
	active atomic.Pointer[cms]
	prev   atomic.Pointer[cms]

	width          uint64
	seeds          [cmsDepth]uint64
	evictThreshold uint8

	samples    atomic.Uint64
	resetAfter uint64

	resetInterval time.Duration
	stopTimer     chan struct{}

	persistInterval time.Duration
	persistFn       func(data []byte)
	stopPersist     chan struct{}

	// bgWg joins background goroutines (timerLoop + persistLoop) on
	// Close. Without this, persistLoop's final Marshal+persistFn call
	// can race with the caller tearing down the resources that
	// persistFn captures (e.g. RocksDB WriteOptions).
	bgWg sync.WaitGroup
}

// Config holds Sketch creation parameters.
type Config struct {
	Counters        uint64        // number of 4-bit counters (default 8M)
	ResetAfter      uint64        // halving after this many Touch calls (default 1M)
	ResetInterval   time.Duration // halving on this interval (0 = disabled)
	EvictThreshold  uint8         // CompactionFilter threshold (default 1)
	PersistInterval time.Duration // sketch persistence interval (0 = disabled)
	PersistFn       func([]byte)  // callback to persist serialised sketch
}

// New creates a Sketch with the given configuration.
func New(cfg Config) *Sketch {
	if cfg.Counters == 0 {
		cfg.Counters = DefaultCounters
	}
	if cfg.ResetAfter == 0 {
		cfg.ResetAfter = DefaultResetAfter
	}
	if cfg.EvictThreshold == 0 {
		cfg.EvictThreshold = DefaultEvictThreshold
	}

	width := nextPow2(cfg.Counters)
	seeds := [cmsDepth]uint64{0xa5a5a5a5, 0x5a5a5a5a, 0x12345678, 0x87654321}

	s := &Sketch{
		width:           width,
		seeds:           seeds,
		evictThreshold:  cfg.EvictThreshold,
		resetAfter:      cfg.ResetAfter,
		resetInterval:   cfg.ResetInterval,
		persistInterval: cfg.PersistInterval,
		persistFn:       cfg.PersistFn,
	}
	s.active.Store(newCms(width, seeds))
	// prev starts nil — Estimate must nil-check.

	if cfg.ResetInterval > 0 {
		s.stopTimer = make(chan struct{})
		s.bgWg.Add(1)
		go s.timerLoop()
	}

	if cfg.PersistInterval > 0 && cfg.PersistFn != nil {
		s.stopPersist = make(chan struct{})
		s.bgWg.Add(1)
		go s.persistLoop()
	}

	return s
}

// Touch records an access for the given key. O(len(key)).
// Lock-free: a single atomic.Pointer load plus 4 CAS loops on packed words.
func (s *Sketch) Touch(key []byte) {
	s.active.Load().increment(key)

	n := s.samples.Add(1)
	if n >= s.resetAfter {
		s.resetIfDue(n)
	}
}

// Estimate returns the approximate access frequency of the key, blending
// the active generation with a half-weight view of the previous generation.
func (s *Sketch) Estimate(key []byte) uint8 {
	v := s.active.Load().estimate(key)
	if p := s.prev.Load(); p != nil {
		v += p.estimate(key) >> 1
	}
	if v > 255 {
		return 255
	}
	return uint8(v)
}

// ShouldEvict returns true if the key's frequency is at or below the
// configured eviction threshold.
func (s *Sketch) ShouldEvict(key []byte) bool {
	return s.Estimate(key) <= s.evictThreshold
}

// Reset rolls the active generation into prev and installs a fresh active.
// Lock-free: one allocation + two atomic pointer ops. Takes ~µs for 1M
// counters, independent of concurrent Touch/Estimate load.
func (s *Sketch) Reset() {
	fresh := newCms(s.width, s.seeds)
	old := s.active.Swap(fresh)
	s.prev.Store(old)
	s.samples.Store(0)
}

// Marshal serialises the active generation for persistence. prev is
// intentionally dropped — it is historical rolling state with no value
// after a restart.
//
// Format: [version(1)] [width(8)] [row0 words] [row1 words] [row2 words] [row3 words]
// version is sketchFormatVersion = 0x02. Words are little-endian uint64.
// Per-word atomic.Load — not a linearisable snapshot, which CMS tolerates.
func (s *Sketch) Marshal() []byte {
	c := s.active.Load()
	wordsPerRow := len(c.rows[0])
	bytesPerRow := wordsPerRow * 8
	buf := make([]byte, 1+8+cmsDepth*bytesPerRow)
	buf[0] = sketchFormatVersion
	binary.LittleEndian.PutUint64(buf[1:9], c.width)
	off := 9
	for i := 0; i < cmsDepth; i++ {
		for j := 0; j < wordsPerRow; j++ {
			w := atomic.LoadUint64(&c.rows[i][j])
			binary.LittleEndian.PutUint64(buf[off:], w)
			off += 8
		}
	}
	return buf
}

// Unmarshal restores sketch state from previously persisted data. Only
// active is restored; prev stays empty. Intended to be called on a freshly
// constructed Sketch before it is exposed to concurrent access.
func (s *Sketch) Unmarshal(data []byte) error {
	if len(data) < 9 {
		return fmt.Errorf("freq: data too short (%d bytes)", len(data))
	}
	if data[0] != sketchFormatVersion {
		return fmt.Errorf("freq: unsupported sketch format version 0x%02x (legacy data discarded)", data[0])
	}
	width := binary.LittleEndian.Uint64(data[1:9])
	if width != s.width {
		return fmt.Errorf("freq: width mismatch: persisted %d, current %d", width, s.width)
	}
	wordsPerRow := int((width + nibblesPerWord - 1) / nibblesPerWord)
	bytesPerRow := wordsPerRow * 8
	expected := 9 + cmsDepth*bytesPerRow
	if len(data) != expected {
		return fmt.Errorf("freq: data size %d, expected %d", len(data), expected)
	}
	c := s.active.Load()
	off := 9
	for i := 0; i < cmsDepth; i++ {
		for j := 0; j < wordsPerRow; j++ {
			w := binary.LittleEndian.Uint64(data[off:])
			atomic.StoreUint64(&c.rows[i][j], w)
			off += 8
		}
	}
	return nil
}

// Close stops background goroutines and waits for them to exit before
// returning. This join is critical when persistFn captures resources
// owned by the caller (e.g. RocksDB WriteOptions) that will be torn
// down immediately after Close returns — without the wait, the final
// Marshal+persistFn tick races the teardown. Not safe to call more
// than once.
func (s *Sketch) Close() error {
	if s.stopTimer != nil {
		close(s.stopTimer)
	}
	if s.stopPersist != nil {
		close(s.stopPersist)
	}
	s.bgWg.Wait()
	return nil
}

func (s *Sketch) resetIfDue(n uint64) {
	if s.samples.CompareAndSwap(n, 0) {
		fresh := newCms(s.width, s.seeds)
		old := s.active.Swap(fresh)
		s.prev.Store(old)
	}
}

func (s *Sketch) timerLoop() {
	defer s.bgWg.Done()
	ticker := time.NewTicker(s.resetInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.Reset()
		case <-s.stopTimer:
			return
		}
	}
}

func (s *Sketch) persistLoop() {
	defer s.bgWg.Done()
	ticker := time.NewTicker(s.persistInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if data := s.Marshal(); len(data) > 0 {
				s.persistFn(data)
			}
		case <-s.stopPersist:
			if data := s.Marshal(); len(data) > 0 {
				s.persistFn(data)
			}
			return
		}
	}
}

// ─── Count-Min Sketch (4-row, 4-bit counters, packed in uint64 words) ──

const (
	cmsDepth       = 4
	nibblesPerWord = 16 // 16 × 4-bit counters per uint64 word
	nibbleMask     = uint64(0xf)

	fnvOffsetBasis = 14695981039346656037
	fnvPrime       = 1099511628211

	// sketchFormatVersion is the first byte of Marshal output. The legacy
	// (pre-lock-free) format started with the low byte of `width`, which
	// is effectively never 0x02 for any realistic counter count — so 0x02
	// serves as an unambiguous sentinel to distinguish the two layouts.
	sketchFormatVersion byte = 0x02
)

// cms is a packed Count-Min Sketch with no synchronisation primitives of
// its own. Touch increments via atomic CAS on 4-bit nibbles; Estimate reads
// via atomic.LoadUint64. Concurrent access is safe without mutexes.
type cms struct {
	rows  [cmsDepth][]uint64
	width uint64 // counter count per row (power of 2)
	seeds [cmsDepth]uint64
}

func newCms(width uint64, seeds [cmsDepth]uint64) *cms {
	wordsPerRow := (width + nibblesPerWord - 1) / nibblesPerWord
	c := &cms{width: width, seeds: seeds}
	for i := range c.rows {
		c.rows[i] = make([]uint64, wordsPerRow)
	}
	return c
}

func (c *cms) increment(key []byte) {
	for i := 0; i < cmsDepth; i++ {
		idx := c.index(key, i)
		c.inc4bit(i, idx)
	}
}

func (c *cms) estimate(key []byte) int {
	min := math.MaxInt
	for i := 0; i < cmsDepth; i++ {
		idx := c.index(key, i)
		v := c.get4bit(i, idx)
		if int(v) < min {
			min = int(v)
		}
	}
	return min
}

// inc4bit performs a saturating +1 on a single 4-bit nibble using CAS on
// the containing uint64 word. Retries on contention; returns early when
// the nibble has reached the 4-bit ceiling (15).
func (c *cms) inc4bit(row int, idx uint64) {
	wordIdx := idx / nibblesPerWord
	shift := (idx % nibblesPerWord) * 4
	mask := nibbleMask << shift
	p := &c.rows[row][wordIdx]
	for {
		w := atomic.LoadUint64(p)
		val := (w & mask) >> shift
		if val >= 15 {
			return // saturated
		}
		newW := (w &^ mask) | ((val + 1) << shift)
		if atomic.CompareAndSwapUint64(p, w, newW) {
			return
		}
		// CAS failed: another goroutine modified the word; retry.
	}
}

func (c *cms) get4bit(row int, idx uint64) uint8 {
	wordIdx := idx / nibblesPerWord
	shift := (idx % nibblesPerWord) * 4
	w := atomic.LoadUint64(&c.rows[row][wordIdx])
	return uint8((w >> shift) & nibbleMask)
}

// index derives a row-specific bucket for the given key via FNV-1a with
// row-seeded starting state. Eats the full key []byte — no upstream
// projection. Deterministic across restarts because seeds are constants.
func (c *cms) index(key []byte, row int) uint64 {
	h := fnvOffsetBasis ^ c.seeds[row]
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime
	}
	return h & (c.width - 1) // fast mod (width is power of 2)
}

func nextPow2(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}
