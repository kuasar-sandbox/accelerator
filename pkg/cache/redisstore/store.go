// Package redisstore implements object and shard cache storage through the
// narrow RESP2 GET/SET surface of a Redis-compatible server over UDS or TCP.
package redisstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	"github.com/kuasar-sandbox/accelerator/pkg/cache/runtime"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const (
	defaultGetPool = 32
	defaultSetPool = 8
	defaultTimeout = 2 * time.Second
	readerSize     = 4 << 10
)

var (
	ErrClosed        = errors.New("redisstore: closed")
	ErrValueTooLarge = errors.New("redisstore: value exceeds cache payload limit")
)

type poolKind uint8

const (
	getPool poolKind = iota
	setPool
)

const (
	opWaiting uint32 = iota
	opAbandoned
	opDelivered
)

// Store implements both complete-object and EC-shard cache contracts. The
// methods share one codec and differ only in the encoded key kind.
type Store struct {
	endpoint string
	network  string
	address  string
	timeout  time.Duration
	blobs    cache.BlobPool

	stop      chan struct{}
	closed    atomic.Bool
	closeOnce sync.Once
	wg        sync.WaitGroup
	workers   []*worker
	get       workerPool
	set       workerPool

	probeMu     sync.Mutex
	probeConn   net.Conn
	probeReader *bufio.Reader

	getConnected atomic.Int64
	setConnected atomic.Int64
	getInflight  atomic.Int64
	setInflight  atomic.Int64
	waiters      atomic.Int64
	draining     atomic.Int64

	getHits          atomic.Uint64
	getMisses        atomic.Uint64
	sets             atomic.Uint64
	cancelled        atomic.Uint64
	lateBytesDrained atomic.Uint64
	reconnects       atomic.Uint64
	protocolErrors   atomic.Uint64
	backendErrors    atomic.Uint64

	getLatency  obstat.Hist
	setLatency  obstat.Hist
	waitLatency obstat.Hist
}

type workerPool struct {
	kind      poolKind
	size      int
	available chan *worker
	store     *Store
}

type worker struct {
	store *Store
	pool  *workerPool
	ops   chan *operation

	connMu sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
}

type operation struct {
	kind  poolKind
	key   [keySize]byte
	value []byte
	start time.Time

	state          atomic.Uint32
	result         chan operationResult
	written        chan struct{}
	abandonedReady chan struct{}
}

type operationResult struct {
	result cache.CacheResult
	blob   cache.Blob
	err    error
}

// Open creates and eagerly connects all backend workers. Startup fails if the
// configured Redis-compatible server cannot satisfy the complete pool.
func Open(cfg runtime.RedisConfig, blobs cache.BlobPool) (*Store, error) {
	network, address, err := runtime.ParseRedisEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("redisstore: %w", err)
	}
	getSize := cfg.GetPool
	if getSize == 0 {
		getSize = defaultGetPool
	}
	setSize := cfg.SetPool
	if setSize == 0 {
		setSize = defaultSetPool
	}
	if getSize < 0 || setSize < 0 {
		return nil, errors.New("redisstore: pool sizes must be non-negative")
	}
	timeout := defaultTimeout
	if cfg.Timeout != "" {
		parsed, err := time.ParseDuration(cfg.Timeout)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("redisstore: invalid timeout %q", cfg.Timeout)
		}
		timeout = parsed
	}
	if blobs == nil {
		blobs = cache.DefaultPool
	}

	s := &Store{
		endpoint: cfg.Endpoint,
		network:  network,
		address:  address,
		timeout:  timeout,
		blobs:    blobs,
		stop:     make(chan struct{}),
	}
	s.get = workerPool{kind: getPool, size: getSize, available: make(chan *worker, getSize), store: s}
	s.set = workerPool{kind: setPool, size: setSize, available: make(chan *worker, setSize), store: s}

	for i := 0; i < getSize; i++ {
		if err := s.addWorker(&s.get); err != nil {
			s.closeInitialWorkers()
			return nil, fmt.Errorf("redisstore: connect GET worker %d: %w", i, err)
		}
	}
	for i := 0; i < setSize; i++ {
		if err := s.addWorker(&s.set); err != nil {
			s.closeInitialWorkers()
			return nil, fmt.Errorf("redisstore: connect SET worker %d: %w", i, err)
		}
	}
	for _, w := range s.workers {
		s.wg.Add(1)
		go w.run()
		w.pool.available <- w
	}
	return s, nil
}

func (s *Store) addWorker(pool *workerPool) error {
	w := &worker{store: s, pool: pool, ops: make(chan *operation)}
	conn, err := s.dial()
	if err != nil {
		return err
	}
	w.installConn(conn)
	s.workers = append(s.workers, w)
	return nil
}

func (s *Store) closeInitialWorkers() {
	for _, w := range s.workers {
		w.dropConn()
	}
}

func (s *Store) dial() (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	return s.dialContext(ctx)
}

func (s *Store) dialContext(ctx context.Context) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, s.network, s.address)
}

func (s *Store) Get(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	encoded, err := encodeKey(p, kindObject, key)
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	res := s.do(ctx, &s.get, encoded, nil)
	return res.result, res.blob, res.err
}

func (s *Store) Fill(ctx context.Context, p store.Partition, key store.ContentKey, data []byte) error {
	encoded, err := encodeKey(p, kindObject, key)
	if err != nil {
		return err
	}
	if err := validateFillSize(data); err != nil {
		return err
	}
	return s.do(ctx, &s.set, encoded, data).err
}

func (s *Store) GetShard(ctx context.Context, p store.Partition, key store.ContentKey) (cache.CacheResult, cache.Blob, error) {
	encoded, err := encodeKey(p, kindShard, key)
	if err != nil {
		return cache.CacheMiss, nil, err
	}
	res := s.do(ctx, &s.get, encoded, nil)
	return res.result, res.blob, res.err
}

func (s *Store) FillShard(ctx context.Context, p store.Partition, key store.ContentKey, value []byte) error {
	encoded, err := encodeKey(p, kindShard, key)
	if err != nil {
		return err
	}
	if err := validateFillSize(value); err != nil {
		return err
	}
	return s.do(ctx, &s.set, encoded, value).err
}

func validateFillSize(value []byte) error {
	if len(value) > maxBulkSize {
		return fmt.Errorf("%w: %d > %d", ErrValueTooLarge, len(value), maxBulkSize)
	}
	return nil
}

func (s *Store) do(ctx context.Context, pool *workerPool, key [keySize]byte, value []byte) operationResult {
	if err := ctx.Err(); err != nil {
		s.cancelled.Add(1)
		return operationResult{result: cache.CacheMiss, err: err}
	}
	w, err := pool.acquire(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.cancelled.Add(1)
		}
		return operationResult{result: cache.CacheMiss, err: err}
	}
	if err := ctx.Err(); err != nil {
		pool.release(w)
		s.cancelled.Add(1)
		return operationResult{result: cache.CacheMiss, err: err}
	}
	op := &operation{
		kind:           pool.kind,
		key:            key,
		value:          value,
		start:          time.Now(),
		result:         make(chan operationResult, 1),
		written:        make(chan struct{}),
		abandonedReady: make(chan struct{}),
	}
	s.inflight(pool.kind).Add(1)
	select {
	case w.ops <- op:
	case <-ctx.Done():
		s.inflight(pool.kind).Add(-1)
		pool.release(w)
		s.cancelled.Add(1)
		return operationResult{result: cache.CacheMiss, err: ctx.Err()}
	case <-s.stop:
		s.inflight(pool.kind).Add(-1)
		pool.release(w)
		return operationResult{result: cache.CacheMiss, err: ErrClosed}
	}

	select {
	case res := <-op.result:
		return res
	case <-ctx.Done():
		if op.state.CompareAndSwap(opWaiting, opAbandoned) {
			s.cancelled.Add(1)
			s.draining.Add(1)
			close(op.abandonedReady)
			if op.kind == setPool {
				// Fill's data slice is borrowed only until this method returns.
				// The worker closes written after writev no longer references it.
				<-op.written
			}
			return operationResult{result: cache.CacheMiss, err: ctx.Err()}
		}
		// Backend completion won the race. Consume and return that result so
		// a pooled Blob cannot be stranded in the buffered result channel.
		return <-op.result
	}
}

func (p *workerPool) acquire(ctx context.Context) (*worker, error) {
	if p.store.closed.Load() {
		return nil, ErrClosed
	}
	start := time.Now()
	select {
	case w := <-p.available:
		p.store.waitLatency.Record(time.Since(start))
		return w, nil
	default:
	}
	p.store.waiters.Add(1)
	defer p.store.waiters.Add(-1)
	select {
	case w := <-p.available:
		p.store.waitLatency.Record(time.Since(start))
		if p.store.closed.Load() {
			return nil, ErrClosed
		}
		return w, nil
	case <-ctx.Done():
		p.store.waitLatency.Record(time.Since(start))
		return nil, ctx.Err()
	case <-p.store.stop:
		p.store.waitLatency.Record(time.Since(start))
		return nil, ErrClosed
	}
}

func (p *workerPool) release(w *worker) {
	select {
	case p.available <- w:
	case <-p.store.stop:
	}
}

func (w *worker) run() {
	defer w.store.wg.Done()
	defer w.dropConn()
	for {
		select {
		case <-w.store.stop:
			return
		case op := <-w.ops:
			res, reconnect := w.execute(op)
			w.complete(op, res)
			if reconnect {
				w.dropConn()
				if !w.reconnect() {
					return
				}
			}
			w.pool.release(w)
		}
	}
}

func (w *worker) execute(op *operation) (operationResult, bool) {
	conn, reader := w.currentConn()
	if conn == nil || reader == nil {
		close(op.written)
		return operationResult{result: cache.CacheMiss, err: ErrClosed}, true
	}
	if err := conn.SetDeadline(time.Now().Add(w.store.timeout)); err != nil {
		close(op.written)
		return operationResult{result: cache.CacheMiss, err: err}, true
	}

	var res operationResult
	var err error
	if op.kind == getPool {
		err = writeGet(conn, op.key[:])
		close(op.written)
		if err == nil {
			res.result, res.blob, err = readGetResponse(reader, w.store.blobs)
		}
	} else {
		err = writeSet(conn, op.key[:], op.value)
		close(op.written)
		if err == nil {
			err = readSetResponse(reader)
		}
		res.result = cache.CacheHit
	}
	res.err = err
	if err == nil {
		_ = conn.SetDeadline(time.Time{})
		return res, false
	}

	w.store.backendErrors.Add(1)
	if isProtocolError(err) {
		w.store.protocolErrors.Add(1)
	}
	// A valid -ERR response preserves RESP framing and the connection can
	// be reused. I/O and malformed responses require a replacement.
	return res, !isResponseError(err)
}

func (w *worker) complete(op *operation, res operationResult) {
	elapsed := time.Since(op.start)
	w.store.inflight(op.kind).Add(-1)
	if op.kind == getPool {
		w.store.getLatency.Record(elapsed)
		if res.err == nil {
			if res.result == cache.CacheHit {
				w.store.getHits.Add(1)
			} else {
				w.store.getMisses.Add(1)
			}
		}
	} else {
		w.store.setLatency.Record(elapsed)
		if res.err == nil {
			w.store.sets.Add(1)
		}
	}

	if op.state.CompareAndSwap(opWaiting, opDelivered) {
		op.result <- res
		return
	}
	<-op.abandonedReady
	if res.blob != nil {
		w.store.lateBytesDrained.Add(uint64(len(res.blob.Bytes())))
		res.blob.Release()
	}
	w.store.draining.Add(-1)
}

func (w *worker) reconnect() bool {
	delay := 20 * time.Millisecond
	for {
		select {
		case <-w.store.stop:
			return false
		default:
		}
		conn, err := w.store.dial()
		if err == nil {
			if !w.installReconnected(conn) {
				return false
			}
			w.store.reconnects.Add(1)
			return true
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-w.store.stop:
			timer.Stop()
			return false
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func (w *worker) installReconnected(conn net.Conn) bool {
	select {
	case <-w.store.stop:
		_ = conn.Close()
		return false
	default:
		w.installConn(conn)
		return true
	}
}

func (w *worker) currentConn() (net.Conn, *bufio.Reader) {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	return w.conn, w.reader
}

func (w *worker) installConn(conn net.Conn) {
	w.connMu.Lock()
	w.conn = conn
	w.reader = bufio.NewReaderSize(conn, readerSize)
	w.connMu.Unlock()
	w.store.connected(w.pool.kind).Add(1)
}

func (w *worker) dropConn() {
	w.connMu.Lock()
	conn := w.conn
	w.conn = nil
	w.reader = nil
	w.connMu.Unlock()
	if conn != nil {
		w.store.connected(w.pool.kind).Add(-1)
		_ = conn.Close()
	}
}

func (s *Store) connected(kind poolKind) *atomic.Int64 {
	if kind == getPool {
		return &s.getConnected
	}
	return &s.setConnected
}

func (s *Store) inflight(kind poolKind) *atomic.Int64 {
	if kind == getPool {
		return &s.getInflight
	}
	return &s.setInflight
}

// Healthy reports whether each operation class has at least one connected
// worker. It is intended for readiness plumbing, not per-request admission.
func (s *Store) Healthy() bool {
	return !s.closed.Load() && s.getConnected.Load() > 0 && s.setConnected.Load() > 0
}

// Probe verifies end-to-end RESP service on a dedicated connection. It sends
// only GET for a reserved binary key and deliberately bypasses hot-path pools
// and counters so readiness traffic cannot distort cache statistics.
func (s *Store) Probe(ctx context.Context) error {
	if s.closed.Load() {
		return ErrClosed
	}
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	if s.closed.Load() {
		return ErrClosed
	}
	if s.probeConn == nil {
		conn, err := s.dialContext(ctx)
		if err != nil {
			return err
		}
		s.probeConn = conn
		s.probeReader = bufio.NewReaderSize(conn, readerSize)
	}

	deadline := time.Now().Add(s.timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := s.probeConn.SetDeadline(deadline); err != nil {
		s.dropProbeLocked()
		return err
	}
	// Version 0 is reserved for control-plane probes. Data keys are always
	// version 1, so this GET cannot accidentally read a user-populated value.
	var key [keySize]byte
	err := writeGet(s.probeConn, key[:])
	if err == nil {
		_, blob, readErr := readGetResponse(s.probeReader, cache.DefaultPool)
		if blob != nil {
			blob.Release()
		}
		err = readErr
	}
	if err != nil {
		if !isResponseError(err) {
			s.dropProbeLocked()
		}
		return err
	}
	_ = s.probeConn.SetDeadline(time.Time{})
	return nil
}

func (s *Store) dropProbeLocked() {
	if s.probeConn != nil {
		_ = s.probeConn.Close()
	}
	s.probeConn = nil
	s.probeReader = nil
}

func (s *Store) Stats() Stats {
	return Stats{
		Endpoint:         s.endpoint,
		Transport:        s.network,
		GetPoolSize:      int64(s.get.size),
		SetPoolSize:      int64(s.set.size),
		GetConnected:     s.getConnected.Load(),
		SetConnected:     s.setConnected.Load(),
		GetInflight:      s.getInflight.Load(),
		SetInflight:      s.setInflight.Load(),
		PoolWaiters:      s.waiters.Load(),
		Draining:         s.draining.Load(),
		GetHits:          s.getHits.Load(),
		GetMisses:        s.getMisses.Load(),
		Sets:             s.sets.Load(),
		Cancelled:        s.cancelled.Load(),
		LateBytesDrained: s.lateBytesDrained.Load(),
		Reconnects:       s.reconnects.Load(),
		ProtocolErrors:   s.protocolErrors.Load(),
		BackendErrors:    s.backendErrors.Load(),
		GetLatency:       s.getLatency.Snapshot(),
		SetLatency:       s.setLatency.Snapshot(),
		PoolWaitLatency:  s.waitLatency.Snapshot(),
	}
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.stop)
		s.probeMu.Lock()
		s.dropProbeLocked()
		s.probeMu.Unlock()
		for _, w := range s.workers {
			w.dropConn()
		}
		s.wg.Wait()
	})
	return nil
}

var _ cache.Tier = (*Store)(nil)
var _ cache.ShardTier = (*Store)(nil)
var _ io.Closer = (*Store)(nil)
