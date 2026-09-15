package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func poolListener(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
		}
	}()
	t.Cleanup(func() {
		l.Close()
		<-done
		for _, c := range conns {
			c.Close()
		}
	})
	return l.Addr().String()
}

func TestDialFailureReturnsWithoutWaitingForPool(t *testing.T) {
	p, err := DialConnPool(t.TempDir()+"/absent.sock", ConnPoolConfig{MaxSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = p.Acquire(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) || p.current.Load() != 0 {
		t.Fatalf("dial failure lost or leaked capacity: err=%v current=%d", err, p.current.Load())
	}
}

func TestBadConnectionWakesCapacityWaiter(t *testing.T) {
	p, err := DialConnPool(poolListener(t), ConnPoolConfig{MaxSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	pc, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready := make(chan struct{})
	waitCtx := &waiterContext{Context: ctx, ready: ready}
	done := make(chan error, 1)
	go func() {
		next, err := p.Acquire(waitCtx)
		if err == nil {
			p.Release(next, true)
		}
		done <- err
	}()
	// This pool has no health/refill worker. Only releasing capacity can
	// advance the original pending Acquire when no healthy conn is returned.
	<-ready
	p.Release(pc, false)
	if err := <-done; err != nil {
		t.Fatalf("waiter stranded: %v", err)
	}
}

func TestAcquireCloseAndRefillShareCapacity(t *testing.T) {
	const limit = 4
	p, err := DialConnPool(poolListener(t), ConnPoolConfig{PoolSize: limit, MaxSize: limit})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	active := make(chan struct{}, limit)
	release := make(chan struct{})
	var acquired atomic.Int32
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				pc, err := p.Acquire(ctx)
				cancel()
				if err != nil {
					return
				}
				if acquired.Add(1) <= limit {
					active <- struct{}{}
					<-release
				}
				if n := p.current.Load(); n > limit || n < 1 {
					t.Errorf("capacity=%d", n)
				}
				p.Release(pc, j%2 == 0)
				p.refill()
			}
		}()
	}
	for range limit {
		<-active
	}
	close(release)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if n := p.current.Load(); n != 0 {
		t.Fatalf("closed capacity=%d", n)
	}
	if _, err := p.Acquire(context.Background()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed pool revived: %v", err)
	}
}

func TestFullPoolWaiterCancellationAndClose(t *testing.T) {
	for _, closing := range []bool{false, true} {
		p, err := DialConnPool(poolListener(t), ConnPoolConfig{MaxSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		pc, err := p.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := p.Acquire(ctx); done <- err }()
		want := context.Canceled
		if closing {
			p.Close()
			want = net.ErrClosed
		} else {
			cancel()
		}
		select {
		case err := <-done:
			if !errors.Is(err, want) {
				t.Fatalf("waiter error=%v, want %v", err, want)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not exit")
		}
		cancel()
		p.Release(pc, true)
		p.Close()
		if p.current.Load() != 0 {
			t.Fatal("connection resurrected after Close")
		}
	}
}

// Done is evaluated by Acquire only once its fast path and reservation fail.
// This establishes an actual full-pool waiter without scheduler sleeps.
type waiterContext struct {
	context.Context
	ready     chan struct{}
	holdDone  <-chan struct{}
	once      sync.Once
	checks    atomic.Int32
	secondErr chan struct{}
	holdErr   <-chan struct{}
}

func (c *waiterContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.ready)
		if c.holdDone != nil {
			<-c.holdDone
		}
	})
	return c.Context.Done()
}
func (c *waiterContext) Err() error {
	if c.checks.Add(1) == 2 && c.secondErr != nil {
		close(c.secondErr)
		<-c.holdErr
	}
	return c.Context.Err()
}
func TestCanceledWaiterRelaysConsumedCapacityWake(t *testing.T) {
	p, err := DialConnPool(poolListener(t), ConnPoolConfig{MaxSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	pc, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allowA, allowB := make(chan struct{}), make(chan struct{})
	a := &waiterContext{Context: ctx, ready: make(chan struct{}), secondErr: make(chan struct{}), holdErr: allowA}
	bctx, bcancel := context.WithTimeout(context.Background(), time.Second)
	defer bcancel()
	b := &waiterContext{Context: bctx, ready: make(chan struct{}), holdDone: allowB}
	adone, bdone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := p.Acquire(a); adone <- err }()
	<-a.ready
	go func() {
		pc, err := p.Acquire(b)
		if err == nil {
			p.Release(pc, true)
		}
		bdone <- err
	}()
	<-b.ready
	p.Release(pc, false)
	<-a.secondErr
	cancel()
	close(allowA)
	if err := <-adone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(allowB)
	if err := <-bdone; err != nil {
		t.Fatalf("capacity wake lost to canceled waiter: %v", err)
	}
}

func TestSuccessfulWaiterRelaysConsumedCapacityWake(t *testing.T) {
	p, err := DialConnPool(poolListener(t), ConnPoolConfig{MaxSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	bad, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Discard the earlier spare-capacity notification; the pool is now full.
	select {
	case <-p.changed:
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	allowA, allowB := make(chan struct{}), make(chan struct{})
	a := &waiterContext{Context: ctx, ready: make(chan struct{}), secondErr: make(chan struct{}), holdErr: allowA}
	b := &waiterContext{Context: ctx, ready: make(chan struct{}), holdDone: allowB}
	adone := make(chan *PoolConn, 1)
	bdone := make(chan error, 1)
	go func() { pc, _ := p.Acquire(a); adone <- pc }()
	<-a.ready
	go func() {
		pc, err := p.Acquire(b)
		if err == nil {
			p.Release(pc, true)
		}
		bdone <- err
	}()
	<-b.ready
	p.Release(bad, false)
	<-a.secondErr // A owns the only wake; B has not entered its select yet.
	p.Release(healthy, true)
	close(allowA)
	got := <-adone
	if got == nil {
		t.Fatal("first waiter failed")
	}
	defer p.Release(got, true)
	close(allowB)
	if err := <-bdone; err != nil {
		t.Fatalf("successful waiter lost spare-capacity wake: %v", err)
	}
}
