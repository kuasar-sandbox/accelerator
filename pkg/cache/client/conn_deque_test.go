package client

import "testing"

func TestConnDequeLIFOAndFIFOMaintenance(t *testing.T) {
	q := newConnDeque(3)
	a, b, c, d := &PoolConn{}, &PoolConn{}, &PoolConn{}, &PoolConn{}
	for _, pc := range []*PoolConn{a, b, c} {
		if !q.pushBack(pc) {
			t.Fatal("push into available capacity failed")
		}
	}
	if q.pushBack(d) {
		t.Fatal("push beyond capacity succeeded")
	}
	if got := q.popBack(); got != c {
		t.Fatalf("LIFO pop = %p, want %p", got, c)
	}
	if !q.pushBack(d) {
		t.Fatal("push after pop failed")
	}
	if got := q.popFront(); got != a {
		t.Fatalf("FIFO pop = %p, want %p", got, a)
	}
	if got := q.popBack(); got != d {
		t.Fatalf("wrapped LIFO pop = %p, want %p", got, d)
	}
	if got := q.popFront(); got != b {
		t.Fatalf("remaining FIFO pop = %p, want %p", got, b)
	}
	if got := q.popFront(); got != nil {
		t.Fatalf("empty pop = %p, want nil", got)
	}
	if q.size != 0 {
		t.Fatalf("queue size = %d, want 0", q.size)
	}
}
