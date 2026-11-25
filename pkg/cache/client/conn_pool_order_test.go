package client

import "testing"

func TestIdleRingOrderAndWrap(t *testing.T) {
	p := &ConnPool{free: newConnDeque(4), changed: make(chan struct{}, 1)}
	a, b, c, d := &PoolConn{}, &PoolConn{}, &PoolConn{}, &PoolConn{}
	for round := 0; round < 12; round++ {
		for _, pc := range []*PoolConn{a, b, c, d} {
			p.Release(pc, true)
		}
		if got := p.takeNewest(); got != d {
			t.Fatal("Acquire must take newest")
		}
		if got := p.popOldest(); got != a {
			t.Fatal("health check must take oldest")
		}
		p.Release(a, true)
		if got := p.takeNewest(); got != a {
			t.Fatal("successful health check must return at newest end")
		}
		if p.popOldest() != b || p.popOldest() != c || p.takeNewest() != nil {
			t.Fatal("ring order or empty state")
		}
		for _, pc := range p.free.items {
			if pc != nil {
				t.Fatal("removed connection retained")
			}
		}
	}
}
