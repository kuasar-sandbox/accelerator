package client

// connDeque is a bounded ring buffer for idle connections. ConnPool.mu
// serializes all operations; the oldest connection is at the front and the
// newest returned connection is at the back.
type connDeque struct {
	items []*PoolConn
	head  int
	size  int
}

func newConnDeque(capacity int) connDeque {
	if capacity < 1 {
		capacity = 1
	}
	return connDeque{items: make([]*PoolConn, capacity)}
}

func (q *connDeque) pushBack(pc *PoolConn) bool {
	if q.size == len(q.items) {
		return false
	}
	i := (q.head + q.size) % len(q.items)
	q.items[i] = pc
	q.size++
	return true
}

func (q *connDeque) popBack() *PoolConn {
	if q.size == 0 {
		return nil
	}
	i := (q.head + q.size - 1) % len(q.items)
	pc := q.items[i]
	q.items[i] = nil
	q.size--
	return pc
}

func (q *connDeque) popFront() *PoolConn {
	if q.size == 0 {
		return nil
	}
	pc := q.items[q.head]
	q.items[q.head] = nil
	q.head = (q.head + 1) % len(q.items)
	q.size--
	return pc
}
