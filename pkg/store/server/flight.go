package server

import (
	"context"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type objectTargetKey struct {
	partition store.Partition
	key       store.ContentKey
}

type objectFlightKey struct {
	partition   store.Partition
	key         store.ContentKey
	generations string
	writeEpoch  uint64
}

type objectFlight struct {
	done           chan struct{}
	found          bool
	data           []byte
	err            error
	leaderCanceled bool
	waiterN        int
}

type objectFlights struct {
	mu          sync.Mutex
	calls       map[objectFlightKey]*objectFlight
	writeEpochs map[objectTargetKey]uint64
}

func generationsKey(gens []store.Generation) string {
	var b strings.Builder
	for _, g := range gens {
		b.WriteString(string(g))
		b.WriteByte(0)
	}
	return b.String()
}

func (g *objectFlights) writeEpoch(partition store.Partition, key store.ContentKey) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.writeEpochs[objectTargetKey{partition: partition, key: key}]
}

func (g *objectFlights) advanceWriteEpoch(partition store.Partition, key store.ContentKey) {
	g.mu.Lock()
	if g.writeEpochs == nil {
		g.writeEpochs = make(map[objectTargetKey]uint64)
	}
	g.writeEpochs[objectTargetKey{partition: partition, key: key}]++
	g.mu.Unlock()
}

func (g *objectFlights) do(ctx context.Context, key objectFlightKey, fn func(context.Context) (bool, []byte, error)) (bool, []byte, error) {
	for {
		g.mu.Lock()
		if call := g.calls[key]; call != nil {
			call.waiterN++
			g.mu.Unlock()
			select {
			case <-call.done:
			case <-ctx.Done():
				g.mu.Lock()
				call.waiterN--
				g.mu.Unlock()
				return false, nil, ctx.Err()
			}
			g.mu.Lock()
			call.waiterN--
			found, data, err := call.found, call.data, call.err
			leaderCanceled := call.leaderCanceled
			if err != nil && leaderCanceled && ctx.Err() == nil {
				key.writeEpoch = g.writeEpochs[objectTargetKey{partition: key.partition, key: key.key}]
				g.mu.Unlock()
				continue
			}
			g.mu.Unlock()
			return found, data, err
		}
		call := &objectFlight{done: make(chan struct{})}
		if g.calls == nil {
			g.calls = make(map[objectFlightKey]*objectFlight)
		}
		g.calls[key] = call
		g.mu.Unlock()

		found, data, err := fn(ctx)
		leaderCanceled := ctx.Err() != nil

		g.mu.Lock()
		call.found, call.data, call.err = found, data, err
		call.leaderCanceled = leaderCanceled
		delete(g.calls, key)
		close(call.done)
		g.mu.Unlock()
		return found, data, err
	}
}
