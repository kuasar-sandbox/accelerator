// Package client provides wire-protocol clients for cache-ctl services.
package client

import (
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Pool is a simple gRPC connection pool (kept for health check client).
type Pool struct {
	conns []*grpc.ClientConn
	idx   atomic.Uint64
}

// DialPool creates a pool of n gRPC connections to the given endpoint.
func DialPool(endpoint string, n int) (*Pool, error) {
	if n <= 0 {
		n = 1
	}
	opts := dialOpts()
	p := &Pool{conns: make([]*grpc.ClientConn, n)}
	for i := range p.conns {
		cc, err := grpc.NewClient(endpoint, opts...)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.conns[i] = cc
	}
	return p, nil
}

// Next returns the next connection in round-robin order.
func (p *Pool) Next() *grpc.ClientConn {
	i := p.idx.Add(1)
	return p.conns[int(i)%len(p.conns)]
}

// Close closes all connections.
func (p *Pool) Close() error {
	var firstErr error
	for _, cc := range p.conns {
		if cc != nil {
			if err := cc.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func dialOpts() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(4<<20),
			grpc.MaxCallSendMsgSize(4<<20),
		),
		grpc.WithInitialWindowSize(64 << 20),
		grpc.WithInitialConnWindowSize(128 << 20),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}
