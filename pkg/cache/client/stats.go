package client

import "github.com/kuasar-sandbox/accelerator/pkg/cache"

// Stats returns a snapshot of per-peer counters. The peer ID is passed
// in by the EC tier at aggregation time; the client layer doesn't own
// peer identity (the connection pool is keyed by endpoint). Safe to
// call concurrently with request handling.
func (c *shardImpl) Stats(id string) cache.PeerStats {
	return cache.PeerStats{
		ID:        id,
		Endpoint:  c.endpoint,
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Errors:    c.errors.Load(),
		Cancelled: c.cancelled.Load(),
		Fills:     c.fills.Load(),
	}
}
