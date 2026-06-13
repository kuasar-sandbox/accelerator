package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/kuasar-sandbox/sandbox-accelerator/internal/util"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache"
	cacheserver "github.com/kuasar-sandbox/sandbox-accelerator/pkg/cache/server"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store/server"
)

// errCacheReadOnly is returned for cache-protocol writes: store-ctl's
// embedded cache server is a read path over the store backend. Clients
// write through the store gRPC, never the cache protocol.
var errCacheReadOnly = errors.New("store-ctl cache server is read-only; write via the store gRPC")

// storeTier adapts the store-backed cache.Getter into a read-only
// cache.Tier for the wire handler. Get is promoted from the embedded
// Getter (a straight backend read); Fill is rejected, and the
// RejectsWrites marker lets the handler short-circuit ObjectPut with
// the canonical "writes not supported" status before touching Fill.
type storeTier struct{ cache.Getter }

func (storeTier) Fill(context.Context, store.Partition, store.ContentKey, []byte) error {
	return errCacheReadOnly
}

func (storeTier) RejectsWrites() bool { return true }

// startCacheWireServer starts an embedded read-only cache wire server on
// cfg.CacheListen, serving cache-protocol object reads
// (chunk/manifest/blob) straight from the store backend — the same bytes
// the store gRPC serves, reachable by cache clients without a separate
// cache-ctl. It is a pass-through (no L1 caching: every read hits the
// backend); for real caching run a cache-ctl with this store as origin.
// Returns a stop function for graceful shutdown.
//
// idleTimeout mirrors cache-ctl (120s silent-connection retirement);
// rpcTimeout is 0 (no per-request deadline — the backend's own timeouts
// and the client govern), so no config beyond cache_listen is added.
func startCacheWireServer(cfg *Config, backend server.Backend) (func(), error) {
	tier := storeTier{cache.NewStoreOrigin(backend)}
	handler := cacheserver.NewCacheHandler(tier, nil) // shard=nil: the store has no shard tier
	ws := cacheserver.NewWireServer(handler, 120*time.Second, 0)

	lis, err := util.Listen(cfg.CacheListen)
	if err != nil {
		return nil, fmt.Errorf("cache listen %s: %w", cfg.CacheListen, err)
	}
	fmt.Fprintf(os.Stderr, "store-ctl cache wire server listen=%s (read-only)\n", cfg.CacheListen)
	go ws.Serve(lis)
	return ws.GracefulStop, nil
}
