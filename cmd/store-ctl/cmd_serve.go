package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	"github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

// cmdServe starts the gRPC store daemon and blocks until the
// process receives SIGINT or SIGTERM, at which point it does a
// graceful gRPC shutdown.
//
// The configured generation source must already contain a valid non-empty
// oldest-to-newest list. Object backends do not own that list.
func cmdServe(args []string) {
	fset := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	fset.Parse(args)

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	cfg, err := LoadConfig(resolved, true)
	if err != nil {
		fatal("%v", err)
	}

	backend, err := serveBackend(cfg)
	if err != nil {
		fatal("%v", err)
	}
	source, err := openGenerationSource(context.Background(), cfg, resolved)
	if err != nil {
		fatal("open generation source: %v", err)
	}
	generationManager, err := newGenerationManager(context.Background(), source)
	if err != nil {
		fatal("load generation source: %v", err)
	}
	// Register SIGHUP before publishing either listener. A reload sent during
	// startup is then buffered rather than being lost to the default action.
	refreshCtx, refreshCancel := context.WithCancel(context.Background())
	defer refreshCancel()
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	go generationManager.Run(refreshCtx, hupCh, log.Printf)

	srv, err := server.New(server.Options{
		Backend:     backend,
		Generations: generationManager.Current,
		VerifyKey:   cfg.VerifyKey(),
	})
	if err != nil {
		fatal("server.New: %v", err)
	}

	gs := grpc.NewServer()
	pb.RegisterStoreServer(gs, srv)

	lis, err := util.Listen(cfg.Listen)
	if err != nil {
		fatal("listen %s: %v", cfg.Listen, err)
	}

	generations := generationManager.Current()
	fmt.Fprintf(os.Stderr, "store-ctl serve listen=%s backend=%s generation=%s verify=%t direct_io=%t\n",
		cfg.Listen, cfg.Backend, generations[len(generations)-1], cfg.VerifyKey(), cfg.Backend == "fs" && cfg.FS.DirectIO)

	errCh := make(chan error, 1)
	go func() { errCh <- gs.Serve(lis) }()

	// Optional embedded read-only cache wire server (cache_listen): serves
	// cache-protocol reads straight from the backend so cache clients can
	// reach store content without a separate cache-ctl. Disabled when empty.
	var stopCache func()
	if cfg.CacheListen != "" {
		stopCache, err = startCacheWireServer(cfg, srv)
		if err != nil {
			fatal("%v", err)
		}
	}

	// Periodic adaptive stats line (stderr). Silent in windows with no traffic;
	// stopped on shutdown via statsCancel. stats_interval=0/off disables it.
	statsCtx, statsCancel := context.WithCancel(context.Background())
	defer statsCancel()
	go obstat.RunAdaptive(statsCtx, cfg.StatsIntervalDur(), storeSampler(srv), log.Printf)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		fmt.Fprintf(os.Stderr, "store-ctl: received %s, shutting down...\n", sig)
		gs.GracefulStop()
		if stopCache != nil {
			stopCache()
		}
	case err := <-errCh:
		if err != nil {
			fatal("gRPC serve: %v", err)
		}
	}
}

// serveBackend dispatches on cfg.Backend. Returns the abstract
// server.Backend so the caller doesn't need to import every backend
// package directly. Error messages keep the per-backend context so
// misconfiguration (missing fs.root, bad s3 endpoint, etc.) lands
// with a clear hint.
func serveBackend(cfg *Config) (server.Backend, error) {
	switch cfg.Backend {
	case "fs":
		return openFSStore(cfg)
	case "s3":
		return openS3Store(context.Background(), cfg)
	default:
		return nil, fmt.Errorf("unknown backend %q", cfg.Backend)
	}
}
