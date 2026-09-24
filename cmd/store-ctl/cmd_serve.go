package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	appstore "github.com/kuasar-sandbox/accelerator/app/store"
)

// cmdServe owns CLI configuration, process signals, and exit behavior. The
// application package owns the complete Store serving lifecycle.
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

	reload := make(chan struct{}, 1)
	opts := appstore.Options{Reload: reload}
	if cfg.Generations.IsConfigSource() {
		source := configGenerationSource{path: resolved}
		opts.Generations = &appstore.GenerationSource{Load: source.Load}
	}

	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	done := make(chan struct{})
	var signalWG sync.WaitGroup
	signalWG.Add(1)
	go func() {
		defer signalWG.Done()
		for {
			select {
			case <-done:
				return
			case sig := <-signals:
				if sig == syscall.SIGHUP {
					select {
					case reload <- struct{}{}:
					default:
					}
					continue
				}
				fmt.Fprintf(os.Stderr, "store-ctl: received %s, shutting down...\n", sig)
				cancel()
			}
		}
	}()

	err = appstore.Run(ctx, *cfg, opts)
	signal.Stop(signals)
	close(done)
	cancel()
	signalWG.Wait()
	if err != nil {
		fatal("serve: %v", err)
	}
}
