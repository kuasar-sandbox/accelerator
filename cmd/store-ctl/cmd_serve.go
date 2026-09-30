package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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

	term := make(chan os.Signal, 2)
	hup := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(hup, syscall.SIGHUP)
	err = serveStore(*cfg, resolved, term, hup, os.Stderr, appstore.Run)
	signal.Stop(hup)
	signal.Stop(term)
	if err != nil {
		fatal("serve: %v", err)
	}
}

type storeRunner func(context.Context, appstore.Config, appstore.Options) error

func serveStore(cfg Config, resolved string, term, hup <-chan os.Signal, output io.Writer, run storeRunner) error {
	reload := make(chan struct{}, 1)
	opts := appstore.Options{Reload: reload}
	if cfg.Generations.IsConfigSource() {
		source := configGenerationSource{path: resolved}
		opts.Generations = &appstore.GenerationSource{Load: source.Load}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var signalWG sync.WaitGroup
	signalWG.Add(1)
	go func() {
		defer signalWG.Done()
		for {
			select {
			case <-done:
				return
			case <-hup:
				select {
				case reload <- struct{}{}:
				default:
				}
			case sig := <-term:
				fmt.Fprintf(output, "store-ctl: received %s, shutting down...\n", sig)
				cancel()
			}
		}
	}()

	err := run(ctx, cfg, opts)
	close(done)
	cancel()
	signalWG.Wait()
	return err
}
