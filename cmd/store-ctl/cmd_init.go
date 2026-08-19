package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// cmdInit creates a writable generation source with --generation as its only
// (and therefore write-admission) entry. It refuses to overwrite an existing
// source. A config source is read-only and must be edited in the main YAML.
//
// Default-source compatibility:
//
//	fs:  writes <root>/__meta/generations
//	s3:  PUTs <prefix>/__meta/generations with If-None-Match: *
func cmdInit(args []string) {
	fset := flag.NewFlagSet("init", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	generation := fset.String("generation", "", "starting generation name (required, e.g. G1)")
	fset.Parse(args)

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	if *generation == "" {
		fatal("--generation is required (e.g. --generation G1)")
	}
	cfg, err := LoadConfig(resolved, false)
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	source, err := openGenerationSource(ctx, cfg, resolved)
	if err != nil {
		fatal("open generation source: %v", err)
	}
	if source.readOnly() {
		fatal("init generation source: config source is read-only; update the main YAML")
	}
	if cfg.Backend == "fs" {
		if err := os.MkdirAll(cfg.FS.Root, 0o755); err != nil {
			fatal("create fs root: %v", err)
		}
	}
	if err := source.initialise(ctx, store.Generation(*generation)); err != nil {
		fatal("init generation source: %v", err)
	}
	fmt.Fprintf(os.Stderr, "init OK: backend=%s generation=%s\n", cfg.Backend, *generation)
}
