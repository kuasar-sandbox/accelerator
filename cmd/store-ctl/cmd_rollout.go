package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// cmdRollout appends a new generation to a writable source, making it the
// write-admission generation. Previous generations stay in the list (objects
// remain readable via the reverse-search Get path) until a future
// `purge --generation` removes it.
//
// Default-source compatibility:
//
//	fs:  rewrites <root>/__meta/generations atomically
//	s3:  CAS-rewrites <prefix>/__meta/generations with If-Match
func cmdRollout(args []string) {
	fset := flag.NewFlagSet("rollout", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	generation := fset.String("generation", "", "new generation name to add and activate (required)")
	fset.Parse(args)

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	if *generation == "" {
		fatal("--generation is required (e.g. --generation G2)")
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
	newGeneration := store.Generation(*generation)
	if err := store.ValidateGeneration(newGeneration); err != nil {
		fatal("rollout: %v", err)
	}
	if err := source.mutate(ctx, func(current []store.Generation) ([]store.Generation, error) {
		return appendGeneration(current, newGeneration)
	}); err != nil {
		fatal("rollout: %v", err)
	}
	fmt.Fprintf(os.Stderr, "rollout OK: backend=%s generation=%s (now receives write admission)\n",
		cfg.Backend, *generation)
}

// openAdminStore opens the explicit-generation object admin helper.
func openAdminStore(cfg *Config) (adminStore, error) {
	switch cfg.Backend {
	case "fs":
		return openFSStore(cfg)
	case "s3":
		return openS3Store(context.Background(), cfg)
	default:
		return nil, fmt.Errorf("unknown backend %q", cfg.Backend)
	}
}
