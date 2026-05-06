package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// cmdRollout adds a new generation to the store and makes it active.
// The previous active generation stays in the list (chunks/manifests
// remain readable via the reverse-search Get path) until a future
// `purge --generation` removes it.
//
// Symmetric across backends:
//
//	fs:  rewrites <root>/__meta/generations atomically
//	obs: CAS-rewrites <prefix>/__meta/generations with If-Match
func cmdRollout(args []string) {
	fset := flag.NewFlagSet("rollout", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (required)")
	generation := fset.String("generation", "", "new generation name to add and activate (required)")
	fset.Parse(args)

	if *configPath == "" {
		fatal("--config is required")
	}
	if *generation == "" {
		fatal("--generation is required (e.g. --generation G2)")
	}
	cfg, err := LoadConfig(*configPath, false)
	if err != nil {
		fatal("%v", err)
	}

	store, err := openAdminStore(cfg)
	if err != nil {
		fatal("%v", err)
	}
	if err := store.Rollout(context.Background(), *generation); err != nil {
		fatal("rollout: %v", err)
	}
	fmt.Fprintf(os.Stderr, "rollout OK: backend=%s generation=%s (now active)\n",
		cfg.Backend, *generation)
}

// openAdminStore opens an existing store of either backend as an
// adminStore. Used by rollout / purge / info — anything that needs
// to call Rollout/Drop/Wipe/Generations.
func openAdminStore(cfg *Config) (adminStore, error) {
	switch cfg.Backend {
	case "fs":
		return openFSStore(cfg)
	case "obs":
		return openOBSStore(context.Background(), cfg)
	default:
		return nil, fmt.Errorf("unknown backend %q", cfg.Backend)
	}
}
