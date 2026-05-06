package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// cmdInit creates a fresh store at the configured location with
// `--generation` as the single (and active) generation. Refuses to
// clobber an already-initialised store; recovery is `purge --all`
// followed by a fresh init.
//
// Symmetric across backends:
//
//	fs:  writes <root>/__meta/generations
//	obs: PUTs <prefix>/__meta/generations with If-None-Match: *
func cmdInit(args []string) {
	fset := flag.NewFlagSet("init", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (required)")
	generation := fset.String("generation", "", "starting generation name (required, e.g. G1)")
	fset.Parse(args)

	if *configPath == "" {
		fatal("--config is required")
	}
	if *generation == "" {
		fatal("--generation is required (e.g. --generation G1)")
	}
	cfg, err := LoadConfig(*configPath, false)
	if err != nil {
		fatal("%v", err)
	}

	switch cfg.Backend {
	case "fs":
		if err := initFSStore(cfg, *generation); err != nil {
			fatal("init fs: %v", err)
		}
		fmt.Fprintf(os.Stderr, "init OK: backend=fs root=%s generation=%s\n",
			cfg.FS.Root, *generation)
	case "obs":
		if err := initOBSStore(context.Background(), cfg, *generation); err != nil {
			fatal("init obs: %v", err)
		}
		fmt.Fprintf(os.Stderr, "init OK: backend=obs bucket=%s prefix=%s generation=%s\n",
			cfg.OBS.Bucket, cfg.OBS.Prefix, *generation)
	default:
		fatal("unknown backend %q", cfg.Backend)
	}
}
