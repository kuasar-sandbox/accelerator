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

	switch cfg.Backend {
	case "fs":
		if err := initFSStore(cfg, *generation); err != nil {
			fatal("init fs: %v", err)
		}
		fmt.Fprintf(os.Stderr, "init OK: backend=fs root=%s generation=%s\n",
			cfg.FS.Root, *generation)
	case "s3":
		if err := initS3Store(context.Background(), cfg, *generation); err != nil {
			fatal("init s3: %v", err)
		}
		fmt.Fprintf(os.Stderr, "init OK: backend=s3 bucket=%s prefix=%s generation=%s\n",
			cfg.S3.Bucket, cfg.S3.Prefix, *generation)
	default:
		fatal("unknown backend %q", cfg.Backend)
	}
}
