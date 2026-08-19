package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// cmdPurge has two modes:
//
//	--generation G   delete a named non-write generation: remove it
//	                 from the source list first, then remove every chunk +
//	                 manifest object under it.
//	--all            wipe all object data and, when writable, remove
//	                 the source list. Afterwards the store is uninitialised;
//	                 a subsequent `init` is required before serve.
//
// Exactly one of the two flags is required. `--all` requires
// `--confirm` because there is no recovery; `--generation` does not
// (the source mutation refuses to remove the final write generation).
func cmdPurge(args []string) {
	fset := flag.NewFlagSet("purge", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	generation := fset.String("generation", "", "older (non-write) generation to drop")
	all := fset.Bool("all", false, "wipe the entire store (requires --confirm)")
	confirm := fset.Bool("confirm", false, "required for --all (proves intent)")
	fset.Parse(args)

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	switch {
	case *all && *generation != "":
		fatal("--all and --generation are mutually exclusive")
	case !*all && *generation == "":
		fatal("one of --generation or --all is required")
	case *all && !*confirm:
		fatal("--all requires --confirm (no recovery after wipe)")
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
	data, err := openAdminStore(cfg)
	if err != nil {
		fatal("open object backend: %v", err)
	}
	if *all {
		if !source.readOnly() {
			if err := source.remove(ctx); err != nil {
				fatal("remove generation source: %v", err)
			}
		}
		if err := data.Wipe(ctx); err != nil {
			fatal("wipe: %v", err)
		}
		if source.readOnly() {
			fmt.Fprintf(os.Stderr, "purge OK: backend=%s object data wiped; config generation source unchanged\n", cfg.Backend)
		} else {
			fmt.Fprintf(os.Stderr, "purge OK: backend=%s object data and generation source removed\n", cfg.Backend)
		}
		return
	}

	target := store.Generation(*generation)
	if err := store.ValidateGeneration(target); err != nil {
		fatal("drop %s: %v", *generation, err)
	}
	if err := source.mutate(ctx, func(current []store.Generation) ([]store.Generation, error) {
		return removeGeneration(current, target)
	}); err != nil {
		fatal("remove generation %s from source: %v", target, err)
	}
	if err := data.DropGeneration(ctx, target); err != nil {
		fatal("generation %s was removed from source but object cleanup failed: %v", target, err)
	}
	fmt.Fprintf(os.Stderr, "purge OK: backend=%s generation=%s dropped\n",
		cfg.Backend, *generation)
}
