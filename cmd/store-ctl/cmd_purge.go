package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// cmdPurge has two modes:
//
//	--generation G   delete the named (non-active) generation: drop it
//	                 from the meta list and remove every chunk +
//	                 manifest object under it.
//	--all            wipe the entire store — every object, including
//	                 the meta. After --all the store is uninitialised;
//	                 a subsequent `init` is required before serve.
//
// Exactly one of the two flags is required. `--all` requires
// `--confirm` because there is no recovery; `--generation` does not
// (the underlying admin Drop refuses to remove the active generation
// so the foot-cannon surface is much smaller).
func cmdPurge(args []string) {
	fset := flag.NewFlagSet("purge", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (required)")
	generation := fset.String("generation", "", "non-active generation to drop")
	all := fset.Bool("all", false, "wipe the entire store (requires --confirm)")
	confirm := fset.Bool("confirm", false, "required for --all (proves intent)")
	fset.Parse(args)

	if *configPath == "" {
		fatal("--config is required")
	}
	switch {
	case *all && *generation != "":
		fatal("--all and --generation are mutually exclusive")
	case !*all && *generation == "":
		fatal("one of --generation or --all is required")
	case *all && !*confirm:
		fatal("--all requires --confirm (no recovery after wipe)")
	}

	cfg, err := LoadConfig(*configPath, false)
	if err != nil {
		fatal("%v", err)
	}
	store, err := openAdminStore(cfg)
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	if *all {
		if err := store.Wipe(ctx); err != nil {
			fatal("wipe: %v", err)
		}
		fmt.Fprintf(os.Stderr, "purge OK: backend=%s wiped (store now uninitialised)\n", cfg.Backend)
		return
	}

	if err := store.Drop(ctx, *generation); err != nil {
		fatal("drop %s: %v", *generation, err)
	}
	fmt.Fprintf(os.Stderr, "purge OK: backend=%s generation=%s dropped\n",
		cfg.Backend, *generation)
}
