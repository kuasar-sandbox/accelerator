package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
)

// cmdInfo opens the store described by the config in read-only mode
// and prints a short report: backend, location, active generation,
// the full generations list, and per-generation chunk/manifest object
// counts.
//
// Works on both backends — the per-partition walk happens through
// the admin GenerationStats method, which fs implements with
// filepath.WalkDir and obs implements with paginated List.
func cmdInfo(args []string) {
	fset := flag.NewFlagSet("info", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	fset.Parse(args)

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	cfg, err := LoadConfig(resolved, false)
	if err != nil {
		fatal("%v", err)
	}
	s, err := openAdminStore(cfg)
	if err != nil {
		fatal("%v", err)
	}

	fmt.Printf("Backend: %s\n", cfg.Backend)
	switch cfg.Backend {
	case "fs":
		fmt.Printf("Root:    %s\n", cfg.FS.Root)
	case "obs":
		fmt.Printf("Bucket:  %s\n", cfg.OBS.Bucket)
		fmt.Printf("Prefix:  %s\n", cfg.OBS.Prefix)
	}
	fmt.Printf("Active:  %s\n", s.ActiveGeneration())
	gens := s.Generations()
	fmt.Printf("Generations (newest first): %v\n\n", gens)

	ctx := context.Background()
	for _, gen := range gens {
		stats, err := s.GenerationStats(ctx, gen)
		if err != nil {
			fmt.Printf("  %s: error: %v\n", gen, err)
			continue
		}
		fmt.Printf("  %s: chunk=%d manifest=%d blob=%d\n",
			gen, stats[store.PartitionChunk], stats[store.PartitionManifest], stats[store.PartitionBlob])
	}
}
