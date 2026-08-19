package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// cmdInfo opens the store described by the config in read-only mode
// and prints a short report: backend, location, current write generation,
// the full generations list, and per-generation chunk/manifest object
// counts.
//
// Works on both backends — the per-partition walk happens through
// the admin GenerationStats method, which fs implements with
// filepath.WalkDir and s3 implements with paginated List.
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
	ctx := context.Background()
	source, err := openGenerationSource(ctx, cfg, resolved)
	if err != nil {
		fatal("open generation source: %v", err)
	}
	generations, err := source.source.Load(ctx)
	if err != nil {
		fatal("load generations: %v", err)
	}
	data, err := openAdminStore(cfg)
	if err != nil {
		fatal("open object backend: %v", err)
	}

	fmt.Printf("Backend: %s\n", cfg.Backend)
	switch cfg.Backend {
	case "fs":
		fmt.Printf("Root:    %s\n", cfg.FS.Root)
	case "s3":
		fmt.Printf("Bucket:  %s\n", cfg.S3.Bucket)
		fmt.Printf("Prefix:  %s\n", cfg.S3.Prefix)
	}
	fmt.Printf("Write:   %s\n", generations[len(generations)-1])
	fmt.Printf("Generations (oldest first): %v\n\n", generations)

	for _, generation := range generations {
		stats, err := data.GenerationStats(ctx, generation)
		if err != nil {
			fmt.Printf("  %s: error: %v\n", generation, err)
			continue
		}
		fmt.Printf("  %s: chunk=%d manifest=%d blob=%d\n",
			generation, stats[store.PartitionChunk], stats[store.PartitionManifest], stats[store.PartitionBlob])
	}
}
