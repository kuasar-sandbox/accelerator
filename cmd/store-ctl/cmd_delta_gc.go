package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// cmdDeltaGC audits one generation's delta-compressed chunk objects and, with
// --delete, removes the orphans. Basically a garbage collector for delta objects.
//
// Why there can be orphans at all ? A delta object is referenced by a manifest
// only if the generation's alias for that chunk names it: the store server
// creates the alias when the object is stored and deletes the object again if
// another writer's alias won the race. What that leaves behind is the failure
// path — the object was stored but the alias step itself failed, so the writer
// recorded the chunk independently (the client counts these as
// delta_orphan_bytes) — and objects written by servers older than the discard.
// Both are delta objects no alias names, and nothing can reference them.
//
// Why --older-than ? A writer that has just stored a delta object has not yet
// created its alias; deleting the object in that window would strand a
// manifest that is about to be written. The server's window is one alias
// request wide, so the default grace of one hour is very conservative. With a
// backend that does not report modification times the age is unknown and such
// orphans are reported but never deleted.
//
// Aliases themselves are never deleted: they are write-once records whose
// absence would let two writers disagree about a chunk's representation. They
// go away with their generation (`purge --generation`). An alias naming an
// object the generation does not hold ("dangling") is reported; it is harmless
// because a writer that resolves to it cannot adopt it and stores the chunk
// independently, which supersedes the alias.
//
// Usage: store-ctl delta-gc --config FILE --generation G ; 
// 	       --delete removes orphans older than --older-than (default: report only)
// 	       --list prints one line per orphan: <hex> <bytes> <age>
//
// Example:
//	store-ctl delta-gc --config config.yaml --generation G1 --delete
//
// This will audit the generation G1 and remove the orphans older than one hour.
//
func cmdDeltaGC(args []string) {
	fset := flag.NewFlagSet("delta-gc", flag.ExitOnError)
	configPath := fset.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	generation := fset.String("generation", "", "generation to audit")
	doDelete := fset.Bool("delete", false, "delete orphaned delta objects older than --older-than (default: report only)")
	olderThan := fset.Duration("older-than", time.Hour, "only delete orphans at least this old (Go duration)")
	list := fset.Bool("list", false, "print one line per orphan: <hex> <bytes> <age>")
	fset.Parse(args)

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	if *generation == "" {
		fatal("--generation is required")
	}
	target := store.Generation(*generation)
	if err := store.ValidateGeneration(target); err != nil {
		fatal("generation %s: %v", *generation, err)
	}
	if *olderThan < 0 {
		fatal("--older-than must not be negative")
	}
	cfg, err := LoadConfig(resolved, false)
	if err != nil {
		fatal("%v", err)
	}
	ctx := context.Background()
	data, err := openAdminStore(cfg)
	if err != nil {
		fatal("open object backend: %v", err)
	}

	audit, err := data.AuditDeltas(ctx, target)
	if err != nil {
		fatal("audit generation %s: %v", target, err)
	}
	now := time.Now()
	fmt.Printf("generation:     %s\n", target)
	fmt.Printf("chunk objects:  %d\n", audit.Chunks)
	fmt.Printf("delta objects:  %d (%s)\n", audit.DeltaObjects, formatBytes(audit.DeltaBytes))
	fmt.Printf("aliases:        %d naming %d distinct delta objects, %d dangling\n", audit.Aliases, audit.Named, audit.Dangling)
	fmt.Printf("orphans:        %d (%s)\n", len(audit.Orphans), formatBytes(audit.OrphanBytes))

	var eligible, young, unknownAge int
	var eligibleBytes uint64
	for _, o := range audit.Orphans {
		age := "unknown age"
		switch {
		case o.ModTime.IsZero():
			unknownAge++
		case now.Sub(o.ModTime) < *olderThan:
			young++
			age = now.Sub(o.ModTime).Truncate(time.Second).String()
		default:
			eligible++
			eligibleBytes += o.Size
			age = now.Sub(o.ModTime).Truncate(time.Second).String()
		}
		if *list {
			fmt.Printf("  %x %d %s\n", o.Key, o.Size, age)
		}
	}
	fmt.Printf("eligible:       %d (%s) older than %s; %d younger; %d of unknown age\n",
		eligible, formatBytes(eligibleBytes), *olderThan, young, unknownAge)

	if !*doDelete {
		if eligible > 0 {
			fmt.Fprintf(os.Stderr, "delta-gc: report only; re-run with --delete to remove %d orphaned delta objects\n", eligible)
		}
		return
	}
	deleted, failed := 0, 0
	var deletedBytes uint64
	for _, o := range audit.Orphans {
		if o.ModTime.IsZero() || now.Sub(o.ModTime) < *olderThan {
			continue
		}
		if err := data.Delete(ctx, target, store.PartitionChunk, o.Key); err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "delta-gc: delete %x: %v\n", o.Key, err)
			continue
		}
		deleted++
		deletedBytes += o.Size
	}
	fmt.Fprintf(os.Stderr, "delta-gc OK: backend=%s generation=%s deleted %d orphaned delta objects (%s), %d failed\n",
		cfg.Backend, target, deleted, formatBytes(deletedBytes), failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
