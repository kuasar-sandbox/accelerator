package main

import (
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// cmdConfig dispatches `manifest-ctl config <subcommand>`.
//
// Subcommands:
//
//	show      Print the resolved YAML at the configured path.
//	generate  Print a commented template to stdout.
//
// `show` requires --manifest-config or MANIFEST_CONFIG; `generate` does not.
func cmdConfig(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: manifest-ctl config <show|generate> [flags]")
		os.Exit(1)
	}
	switch args[0] {
	case "show":
		cmdConfigShow(args[1:])
	case "generate":
		cmdConfigGenerate(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown config subcommand %q\n", args[0])
		os.Exit(1)
	}
}

func cmdConfigShow(args []string) {
	fs := flag.NewFlagSet("config show", flag.ExitOnError)
	configPath := fs.String("manifest-config", "", "path to manifest config YAML (overrides MANIFEST_CONFIG env)")
	fs.Parse(args)

	cfg := loadCfg(*configPath)
	out, err := yaml.Marshal(cfg)
	if err != nil {
		fatal("marshal config: %v", err)
	}
	os.Stdout.Write(out)
}

func cmdConfigGenerate(args []string) {
	fs := flag.NewFlagSet("config generate", flag.ExitOnError)
	fs.Parse(args)
	fmt.Print(defaultYAMLTemplate)
}

// defaultYAMLTemplate is the commented-template YAML emitted by
// `manifest-ctl config generate`. Mirrors the schema in
// pkg/manifest.Config; placeholders are obvious so users notice and
// edit them.
const defaultYAMLTemplate = `# manifest-ctl / sandbox-ctl shared manifest configuration.
# Reference this file via --manifest-config or the MANIFEST_CONFIG
# environment variable. There is no search path; unset = error.

manifest:
  # 32-byte hex-encoded customer key (64 hex chars).
  # Generate with: openssl rand -hex 32
  # May be left empty here and supplied via the MANIFEST_KEY environment
  # variable instead (MANIFEST_KEY also overrides a value set here), so the
  # secret can stay out of this shared file.
  key: ""
  # Re-hash physical Manifest/Chunk objects during ordinary reads. Omitted
  # defaults to true. Explicit verify/import/upload operations always verify.
  verify_content: true
  # Optional generation for writes. With store.endpoint it must still be in
  # the Store's current list; without a Store, local Bundle creation derives
  # its canonical admission locally (empty defaults to the ordinary name NONE).
  # write_generation: G3

store:
  # store-ctl gRPC endpoint, host:port. Required for remote ingest/fetch;
  # may be empty for offline local Bundle creation.
  endpoint: "127.0.0.1:7100"
  # Independent gRPC ClientConns to multiplex over.
  pool: 4
  # Per-RPC wall-clock budget.
  timeout: 5s

cache:
  # cache-ctl wire endpoint, host:port. Empty = bypass cache and
  # talk to store directly (slower; only sensible for one-off ops).
  endpoint: "127.0.0.1:7070"
  pool: 4
  timeout: 2s

chunker:
  # Content-defined ("cdc") or "fixed" chunking.
  mode: cdc
  cdc:
    min: 128KiB
    avg: 512KiB
    max: 1MiB
  fixed:
    size: 512KiB

crypto:
  # AES-256-CTR convergent encryption ("aes") or HMAC + plaintext ("fake").
  # Production must use aes; fake is for performance baselining only.
  chunk: aes
  manifest: aes
  # Local immutable-file compatibility policy (off | auto | required).
  # This is not an algorithm selector. Omitted means off.
  local: off
`
