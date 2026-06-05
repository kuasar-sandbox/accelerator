package main

import (
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// cmdConfig dispatches `store-ctl config <subcommand>`.
//
//	show      Print the resolved YAML at the configured path.
//	generate  Print a commented daemon-config template to stdout.
func cmdConfig(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: store-ctl config <show|generate> [flags]")
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
	configPath := fs.String("config", "", "YAML config file (overrides STORE_CONFIG env)")
	fs.Parse(args)

	resolved := resolveConfigPath(*configPath)
	if resolved == "" {
		fatal("--config or %s required", storeConfigEnv)
	}
	cfg, err := LoadConfig(resolved, false)
	if err != nil {
		fatal("%v", err)
	}
	out, err := yaml.Marshal(cfg)
	if err != nil {
		fatal("marshal config: %v", err)
	}
	os.Stdout.Write(out)
}

func cmdConfigGenerate(args []string) {
	fs := flag.NewFlagSet("config generate", flag.ExitOnError)
	fs.Parse(args)
	fmt.Print(storeConfigTemplate)
}

// storeConfigTemplate is a commented YAML for `store-ctl config generate`.
// Defaults to the fs backend (simplest local deploy); operators using
// obs swap the backend block.
const storeConfigTemplate = `# store-ctl daemon configuration.
# Reference this file via --config or the STORE_CONFIG environment
# variable. There is no auto-discovery; unset = error.

# gRPC listen address (host:port).
listen: 127.0.0.1:7100

# Backend: fs | obs.
backend: fs

# Filesystem backend (when backend: fs).
fs:
  root: /var/lib/store-ctl
  # Refuse to PUT a chunk whose declared content key doesn't match its
  # bytes. Cheap insurance against client bugs; off only for benchmarks.
  verify_content_key: true

# Object-storage backend (when backend: obs). Unused under backend: fs.
# obs:
#   endpoint: obs.example.com
#   region: cn-north-4
#   bucket: my-store
#   prefix: store/
#   access_key: AKIA...
#   secret_key: ...
`
