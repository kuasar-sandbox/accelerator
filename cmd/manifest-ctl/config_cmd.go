package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/fullof-work/mass-sandbox/pkg/config"
	"gopkg.in/yaml.v3"
)

// cmdConfig dispatches `manifest-ctl config <subcommand>`.
//
// Subcommands:
//
//	show      Print the resolved YAML at the configured path.
//	generate  Print a commented template to stdout.
//
// `show` requires --config or MANIFEST_CONFIG; `generate` does not.
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
	configPath := fs.String("config", "", "path to manifest config YAML (overrides MANIFEST_CONFIG env)")
	fs.Parse(args)

	cfg, err := loadConfig(*configPath)
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
	fmt.Print(config.DefaultYAMLString())
}
