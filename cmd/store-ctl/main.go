// store-ctl is the daemon-and-admin entry point for the gRPC store
// service. It owns a filesystem or object-storage backend and is the
// only process that reads or writes its root / bucket prefix.
// manifest-ctl and cache-ctl reach the store exclusively through the
// gRPC client.
//
// Subcommands:
//
//	store-ctl serve   --config FILE                  start the gRPC daemon
//	store-ctl init    --config FILE --generation G   initialise an empty store
//	store-ctl rollout --config FILE --generation G   add a new active generation
//	store-ctl purge   --config FILE --generation G   delete one inactive generation
//	store-ctl purge   --config FILE --all            wipe the entire store
//	store-ctl info    --config FILE                  show generations + object counts
//
// `init` is the only subcommand that creates state; everything else
// requires an already-initialised store and surfaces a clear
// `run store-ctl init` hint when it isn't.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "serve":
		cmdServe(os.Args[2:])
	case "init":
		cmdInit(os.Args[2:])
	case "rollout":
		cmdRollout(os.Args[2:])
	case "purge":
		cmdPurge(os.Args[2:])
	case "info":
		cmdInfo(os.Args[2:])
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "store-ctl: unknown command %q\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: store-ctl <command> [args]")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  serve     Start the gRPC store daemon")
	fmt.Fprintln(os.Stderr, "  init      Initialise an empty store with a starting generation")
	fmt.Fprintln(os.Stderr, "  rollout   Add a new generation and make it active")
	fmt.Fprintln(os.Stderr, "  purge     Delete a non-active generation, or --all to wipe the store")
	fmt.Fprintln(os.Stderr, "  info      Show generations and per-partition object counts")
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "store-ctl: "+format+"\n", args...)
	os.Exit(1)
}
