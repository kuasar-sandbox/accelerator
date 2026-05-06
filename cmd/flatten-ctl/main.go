package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/fullof-work/mass-sandbox/pkg/flatten"
)

func main() {
	// Subcommand dispatch: `info` is an explicit subcommand; the
	// flag-based default ("flatten this image") stays for back-compat
	// with existing scripts that call `flatten-ctl --image x --output y`.
	if len(os.Args) >= 2 && os.Args[1] == "info" {
		runInfo(os.Args[2:])
		return
	}

	image := flag.String("image", "-", "OCI image reference (default stdin tar stream)")
	output := flag.String("output", "-", "EROFS output path (default stdout)")
	verify := flag.Bool("verify", false, "Flatten twice and verify determinism")
	noProgress := flag.Bool("no-progress", false, "Disable progress output")
	flag.Parse()

	if *verify {
		runVerify(*image, *noProgress)
		return
	}

	runFlatten(*image, *output, *noProgress)
}

// runInfo implements `flatten-ctl info <file> [--json]`. It reads the
// EROFS superblock for image size and the trailing ZIP for the OCI
// runtime config; in --json mode it emits a single JSON object that
// orchestrators can consume programmatically.
func runInfo(args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "machine-readable JSON output")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: flatten-ctl info [--json] <file.erofs>")
	}
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}
	path := fs.Arg(0)

	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: open %s: %v\n", path, err)
		os.Exit(1)
	}
	defer f.Close()

	erofsSize, sbErr := flatten.ReadEROFSSize(f)
	if sbErr != nil {
		fmt.Fprintf(os.Stderr, "Error: read EROFS superblock: %v\n", sbErr)
		os.Exit(1)
	}

	cfg, cfgErr := flatten.ReadConfigFromFile(path)
	// Tolerate "no config zip appended" in human mode (older outputs);
	// in --json mode emit `"config": null` so consumers can detect it.
	if cfgErr != nil && !errors.Is(cfgErr, fsErrNotExist()) {
		fmt.Fprintf(os.Stderr, "Error: read config: %v\n", cfgErr)
		os.Exit(1)
	}

	if *asJSON {
		out := struct {
			ErofsSize uint64                  `json:"erofs_size"`
			Config    *flatten.RuntimeConfig  `json:"config"`
		}{ErofsSize: erofsSize, Config: cfg}
		body, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: marshal: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}

	printInfoHuman(erofsSize, cfg)
}

// fsErrNotExist returns fs.ErrNotExist as a value comparable to
// errors.Is targets returned by zip_append.go. Wrapped in a func so
// the import shadow on `fs` (the FlagSet) inside runInfo is moot.
func fsErrNotExist() error { return fs.ErrNotExist }

// printInfoHuman renders the inspect output for human consumption.
// Empty-valued config fields are skipped to keep the report tight.
func printInfoHuman(erofsSize uint64, cfg *flatten.RuntimeConfig) {
	fmt.Printf("EROFS image size:  %s (%d bytes)\n", formatSize(int64(erofsSize)), erofsSize)
	if cfg == nil {
		fmt.Println("(no OCI config trailer)")
		return
	}
	fmt.Printf("Architecture:      %s\n", emptyDash(cfg.Architecture))
	fmt.Printf("Os:                %s\n", emptyDash(cfg.Os))
	if cfg.User != "" {
		fmt.Printf("User:              %s\n", cfg.User)
	}
	if len(cfg.Entrypoint) > 0 {
		fmt.Printf("Entrypoint:        %s\n", jsonInline(cfg.Entrypoint))
	}
	if len(cfg.Cmd) > 0 {
		fmt.Printf("Cmd:               %s\n", jsonInline(cfg.Cmd))
	}
	if cfg.WorkingDir != "" {
		fmt.Printf("WorkingDir:        %s\n", cfg.WorkingDir)
	}
	if len(cfg.Env) > 0 {
		fmt.Printf("Env (%d):\n", len(cfg.Env))
		for _, e := range cfg.Env {
			fmt.Printf("  %s\n", e)
		}
	}
	if len(cfg.ExposedPorts) > 0 {
		fmt.Printf("ExposedPorts:      %s\n", strings.Join(sortedKeys(cfg.ExposedPorts), ", "))
	}
	if len(cfg.Volumes) > 0 {
		fmt.Printf("Volumes:           %s\n", strings.Join(sortedKeys(cfg.Volumes), ", "))
	}
	if cfg.StopSignal != "" {
		fmt.Printf("StopSignal:        %s\n", cfg.StopSignal)
	}
	if len(cfg.Labels) > 0 {
		fmt.Println("Labels:")
		keys := make([]string, 0, len(cfg.Labels))
		for k := range cfg.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("  %s=%s\n", k, cfg.Labels[k])
		}
	}
	if cfg.Healthcheck != nil {
		fmt.Printf("Healthcheck:       Test=%s Interval=%dns Retries=%d\n",
			jsonInline(cfg.Healthcheck.Test), cfg.Healthcheck.Interval, cfg.Healthcheck.Retries)
	}
}

func emptyDash(s string) string {
	if s == "" {
		return "(unset)"
	}
	return s
}

func jsonInline(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func runFlatten(image, output string, noProgress bool) {
	// Determine input source.
	var inputDesc string
	switch {
	case image == "-":
		inputDesc = "stdin (docker-archive)"
	case strings.HasPrefix(image, "oci:"):
		fmt.Fprintln(os.Stderr, "OCI layout not yet supported, use docker-archive format")
		os.Exit(1)
	case strings.HasPrefix(image, "docker-archive:"):
		image = strings.TrimPrefix(image, "docker-archive:")
		inputDesc = image + " (docker-archive)"
	default:
		inputDesc = image + " (docker-archive)"
	}

	// Determine output destination.
	if output == "-" {
		// Flatten to a temp file, then copy to stdout.
		tmp, err := os.CreateTemp("", "flatten-out-*.img")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: create temp file: %v\n", err)
			os.Exit(1)
		}
		tmpPath := tmp.Name()
		tmp.Close()
		defer os.Remove(tmpPath)

		if err := flattenTo(image, tmpPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		f, err := os.Open(tmpPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: open temp output: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: stat output: %v\n", err)
			os.Exit(1)
		}

		if !noProgress {
			fmt.Fprintf(os.Stderr, "Input:  %s\n", inputDesc)
			fmt.Fprintf(os.Stderr, "Output: stdout (%s)\n", formatSize(info.Size()))
		}

		if _, err := io.Copy(os.Stdout, f); err != nil {
			fmt.Fprintf(os.Stderr, "Error: write to stdout: %v\n", err)
			os.Exit(1)
		}
	} else {
		if err := flattenTo(image, output); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if !noProgress {
			info, err := os.Stat(output)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: stat output: %v\n", err)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "Input:  %s\n", inputDesc)
			fmt.Fprintf(os.Stderr, "Output: %s (%s)\n", output, formatSize(info.Size()))
		}
	}
}

// flattenTo dispatches to FlattenFile or Flatten depending on whether input is
// a file path or stdin.
func flattenTo(image, outputPath string) error {
	if image == "-" {
		return flatten.Flatten(os.Stdin, outputPath)
	}
	return flatten.FlattenFile(image, outputPath)
}

func runVerify(image string, noProgress bool) {
	// We need a seekable input for Verify. If stdin, save to temp file first.
	var inputPath string
	if image == "-" {
		tmp, err := os.CreateTemp("", "verify-input-*.tar")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: create temp file: %v\n", err)
			os.Exit(1)
		}
		defer os.Remove(tmp.Name())
		if _, err := io.Copy(tmp, os.Stdin); err != nil {
			tmp.Close()
			fmt.Fprintf(os.Stderr, "Error: read stdin: %v\n", err)
			os.Exit(1)
		}
		tmp.Close()
		inputPath = tmp.Name()
	} else if strings.HasPrefix(image, "docker-archive:") {
		inputPath = strings.TrimPrefix(image, "docker-archive:")
	} else {
		inputPath = image
	}

	// Flatten twice to temp files.
	tmp1, err := os.CreateTemp("", "verify-pass1-*.img")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: create temp file: %v\n", err)
		os.Exit(1)
	}
	tmp1Path := tmp1.Name()
	tmp1.Close()
	defer os.Remove(tmp1Path)

	tmp2, err := os.CreateTemp("", "verify-pass2-*.img")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: create temp file: %v\n", err)
		os.Exit(1)
	}
	tmp2Path := tmp2.Name()
	tmp2.Close()
	defer os.Remove(tmp2Path)

	if err := flatten.FlattenFile(inputPath, tmp1Path); err != nil {
		fmt.Fprintf(os.Stderr, "Error: flatten pass 1: %v\n", err)
		os.Exit(1)
	}

	if err := flatten.FlattenFile(inputPath, tmp2Path); err != nil {
		fmt.Fprintf(os.Stderr, "Error: flatten pass 2: %v\n", err)
		os.Exit(1)
	}

	hash1, size1, err := hashAndSize(tmp1Path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: hash pass 1: %v\n", err)
		os.Exit(1)
	}

	hash2, size2, err := hashAndSize(tmp2Path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: hash pass 2: %v\n", err)
		os.Exit(1)
	}

	if !noProgress {
		fmt.Fprintf(os.Stderr, "Pass 1: sha256:%s (%s)\n", hash1, formatSize(size1))
		fmt.Fprintf(os.Stderr, "Pass 2: sha256:%s (%s)\n", hash2, formatSize(size2))
	}

	if hash1 == hash2 {
		fmt.Fprintln(os.Stderr, "DETERMINISTIC")
	} else {
		fmt.Fprintln(os.Stderr, "NOT DETERMINISTIC")
		os.Exit(1)
	}
}

func hashAndSize(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", 0, err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), info.Size(), nil
}

func formatSize(bytes int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case bytes >= gib:
		return fmt.Sprintf("%.1f GiB", float64(bytes)/float64(gib))
	case bytes >= mib:
		return fmt.Sprintf("%.1f MiB", float64(bytes)/float64(mib))
	case bytes >= kib:
		return fmt.Sprintf("%.1f KiB", float64(bytes)/float64(kib))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
