// flatten-ctl converts OCI / docker-archive images into deterministic
// EROFS rootfs images with an appended OCI runtime-config ZIP.
//
// Subcommands:
//
//	flatten-ctl export   --input <path|->  --output <path|-> [--tmpdir D]
//	                     [--manifest-config <path>] [--upload]
//	flatten-ctl verify   --input <path|-> [--tmpdir D]
//	flatten-ctl info     --input <path|manifest://hex> [--json]
//	                     [--manifest-config <path>]
//
// `export --upload` ingests the produced EROFS into the content store
// and prints the resulting manifest key on stdout (the EROFS itself
// is discarded unless --output is also given).
//
// `info` with a manifest:// reference fetches the EROFS via cache-ctl
// + store-ctl, then reads its superblock + ZIP trailer.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/fullof-work/mass-sandbox/pkg/manifest"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/fetch"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/ingest"
)

const manifestConfigEnv = "MANIFEST_CONFIG"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "export":
		cmdExport(os.Args[2:])
	case "verify":
		cmdVerify(os.Args[2:])
	case "info":
		cmdInfo(os.Args[2:])
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: flatten-ctl <command> [flags]

Commands:
  export   Flatten OCI/docker-archive → EROFS (optionally upload).
  verify   Flatten twice, fail if outputs differ.
  info     Print EROFS metadata + OCI runtime config.

See `+"`flatten-ctl <command> -h`"+` for per-command flags.
`)
}

// ---------------------------------------------------------------------------
// export
// ---------------------------------------------------------------------------

func cmdExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	input := fs.String("input", "-", "docker-archive tar (- for stdin)")
	output := fs.String("output", "", "EROFS output path (- for stdout, empty = required with --upload off)")
	tmpDir := fs.String("tmpdir", "", "parent of the per-run scratch directory (default $TMPDIR or /tmp)")
	manifestCfg := fs.String("manifest-config", "", "manifest config YAML (overrides MANIFEST_CONFIG env)")
	upload := fs.Bool("upload", false, "after flatten, ingest the EROFS into the store and print the manifest key on stdout")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	fs.Parse(args)

	if !*upload && (*output == "" || *output == "/dev/null") {
		fatal("--output is required when --upload is not set")
	}

	// Resolve output path: when --output is "-" or empty (+upload), use
	// a temp file we can re-open after FlattenFile finishes.
	tmpOut := false
	outPath := *output
	if *upload || outPath == "-" {
		tf, err := os.CreateTemp(*tmpDir, "flatten-out-*.img")
		if err != nil {
			fatal("create temp output: %v", err)
		}
		tf.Close()
		outPath = tf.Name()
		tmpOut = true
		defer os.Remove(outPath)
	}

	if err := runFlatten(*input, outPath, *tmpDir); err != nil {
		fatal("%v", err)
	}

	info, err := os.Stat(outPath)
	if err != nil {
		fatal("stat output: %v", err)
	}
	if !*noProgress {
		fmt.Fprintf(os.Stderr, "EROFS image: %s\n", formatSize(info.Size()))
	}

	// Pipe to stdout if requested.
	if *output == "-" {
		f, err := os.Open(outPath)
		if err != nil {
			fatal("re-open temp output: %v", err)
		}
		if _, err := io.Copy(os.Stdout, f); err != nil {
			f.Close()
			fatal("write to stdout: %v", err)
		}
		f.Close()
	}

	if !*upload {
		return
	}

	cfg := loadManifestCfg(*manifestCfg)
	ing, err := cfg.NewIngester(cfg.IngestKeyFunc(), nil)
	if err != nil {
		fatal("ingester: %v", err)
	}
	defer ing.Close()

	in, err := os.Open(outPath)
	if err != nil {
		fatal("re-open temp output for upload: %v", err)
	}
	defer in.Close()

	res, err := ing.Ingest(context.Background(), in, uint64(info.Size()), ingest.IngestOption{})
	if err != nil {
		fatal("ingest: %v", err)
	}
	if !*noProgress {
		fmt.Fprintf(os.Stderr, "stored: %s (chunks stored=%d dedup=%d zero=%d)\n",
			formatSize(int64(res.StoredBytes)), res.StoredChunks, res.DedupChunks, res.ZeroChunks)
		fmt.Fprintf(os.Stderr, "generation: %s\n", res.Generation)
	}
	fmt.Println(hex.EncodeToString(res.ManifestKey[:]))

	// Belt and braces: tmpOut already covered by defer, but make the
	// intent explicit when --upload finishes without --output.
	_ = tmpOut
}

func runFlatten(image, outputPath, tmpDir string) error {
	opts := flatten.Options{TmpDir: tmpDir}
	if image == "-" {
		return flatten.FlattenWith(os.Stdin, outputPath, opts)
	}
	image = strings.TrimPrefix(image, "docker-archive:")
	return flatten.FlattenFileWith(image, outputPath, opts)
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	input := fs.String("input", "-", "docker-archive tar (- for stdin)")
	tmpDir := fs.String("tmpdir", "", "parent of the per-run scratch directory")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	fs.Parse(args)

	// Buffer stdin to a temp file so we can seek across two flatten passes.
	inputPath := *input
	if *input == "-" {
		tmp, err := os.CreateTemp(*tmpDir, "verify-input-*.tar")
		if err != nil {
			fatal("create temp: %v", err)
		}
		defer os.Remove(tmp.Name())
		if _, err := io.Copy(tmp, os.Stdin); err != nil {
			tmp.Close()
			fatal("read stdin: %v", err)
		}
		tmp.Close()
		inputPath = tmp.Name()
	} else {
		inputPath = strings.TrimPrefix(*input, "docker-archive:")
	}

	tmp1, err := os.CreateTemp(*tmpDir, "verify-pass1-*.img")
	if err != nil {
		fatal("create temp: %v", err)
	}
	tmp1.Close()
	defer os.Remove(tmp1.Name())

	tmp2, err := os.CreateTemp(*tmpDir, "verify-pass2-*.img")
	if err != nil {
		fatal("create temp: %v", err)
	}
	tmp2.Close()
	defer os.Remove(tmp2.Name())

	opts := flatten.Options{TmpDir: *tmpDir}
	if err := flatten.FlattenFileWith(inputPath, tmp1.Name(), opts); err != nil {
		fatal("flatten pass 1: %v", err)
	}
	if err := flatten.FlattenFileWith(inputPath, tmp2.Name(), opts); err != nil {
		fatal("flatten pass 2: %v", err)
	}

	h1, sz1, err := hashAndSize(tmp1.Name())
	if err != nil {
		fatal("hash pass 1: %v", err)
	}
	h2, sz2, err := hashAndSize(tmp2.Name())
	if err != nil {
		fatal("hash pass 2: %v", err)
	}
	if !*noProgress {
		fmt.Fprintf(os.Stderr, "Pass 1: sha256:%s (%s)\n", h1, formatSize(sz1))
		fmt.Fprintf(os.Stderr, "Pass 2: sha256:%s (%s)\n", h2, formatSize(sz2))
	}
	if h1 == h2 {
		fmt.Fprintln(os.Stderr, "DETERMINISTIC")
		return
	}
	fmt.Fprintln(os.Stderr, "NOT DETERMINISTIC")
	os.Exit(1)
}

// ---------------------------------------------------------------------------
// info
// ---------------------------------------------------------------------------

func cmdInfo(args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	input := fs.String("input", "", "EROFS file path or manifest://<hex>")
	asJSON := fs.Bool("json", false, "machine-readable JSON output")
	manifestCfg := fs.String("manifest-config", "", "manifest config YAML (overrides MANIFEST_CONFIG env); required for manifest:// inputs")
	fs.Parse(args)
	if *input == "" {
		fatal("--input is required")
	}

	if strings.HasPrefix(*input, "manifest://") {
		hexKey := strings.TrimPrefix(*input, "manifest://")
		cfg := loadManifestCfg(*manifestCfg)
		path, cleanup, err := materializeManifest(cfg, hexKey)
		if err != nil {
			fatal("%v", err)
		}
		defer cleanup()
		printInfo(path, *asJSON)
		return
	}
	printInfo(*input, *asJSON)
}

func printInfo(path string, asJSON bool) {
	f, err := os.Open(path)
	if err != nil {
		fatal("open %s: %v", path, err)
	}
	defer f.Close()

	erofsSize, sbErr := flatten.ReadEROFSSize(f)
	if sbErr != nil {
		fatal("read EROFS superblock: %v", sbErr)
	}
	cfg, cfgErr := flatten.ReadConfigFromFile(path)
	if cfgErr != nil && !errors.Is(cfgErr, fs.ErrNotExist) {
		fatal("read config: %v", cfgErr)
	}

	if asJSON {
		out := struct {
			ErofsSize uint64                 `json:"erofs_size"`
			Config    *flatten.RuntimeConfig `json:"config"`
		}{ErofsSize: erofsSize, Config: cfg}
		body, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			fatal("marshal: %v", err)
		}
		fmt.Println(string(body))
		return
	}
	printInfoHuman(erofsSize, cfg)
}

// materializeManifest fetches the EROFS image addressed by hexKey into
// a temp file and returns its path plus a cleanup func. We need a
// regular file because flatten.ReadConfigFromFile / ReadEROFSSize use
// os.Open + seek-style reads.
func materializeManifest(cfg *manifest.Config, hexKey string) (string, func(), error) {
	fc, err := cfg.NewFetcher(cfg.FetchKeyFunc())
	if err != nil {
		return "", func() {}, fmt.Errorf("fetcher: %w", err)
	}
	defer fc.Close()

	key, err := manifest.ParseHexKey(hexKey)
	if err != nil {
		return "", func() {}, err
	}
	stream, err := fc.Fetch(context.Background(), key)
	if err != nil {
		return "", func() {}, fmt.Errorf("fetch manifest: %w", err)
	}
	tf, err := os.CreateTemp("", "flatten-info-*.img")
	if err != nil {
		return "", func() {}, fmt.Errorf("create temp: %w", err)
	}
	cleanup := func() { os.Remove(tf.Name()) }
	if err := stream.WriteTo(context.Background(), tf, 0, stream.ImageSize(), fetch.ReadOptions{}); err != nil {
		tf.Close()
		cleanup()
		return "", func() {}, fmt.Errorf("write: %w", err)
	}
	tf.Close()
	return tf.Name(), cleanup, nil
}

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

// ---------------------------------------------------------------------------
// Common helpers
// ---------------------------------------------------------------------------

func loadManifestCfg(flagPath string) *manifest.Config {
	cfg, err := manifest.LoadConfig(flagPath, manifestConfigEnv)
	if err != nil {
		if errors.Is(err, manifest.ErrConfigNotProvided) {
			fatal("missing manifest config: pass --manifest-config <path> or set %s", manifestConfigEnv)
		}
		fatal("load config: %v", err)
	}
	return cfg
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
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
