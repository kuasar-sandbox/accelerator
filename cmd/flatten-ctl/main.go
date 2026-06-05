// flatten-ctl converts OCI / docker-archive images into deterministic
// EROFS rootfs images with an appended OCI runtime-config ZIP.
//
// Subcommands:
//
//	flatten-ctl export   [--output <path|->] [--config <path>]
//	                     [--manifest-config <path>] [--upload] [--with-referer]  <path|->
//	flatten-ctl verify   [--tmpdir D] [--no-progress]  <path|->
//	flatten-ctl info     [--json] [--manifest-config <path>]  <path|manifest://hex>
//	flatten-ctl cache    gc | info
//	flatten-ctl config   [--config <path>] [--template] [-o <file>]
//
// The image is a positional arg (flags must precede it — stdlib flag).
// For export/verify it defaults to `-` (docker-archive on stdin) when
// omitted; `-` may also be given explicitly.
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

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/sandbox-builder/internal/util"
	"github.com/kuasar-sandbox/sandbox-builder/pkg/flatten"
	"github.com/kuasar-sandbox/sandbox-builder/pkg/image"
	"github.com/kuasar-sandbox/sandbox-builder/pkg/remote"
)

const (
	manifestConfigEnv = "MANIFEST_CONFIG"
	flattenConfigEnv  = "FLATTEN_CONFIG"
)

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
	case "cache":
		cmdCache(os.Args[2:])
	case "config":
		cmdConfig(os.Args[2:])
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
  export   Flatten OCI/docker-archive or a registry image → EROFS (optionally upload).
  verify   Flatten twice, fail if outputs differ.
  info     Print EROFS metadata + OCI runtime config.
  cache    Inspect or garbage-collect the registry blob cache.
  config   Emit/validate a flatten config (FLATTEN_CONFIG): tmpdir/platform/cache/referer.

See `+"`flatten-ctl <command> -h`"+` for per-command flags.
`)
}

// ---------------------------------------------------------------------------
// export
// ---------------------------------------------------------------------------

func cmdExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	output := fs.String("output", "", "EROFS output path (- for stdout, empty = required with --upload off)")
	configPath := fs.String("config", "", "flatten config YAML (overrides FLATTEN_CONFIG env): tmpdir/platform/cache/referer (registry sources)")
	platform := fs.String("platform", "", "override the pull platform (os/arch[/variant]) from the config")
	manifestCfg := fs.String("manifest-config", "", "manifest config YAML (overrides MANIFEST_CONFIG env)")
	upload := fs.Bool("upload", false, "after flatten, ingest the EROFS into the store and print the manifest key on stdout")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	printDigest := fs.Bool("print-digest", false, "print the resolved source image digest (repo@sha256:...) on stdout (registry sources; incompatible with --output -)")
	forceRegistry := fs.Bool("registry", false, "force the positional arg to be a registry reference")
	forceArchive := fs.Bool("archive", false, "force the positional arg to be a local docker-archive")
	withReferer := fs.Bool("with-referer", false, "force the idempotent OCI-Referrers flow (also enableable via referer.enabled in --config); requires --upload")
	fs.Parse(args)
	input := fs.Arg(0)
	if input == "" {
		input = "-" // default: docker-archive stream on stdin
	}

	cfg, err := remote.LoadConfig(*configPath, flattenConfigEnv)
	if err != nil {
		fatal("%v", err)
	}
	if err := cfg.SetPlatform(*platform); err != nil {
		fatal("%v", err)
	}
	withRef := *withReferer || cfg.Referer.Enabled

	if !*upload && (*output == "" || *output == "/dev/null") {
		fatal("--output is required when --upload is not set")
	}
	if *forceRegistry && *forceArchive {
		fatal("--registry and --archive are mutually exclusive")
	}
	remoteSrc := isRemoteSource(input, *forceRegistry, *forceArchive)
	if *printDigest && !remoteSrc {
		fatal("--print-digest applies only to registry sources")
	}
	if *printDigest && *output == "-" {
		fatal("--print-digest is incompatible with --output - (both write stdout)")
	}

	// The idempotent OCI-Referrers flow (--with-referer or referer.enabled): its
	// deliverable is the manifest key on stdout, so it owns its own pull /
	// flatten / ingest path and returns early.
	if withRef {
		if !remoteSrc {
			fatal("--with-referer / referer.enabled applies only to registry sources")
		}
		if !*upload {
			fatal("--with-referer / referer.enabled requires --upload")
		}
		if *output != "" {
			fatal("--with-referer is incompatible with --output (deliverable is the manifest key)")
		}
		if *printDigest {
			fatal("--with-referer is incompatible with --print-digest (stdout carries the manifest key)")
		}
		runReferrerExport(input, cfg, *manifestCfg, *noProgress)
		return
	}

	// Resolve output path: when --output is "-" or empty (+upload), use
	// a temp file we can re-open after FlattenFile finishes.
	tmpOut := false
	outPath := *output
	if *upload || outPath == "-" {
		tf, err := os.CreateTemp(cfg.TmpDir, "flatten-out-*.img")
		if err != nil {
			fatal("create temp output: %v", err)
		}
		tf.Close()
		outPath = tf.Name()
		tmpOut = true
		defer os.Remove(outPath)
	}

	if remoteSrc {
		if err := runRemoteFlatten(input, outPath, cfg, *printDigest, *noProgress); err != nil {
			fatal("%v", err)
		}
	} else {
		if err := runFlatten(input, outPath, cfg.TmpDir); err != nil {
			fatal("%v", err)
		}
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

	mcfg := loadManifestCfg(*manifestCfg)
	key, err := ingestEROFS(outPath, info.Size(), mcfg, *noProgress)
	if err != nil {
		fatal("%v", err)
	}
	fmt.Println(key)

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

// isRemoteSource decides whether the export/verify positional arg names a
// remote registry reference (vs a local docker-archive). Precedence: --archive
// and stdin / `docker-archive:` force local; --registry forces remote; an
// existing on-disk file is local; otherwise anything that parses as a registry
// reference is remote. The force flags disambiguate the rare case of a local
// file literally named like `repo:tag`.
func isRemoteSource(input string, forceRegistry, forceArchive bool) bool {
	if forceArchive {
		return false
	}
	if input == "-" || strings.HasPrefix(input, "docker-archive:") {
		return false
	}
	if forceRegistry {
		return true
	}
	if st, err := os.Stat(input); err == nil && !st.IsDir() {
		return false
	}
	return remote.LooksLikeReference(input)
}

// runRemoteFlatten resolves a registry reference (pinning the platform-selected
// digest), pulls its blobs through the shared OCI-layout cache, and flattens
// them into outputPath via the same deterministic sink the docker-archive path
// uses. The resolved repo@sha256 is logged to stderr (unless --no-progress) and
// echoed to stdout when --print-digest is set.
func runRemoteFlatten(ref, outputPath string, cfg *remote.Config, printDigest, noProgress bool) error {
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		return err
	}
	if !noProgress {
		fmt.Fprintf(os.Stderr, "resolved: %s\n", res.Digest)
	}
	if printDigest {
		fmt.Println(res.Digest.String())
	}
	cache, cleanup, err := cfg.OpenCache()
	if err != nil {
		return err
	}
	defer cleanup()
	src, err := cfg.Pull(ctx, res, cache)
	if err != nil {
		return err
	}
	if err := flatten.Build(src, outputPath, flatten.Options{TmpDir: cfg.TmpDir}); err != nil {
		return err
	}
	return cache.MaybeEvict()
}

// ingestEROFS uploads the EROFS at path into the content store via the
// manifest ingester and returns the hex manifest key. Shared by the plain
// --upload tail and the --with-referer flow.
func ingestEROFS(path string, size int64, mcfg *manifest.Config, noProgress bool) (string, error) {
	ing, err := mcfg.NewIngester(mcfg.IngestKeyFunc(), nil)
	if err != nil {
		return "", fmt.Errorf("ingester: %w", err)
	}
	defer ing.Close()

	in, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("re-open output for upload: %w", err)
	}
	defer in.Close()

	res, err := ing.Ingest(context.Background(), in, uint64(size), ingest.IngestOption{})
	if err != nil {
		return "", fmt.Errorf("ingest: %w", err)
	}
	if !noProgress {
		fmt.Fprintf(os.Stderr, "stored: %s (chunks stored=%d dedup=%d zero=%d)\n",
			formatSize(int64(res.StoredBytes)), res.StoredChunks, res.DedupChunks, res.ZeroChunks)
		fmt.Fprintf(os.Stderr, "generation: %s\n", res.Generation)
	}
	return hex.EncodeToString(res.ManifestKey[:]), nil
}

// runReferrerExport implements --with-referer: resolve the source, look up an
// existing flatten-manifest referrer for this owner and (on a hit) print its
// manifest id without re-exporting; otherwise pull + flatten + ingest, then
// write the referrer back to the source repo. Requires manifest config (for
// the customer key / ingest) and push access to the source repo.
func runReferrerExport(ref string, rcfg *remote.Config, manifestCfgPath string, noProgress bool) {
	mcfg := loadManifestCfg(manifestCfgPath)
	ck, err := mcfg.CustomerKey()
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	res, err := rcfg.Resolve(ctx, ref)
	if err != nil {
		fatal("%v", err)
	}
	if !noProgress {
		fmt.Fprintf(os.Stderr, "resolved: %s\n", res.Digest)
	}

	// Idempotent skip: a matching, unexpired referrer means the manifest was
	// already produced — reuse its id, no pull/flatten/upload.
	if id, ok, err := rcfg.FindReferrer(ctx, res, ck[:]); err != nil {
		if !noProgress {
			fmt.Fprintf(os.Stderr, "referrer lookup failed (%v); proceeding with export\n", err)
		}
	} else if ok {
		if !noProgress {
			fmt.Fprintf(os.Stderr, "referrer hit: reusing manifest %s (skipped flatten+upload)\n", id)
		}
		fmt.Println(id)
		return
	}

	// Miss: pull + flatten + ingest, then write the referrer back.
	cache, cleanup, err := rcfg.OpenCache()
	if err != nil {
		fatal("%v", err)
	}
	defer cleanup()
	src, err := rcfg.Pull(ctx, res, cache)
	if err != nil {
		fatal("%v", err)
	}
	tmpOut, err := os.CreateTemp(rcfg.TmpDir, "flatten-out-*.img")
	if err != nil {
		fatal("create temp output: %v", err)
	}
	tmpOut.Close()
	defer os.Remove(tmpOut.Name())
	if err := flatten.Build(src, tmpOut.Name(), flatten.Options{TmpDir: rcfg.TmpDir}); err != nil {
		fatal("%v", err)
	}
	if err := cache.MaybeEvict(); err != nil && !noProgress {
		fmt.Fprintf(os.Stderr, "cache gc warning: %v\n", err)
	}
	info, err := os.Stat(tmpOut.Name())
	if err != nil {
		fatal("stat output: %v", err)
	}
	if !noProgress {
		fmt.Fprintf(os.Stderr, "EROFS image: %s\n", formatSize(info.Size()))
	}

	key, err := ingestEROFS(tmpOut.Name(), info.Size(), mcfg, noProgress)
	if err != nil {
		fatal("%v", err)
	}
	if err := rcfg.PutReferrer(ctx, res, key, ck[:]); err != nil {
		fatal("%v", err) // hard fail (e.g. no push access to source repo) per design
	}
	if !noProgress {
		fmt.Fprintf(os.Stderr, "referrer written: %s\n", res.Digest)
	}
	fmt.Println(key)
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	tmpDir := fs.String("tmpdir", "", "parent of the per-run scratch directory")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	configPath := fs.String("config", "", "flatten config YAML (overrides FLATTEN_CONFIG env); used when the source is a registry reference")
	fs.Parse(args)
	input := fs.Arg(0)
	if input == "" {
		input = "-" // default: docker-archive stream on stdin
	}

	// Registry source: pull once into the cache, then flatten twice from the
	// same cached blobs and compare. (A moving tag is only as reproducible as
	// the tag; pin @sha256 for a stable check.)
	if isRemoteSource(input, false, false) {
		verifyRemote(input, *tmpDir, *configPath, *noProgress)
		return
	}

	// Buffer stdin to a temp file so we can seek across two flatten passes.
	inputPath := input
	if input == "-" {
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
		inputPath = strings.TrimPrefix(input, "docker-archive:")
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

// verifyRemote pulls a registry image once into the shared cache, flattens it
// twice from the same cached blobs, and compares the EROFS hashes.
func verifyRemote(ref, tmpDir, configPath string, noProgress bool) {
	cfg, err := remote.LoadConfig(configPath, flattenConfigEnv)
	if err != nil {
		fatal("%v", err)
	}
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		fatal("%v", err)
	}
	cache, cleanup, err := cfg.OpenCache()
	if err != nil {
		fatal("%v", err)
	}
	defer cleanup()
	src, err := cfg.Pull(ctx, res, cache)
	if err != nil {
		fatal("%v", err)
	}

	tmp1, err := os.CreateTemp(tmpDir, "verify-pass1-*.img")
	if err != nil {
		fatal("create temp: %v", err)
	}
	tmp1.Close()
	defer os.Remove(tmp1.Name())
	tmp2, err := os.CreateTemp(tmpDir, "verify-pass2-*.img")
	if err != nil {
		fatal("create temp: %v", err)
	}
	tmp2.Close()
	defer os.Remove(tmp2.Name())

	opts := flatten.Options{TmpDir: tmpDir}
	if err := flatten.Build(src, tmp1.Name(), opts); err != nil {
		fatal("flatten pass 1: %v", err)
	}
	if err := flatten.Build(src, tmp2.Name(), opts); err != nil {
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
	if !noProgress {
		fmt.Fprintf(os.Stderr, "resolved: %s\n", res.Digest)
		fmt.Fprintf(os.Stderr, "Pass 1: sha256:%s (%s)\n", h1, formatSize(sz1))
		fmt.Fprintf(os.Stderr, "Pass 2: sha256:%s (%s)\n", h2, formatSize(sz2))
	}
	if h1 != h2 {
		fmt.Fprintln(os.Stderr, "NOT DETERMINISTIC")
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "DETERMINISTIC")
}

// ---------------------------------------------------------------------------
// info
// ---------------------------------------------------------------------------

func cmdInfo(args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "machine-readable JSON output")
	manifestCfg := fs.String("manifest-config", "", "manifest config YAML (overrides MANIFEST_CONFIG env); required for manifest:// inputs")
	fs.Parse(args)
	input := fs.Arg(0)
	if input == "" {
		fatal("usage: flatten-ctl info <erofs-path|manifest://hex> [--json] [--manifest-config <file>]")
	}

	if strings.HasPrefix(input, "manifest://") {
		hexKey := strings.TrimPrefix(input, "manifest://")
		cfg := loadManifestCfg(*manifestCfg)
		fc, err := cfg.NewFetcher(cfg.FetchKeyFunc())
		if err != nil {
			fatal("fetcher: %v", err)
		}
		defer fc.Close()
		key, err := manifest.ParseHexKey(hexKey)
		if err != nil {
			fatal("%v", err)
		}
		ctx := context.Background()
		stream, err := fc.Fetch(ctx, key)
		if err != nil {
			fatal("fetch manifest: %v", err)
		}
		defer stream.Close()
		// Read only the EROFS superblock + trailing ZIP directly over
		// the chunk-granular fetch path — no full materialization.
		size := int64(stream.Size())
		printInfo(fetch.NewReaderAt(ctx, stream, size), size, *asJSON)
		return
	}

	f, err := os.Open(input)
	if err != nil {
		fatal("open %s: %v", input, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		fatal("stat %s: %v", input, err)
	}
	printInfo(f, st.Size(), *asJSON)
}

// printInfo reports the EROFS image size and embedded RuntimeConfig from
// ra. Both image.ReadEROFSSize and image.ReadConfig need only the
// superblock and the trailing ZIP, so ra may be a plain *os.File (local
// path) or a fetch-backed io.ReaderAt (manifest://) — the manifest case
// then transfers only those few KB, never the whole image.
func printInfo(ra io.ReaderAt, size int64, asJSON bool) {
	erofsSize, sbErr := image.ReadEROFSSize(ra)
	if sbErr != nil {
		fatal("read EROFS superblock: %v", sbErr)
	}
	cfg, cfgErr := image.ReadConfig(ra, size)
	if cfgErr != nil && !errors.Is(cfgErr, fs.ErrNotExist) {
		fatal("read config: %v", cfgErr)
	}

	if asJSON {
		out := struct {
			ErofsSize uint64               `json:"erofs_size"`
			Config    *image.RuntimeConfig `json:"config"`
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

func printInfoHuman(erofsSize uint64, cfg *image.RuntimeConfig) {
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
// cache
// ---------------------------------------------------------------------------

func cmdCache(args []string) {
	if len(args) == 0 {
		fatal("usage: flatten-ctl cache <gc|info> [flags]")
	}
	switch args[0] {
	case "gc":
		cmdCacheGC(args[1:])
	case "info":
		cmdCacheInfo(args[1:])
	case "-h", "--help":
		fmt.Fprintln(os.Stderr, "usage: flatten-ctl cache <gc|info> [flags]")
	default:
		fatal("unknown cache subcommand %q (want gc|info)", args[0])
	}
}

// openCacheFromFlags resolves the cache directory and size cap from the remote
// config plus optional overrides, then opens the OCI-layout cache.
func openCacheFromFlags(remoteCfgPath, cacheDirOverride, maxSizeOverride string) (*remote.Cache, error) {
	cfg, err := remote.LoadConfig(remoteCfgPath, flattenConfigEnv)
	if err != nil {
		return nil, err
	}
	dir := cfg.CacheDir()
	if cacheDirOverride != "" {
		dir = cacheDirOverride
	}
	maxSize := cfg.MaxCacheBytes()
	if maxSizeOverride != "" {
		v, err := util.ParseSize(maxSizeOverride)
		if err != nil {
			return nil, fmt.Errorf("--cache-max-size: %w", err)
		}
		maxSize = int64(v)
	}
	return remote.OpenCache(dir, maxSize)
}

func cmdCacheInfo(args []string) {
	fs := flag.NewFlagSet("cache info", flag.ExitOnError)
	remoteCfg := fs.String("config", "", "flatten config YAML (overrides FLATTEN_CONFIG env)")
	cacheDir := fs.String("cache-dir", "", "cache directory (overrides config)")
	fs.Parse(args)

	cache, err := openCacheFromFlags(*remoteCfg, *cacheDir, "")
	if err != nil {
		fatal("%v", err)
	}
	st, err := cache.Stats()
	if err != nil {
		fatal("%v", err)
	}
	maxStr := "unlimited"
	if st.MaxSize > 0 {
		maxStr = formatSize(st.MaxSize)
	}
	fmt.Printf("Cache dir:   %s\n", st.Dir)
	fmt.Printf("Blobs:       %d\n", st.Blobs)
	fmt.Printf("Total size:  %s (%d bytes)\n", formatSize(st.TotalSize), st.TotalSize)
	fmt.Printf("Max size:    %s\n", maxStr)
}

func cmdCacheGC(args []string) {
	fs := flag.NewFlagSet("cache gc", flag.ExitOnError)
	remoteCfg := fs.String("config", "", "flatten config YAML (overrides FLATTEN_CONFIG env)")
	cacheDir := fs.String("cache-dir", "", "cache directory (overrides config)")
	maxSize := fs.String("cache-max-size", "", "evict LRU blobs down to this size (overrides config; \"0\" = evict all eligible)")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	fs.Parse(args)

	cache, err := openCacheFromFlags(*remoteCfg, *cacheDir, *maxSize)
	if err != nil {
		fatal("%v", err)
	}
	// Evict down to the (possibly overridden) configured cap. Stats reports
	// the resolved cap so we evict to exactly that target.
	st, err := cache.Stats()
	if err != nil {
		fatal("%v", err)
	}
	freed, err := cache.Evict(st.MaxSize)
	if err != nil {
		fatal("%v", err)
	}
	if !*noProgress {
		fmt.Fprintf(os.Stderr, "evicted: %s\n", formatSize(freed))
	}
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
