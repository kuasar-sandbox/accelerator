// manifest-ctl provides a CLI for manifest lifecycle operations:
// ingest, fetch, retrieve manifest blobs, inspect, verify, diff.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

const manifestConfigEnv = "MANIFEST_CONFIG"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "store":
		cmdStore(os.Args[2:])
	case "load":
		cmdLoad(os.Args[2:])
	case "get-manifest":
		cmdGetManifest(os.Args[2:])
	case "info":
		cmdInfo(os.Args[2:])
	case "verify":
		cmdVerify(os.Args[2:])
	case "diff":
		cmdDiff(os.Args[2:])
	case "config":
		cmdConfig(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: manifest-ctl <command> [flags]

Commands:
  store          Ingest a tarstream artifact (file or stdin) into the content
                 store; holes come from the envelope's map. Prints the hex key.
  load           Reconstruct (a window of) an image from a manifest key as a
                 tarstream artifact (holes ride the envelope losslessly).
  get-manifest   Fetch a manifest blob by hex content key.
  info           Print manifest metadata.
  verify         Verify chunk integrity against a manifest.
  diff           Compare two manifests' shared / unique chunks.
  config         Inspect or generate the manifest config file.

Configuration:
  Every command (except 'config generate') reads a YAML config file
  via --manifest-config <path> or the MANIFEST_CONFIG environment
  variable. Flag wins; there is no auto-discovery.
  The sensitive manifest.key may be omitted from the file and supplied
  via the MANIFEST_KEY environment variable instead (MANIFEST_KEY also
  overrides a manifest.key set in the file).
`)
}

// globalFlags carries the flags shared by every subcommand.
type globalFlags struct {
	configPath *string
}

func addGlobalFlags(fs *flag.FlagSet) globalFlags {
	var g globalFlags
	g.configPath = fs.String("manifest-config", "", "path to manifest config YAML (overrides MANIFEST_CONFIG env)")
	return g
}

func loadCfg(configPath string) *manifest.Config {
	cfg, err := manifest.LoadConfig(configPath, manifestConfigEnv)
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

// ---------------------------------------------------------------------------
// Common helpers
// ---------------------------------------------------------------------------

func createOutput(path string) (*os.File, error) {
	if path == "" || path == "-" {
		return os.Stdout, nil
	}
	return os.Create(path)
}

func formatSize(n uint64) string {
	switch {
	case n >= 1<<40:
		return fmt.Sprintf("%.1f TiB", float64(n)/float64(uint64(1)<<40))
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/float64(uint64(1)<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/float64(uint64(1)<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/float64(uint64(1)<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func chunkModeString(m codec.ChunkMode) string {
	switch m {
	case codec.ChunkModeFastCDC:
		return "cdc"
	case codec.ChunkModeFixed:
		return "fixed"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// readManifestData reads a manifest blob from path ("-" or "" → stdin).
func readManifestData(path string) ([]byte, error) {
	if path == "" || path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

// ---------------------------------------------------------------------------
// store command
// ---------------------------------------------------------------------------

func cmdStore(args []string) {
	fs := flag.NewFlagSet("store", flag.ExitOnError)
	extraSalt := fs.String("extra-salt", "", "optional extra-salt bytes mixed with the store-supplied salt")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	gf := addGlobalFlags(fs)
	fs.Parse(args)
	input := fs.Arg(0)
	if input == "" {
		input = "-" // default: read the artifact from stdin
	}

	cfg := loadCfg(*gf.configPath)

	// The input is a tarstream artifact (the platform container for
	// images and snapshots): the envelope carries size + hole map, so
	// stdin streams straight through one-pass with nothing buffered,
	// and files are positioned-read in place. Holes come from the
	// envelope — never detected from the filesystem or content.
	var (
		src  sparse.Source
		size uint64
	)
	if input == "-" {
		s, _, err := tarstream.SourceFrom(os.Stdin, "")
		if err != nil {
			fatal("stdin: not a tarstream artifact: %v", err)
		}
		src, size = s, s.Size()
	} else {
		stream, err := fetch.OpenTarStream(input)
		if err != nil {
			fatal("%v", err)
		}
		defer stream.Close()
		src, size = stream, stream.Size()
	}

	// Optional extra-salt closure — nil when --extra-salt is empty.
	var extraSaltFn ingest.ExtraSaltFunc
	if *extraSalt != "" {
		bs := []byte(*extraSalt)
		extraSaltFn = func() ([]byte, error) { return bs, nil }
	}

	ing, err := cfg.NewIngester(cfg.IngestKeyFunc(), extraSaltFn)
	if err != nil {
		fatal("ingester: %v", err)
	}
	defer ing.Close()

	var onProgress func(processed, total uint64)
	if !*noProgress {
		onProgress = func(processed, total uint64) {
			if total > 0 {
				pct := float64(processed) / float64(total) * 100
				fmt.Fprintf(os.Stderr, "\rstore: %s / %s (%.1f%%)", formatSize(processed), formatSize(total), pct)
			} else {
				fmt.Fprintf(os.Stderr, "\rstore: %s", formatSize(processed))
			}
		}
	}

	ctx := context.Background()
	result, err := ing.Ingest(ctx, src, ingest.IngestOption{
		OnProgress: onProgress,
	})
	if err != nil {
		fatal("ingest: %v", err)
	}

	if !*noProgress {
		fmt.Fprintln(os.Stderr)
	}

	// Print the manifest content key on stdout (the consumer-friendly
	// short form — no separate --manifest output file).
	fmt.Println(hex.EncodeToString(result.ManifestKey[:]))

	fmt.Fprintf(os.Stderr, "image size:   %s\n", formatSize(size))
	fmt.Fprintf(os.Stderr, "stored bytes: %s\n", formatSize(result.StoredBytes))
	fmt.Fprintf(os.Stderr, "chunks:       stored=%d dedup=%d zero=%d\n",
		result.StoredChunks, result.DedupChunks, result.ZeroChunks)
	fmt.Fprintf(os.Stderr, "manifest key: %s\n", hex.EncodeToString(result.ManifestKey[:]))
}

// ---------------------------------------------------------------------------
// load command
// ---------------------------------------------------------------------------

func cmdLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	output := fs.String("output", "-", "output file (- for stdout)")
	name := fs.String("name", "image", "entry name inside the emitted tarstream artifact")
	offset := fs.Uint64("offset", 0, "byte offset of the window to load")
	length := fs.Uint64("length", 0, "window length (0 = remainder)")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	keyArg := fs.Arg(0)
	if keyArg == "" {
		fatal("usage: manifest-ctl load [flags] <hex|manifest://hex>")
	}
	cfg := loadCfg(*gf.configPath)

	fc, err := cfg.NewFetcher()
	if err != nil {
		fatal("fetcher: %v", err)
	}
	defer fc.Close()

	key, err := manifest.ParseKeyRef(keyArg)
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	stream, err := fc.OpenManifest(ctx, key)
	if err != nil {
		fatal("fetch manifest: %v", err)
	}
	defer stream.Close()

	imageSize := stream.Size()
	readOffset := *offset
	readLength := *length
	if readLength == 0 && imageSize > readOffset {
		readLength = imageSize - readOffset
	}
	if readOffset >= imageSize {
		fatal("offset %d is at or beyond image size %d", readOffset, imageSize)
	}
	if readOffset+readLength > imageSize {
		readLength = imageSize - readOffset
	}

	out, err := createOutput(*output)
	if err != nil {
		fatal("create output: %v", err)
	}
	defer func() {
		if out != os.Stdout {
			out.Close()
		}
	}()
	if out == os.Stdout {
		if st, err := os.Stdout.Stat(); err == nil && st.Mode()&os.ModeCharDevice != 0 {
			fatal("load: refusing to write a tarstream artifact to a terminal (use --output FILE or redirect stdout)")
		}
	}

	// The output is a tarstream artifact: holes ride the envelope's
	// map losslessly (no materialization policy to choose), Zero
	// chunks are synthesized by the writer without fetching, and Data
	// chunks flow through the fetch path. A window (--offset/--length)
	// loads that slice as its own artifact.
	var src sparse.Source = stream
	if readOffset != 0 || readLength != imageSize {
		src = &windowSource{s: stream, base: readOffset, size: readLength}
	}
	var w io.Writer = out
	if !*noProgress {
		w = &progressWriter{w: out, label: "load"}
	}
	if _, err := tarstream.WriteTo(ctx, w, *name, src); err != nil {
		fatal("%v", err)
	}
	if !*noProgress {
		fmt.Fprintln(os.Stderr)
	}
	fmt.Fprintf(os.Stderr, "loaded %s from offset %d as %q\n", formatSize(readLength), readOffset, *name)
}

// windowSource exposes [base, base+size) of s as a source of its own.
type windowSource struct {
	s          sparse.Source
	base, size uint64
}

func (w *windowSource) Size() uint64 { return w.size }

func (w *windowSource) RunAt(off, limit uint64) (sparse.RunKind, uint64, error) {
	if off >= w.size {
		return 0, 0, io.EOF
	}
	if limit > w.size-off {
		limit = w.size - off
	}
	kind, end, err := w.s.RunAt(w.base+off, limit)
	if err != nil {
		return kind, 0, err
	}
	return kind, end - w.base, nil
}

func (w *windowSource) ReadAt(ctx context.Context, buf []byte, off uint64) (int, error) {
	if off >= w.size {
		return 0, io.EOF
	}
	var eof error
	if off+uint64(len(buf)) > w.size {
		buf = buf[:w.size-off]
		eof = io.EOF
	}
	n, err := w.s.ReadAt(ctx, buf, w.base+off)
	if err != nil && err != io.EOF {
		return 0, err
	}
	return n, eof
}

// progressWriter logs running byte counts to stderr (throttled).
type progressWriter struct {
	w     io.Writer
	label string
	n     uint64
	last  time.Time
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	p.n += uint64(n)
	if time.Since(p.last) >= 2*time.Second {
		fmt.Fprintf(os.Stderr, "\r%s: %s written", p.label, formatSize(p.n))
		p.last = time.Now()
	}
	return n, err
}

// ---------------------------------------------------------------------------
// get-manifest command — fetch the manifest blob (raw bytes) by key
// ---------------------------------------------------------------------------

func cmdGetManifest(args []string) {
	fs := flag.NewFlagSet("get-manifest", flag.ExitOnError)
	output := fs.String("output", "-", "output file (- for stdout)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)
	keyArg := fs.Arg(0)
	if keyArg == "" {
		fatal("usage: manifest-ctl get-manifest [--output -|FILE] <hex|manifest://hex>")
	}
	cfg := loadCfg(*gf.configPath)

	key, err := manifest.ParseKeyRef(keyArg)
	if err != nil {
		fatal("%v", err)
	}
	data, err := cfg.GetManifestBlob(context.Background(), key)
	if err != nil {
		fatal("%v", err)
	}
	out, err := createOutput(*output)
	if err != nil {
		fatal("create output: %v", err)
	}
	defer func() {
		if out != os.Stdout {
			out.Close()
		}
	}()
	if _, err := out.Write(data); err != nil {
		fatal("write output: %v", err)
	}
}

// ---------------------------------------------------------------------------
// info command
// ---------------------------------------------------------------------------

func cmdInfo(args []string) {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	gf := addGlobalFlags(fs)
	fs.Parse(args)
	src := fs.Arg(0)
	if src == "" {
		src = "-" // default: manifest bytes on stdin
	}

	var data []byte
	var err error
	if strings.HasPrefix(src, "manifest://") {
		// manifest://<hex>: fetch the raw manifest blob from store
		// (--manifest-config required, as for get-manifest).
		key, perr := manifest.ParseKeyRef(src)
		if perr != nil {
			fatal("%v", perr)
		}
		data, err = loadCfg(*gf.configPath).GetManifestBlob(context.Background(), key)
		if err != nil {
			fatal("get manifest: %v", err)
		}
	} else {
		// File path, or - / empty = stdin.
		data, err = readManifestData(src)
		if err != nil {
			fatal("read manifest: %v", err)
		}
	}
	m, sealedKT, err := codec.Unmarshal(data)
	if err != nil {
		fatal("unmarshal manifest: %v", err)
	}

	count := m.ChunkCount()
	var (
		totalChunkBytes uint64
		zeroChunks      uint32
		zeroBytes       uint64
	)
	for _, e := range m.Entries {
		totalChunkBytes += uint64(e.Size)
		if e.IsZero {
			zeroChunks++
			zeroBytes += uint64(e.Size)
		}
	}
	var avgSize uint64
	if count > 0 {
		avgSize = totalChunkBytes / uint64(count)
	}
	var holeBytes uint64
	for _, h := range m.Holes {
		holeBytes += h.Size
	}

	fmt.Printf("version:       %d\n", m.Version)
	fmt.Printf("image size:    %s (%d bytes)\n", formatSize(m.ImageSize), m.ImageSize)
	fmt.Printf("chunk mode:    %s\n", chunkModeString(m.ChunkMode))
	fmt.Printf("chunk count:   %d\n", count)
	fmt.Printf("zero chunks:   %d (%s)\n", zeroChunks, formatSize(zeroBytes))
	fmt.Printf("holes:         %d (%s)\n", len(m.Holes), formatSize(holeBytes))
	fmt.Printf("min chunk:     %s    (configured)\n", formatSize(uint64(m.MinChunkSize)))
	fmt.Printf("max chunk:     %s    (configured)\n", formatSize(uint64(m.MaxChunkSize)))
	fmt.Printf("avg chunk:     %s\n", formatSize(avgSize))
	printChunkDistribution(m.Entries, m.MinChunkSize, m.MaxChunkSize)
	fmt.Printf("key table:     %d bytes (sealed)\n", len(sealedKT))
	fmt.Printf("manifest size: %d bytes\n", len(data))
}

// printChunkDistribution renders a single-line percentile table for
// data-chunk sizes; same shape as the legacy implementation.
func printChunkDistribution(entries []codec.ChunkEntry, configuredMin, configuredMax uint32) {
	sizes := make([]uint32, 0, len(entries))
	for _, e := range entries {
		if !e.IsZero {
			sizes = append(sizes, e.Size)
		}
	}
	if len(sizes) == 0 {
		return
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
	minObs := sizes[0]
	maxObs := sizes[len(sizes)-1]
	minCount, maxCount := 0, 0
	for _, s := range sizes {
		if s == minObs {
			minCount++
		}
		if s == maxObs {
			maxCount++
		}
	}
	pctVal := func(p float64) string { return shortSize(uint64(percentileU32(sizes, p))) }

	const cellW = 11
	fmt.Println()
	fmt.Println("size distribution (data chunks):")
	fmt.Printf("  %*s %*s %*s %*s %*s %*s %*s %*s %*s\n",
		cellW, "min(N)",
		cellW, "P1", cellW, "P5", cellW, "P25", cellW, "P50",
		cellW, "P75", cellW, "P95", cellW, "P99",
		cellW, "max(N)")
	fmt.Printf("  %*s %*s %*s %*s %*s %*s %*s %*s %*s\n",
		cellW, fmt.Sprintf("%s(%d)", shortSize(uint64(minObs)), minCount),
		cellW, pctVal(0.01),
		cellW, pctVal(0.05),
		cellW, pctVal(0.25),
		cellW, pctVal(0.50),
		cellW, pctVal(0.75),
		cellW, pctVal(0.95),
		cellW, pctVal(0.99),
		cellW, fmt.Sprintf("%s(%d)", shortSize(uint64(maxObs)), maxCount))
	if maxObs == configuredMax && configuredMin < configuredMax {
		fmt.Printf("  (max == configured max — %d/%d (%.1f%%) chunks are forced CDC cuts)\n",
			maxCount, len(sizes), 100.0*float64(maxCount)/float64(len(sizes)))
	}
}

func percentileU32(sorted []uint32, p float64) uint32 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func shortSize(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(n)/float64(uint64(1)<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(n)/float64(uint64(1)<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fK", float64(n)/float64(uint64(1)<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// ---------------------------------------------------------------------------
// verify command
// ---------------------------------------------------------------------------

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	gf := addGlobalFlags(fs)
	fs.Parse(args)
	keyArg := fs.Arg(0)
	if keyArg == "" {
		fatal("usage: manifest-ctl verify [flags] <hex|manifest://hex>")
	}
	cfg := loadCfg(*gf.configPath)

	fc, err := cfg.NewFetcher()
	if err != nil {
		fatal("fetcher: %v", err)
	}
	defer fc.Close()

	key, err := manifest.ParseKeyRef(keyArg)
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	totalFailed := 0
	// Enumerate chunks from the raw manifest blob (as `info` does), then
	// verify each non-zero chunk through the fetch path — fetch, decrypt,
	// and per-chunk key validation end-to-end.
	data, derr := cfg.GetManifestBlob(ctx, key)
	if derr != nil {
		fatal("get manifest: %v", derr)
	}
	m, _, derr := codec.Unmarshal(data)
	if derr != nil {
		fatal("unmarshal manifest: %v", derr)
	}
	stream, ferr := fc.OpenManifest(ctx, key)
	if ferr != nil {
		fatal("open manifest: %v", ferr)
	}
	defer stream.Close()

	count := int(m.ChunkCount())
	verified, skipped, failed := 0, 0, 0
	buf := make([]byte, m.MaxChunkSize)
	for i, entry := range m.Entries {
		if entry.IsZero {
			skipped++
			if !*noProgress {
				fmt.Fprintf(os.Stderr, "\rverify: %d/%d (skip zero)", i+1, count)
			}
			continue
		}
		if cap(buf) < int(entry.Size) {
			buf = make([]byte, entry.Size)
		}
		buf = buf[:entry.Size]
		if _, rerr := stream.ReadAt(ctx, buf, entry.Offset); rerr != nil {
			fmt.Fprintf(os.Stderr, "\nchunk %d: read failed: %v\n", i, rerr)
			failed++
			continue
		}
		verified++
		if !*noProgress {
			fmt.Fprintf(os.Stderr, "\rverify: %d/%d", i+1, count)
		}
	}
	if !*noProgress {
		fmt.Fprintln(os.Stderr)
	}
	fmt.Fprintf(os.Stderr, "verified: %d  skipped(zero): %d  holes: %d  failed: %d  total chunks: %d\n",
		verified, skipped, len(m.Holes), failed, count)
	totalFailed += failed
	if totalFailed > 0 {
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// diff command
// ---------------------------------------------------------------------------

func cmdDiff(args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: manifest-ctl diff <manifest-a> <manifest-b>\n")
		os.Exit(1)
	}
	pathA, pathB := args[0], args[1]
	dataA, err := os.ReadFile(pathA)
	if err != nil {
		fatal("read manifest-a: %v", err)
	}
	dataB, err := os.ReadFile(pathB)
	if err != nil {
		fatal("read manifest-b: %v", err)
	}
	mA, _, err := codec.Unmarshal(dataA)
	if err != nil {
		fatal("unmarshal manifest-a: %v", err)
	}
	mB, _, err := codec.Unmarshal(dataB)
	if err != nil {
		fatal("unmarshal manifest-b: %v", err)
	}
	setA := make(map[[32]byte]struct{})
	var bytesA uint64
	for _, e := range mA.Entries {
		if !e.IsZero {
			setA[e.CiphertextHash] = struct{}{}
			bytesA += uint64(e.Size)
		}
	}
	setB := make(map[[32]byte]struct{})
	var bytesB uint64
	for _, e := range mB.Entries {
		if !e.IsZero {
			setB[e.CiphertextHash] = struct{}{}
			bytesB += uint64(e.Size)
		}
	}
	var shared, onlyA, onlyB int
	var sharedBytes, onlyABytes, onlyBBytes uint64
	for _, e := range mA.Entries {
		if e.IsZero {
			continue
		}
		if _, ok := setB[e.CiphertextHash]; ok {
			shared++
			sharedBytes += uint64(e.Size)
		} else {
			onlyA++
			onlyABytes += uint64(e.Size)
		}
	}
	for _, e := range mB.Entries {
		if e.IsZero {
			continue
		}
		if _, ok := setA[e.CiphertextHash]; !ok {
			onlyB++
			onlyBBytes += uint64(e.Size)
		}
	}
	uniqueA := len(setA)
	uniqueB := len(setB)
	merged := make(map[[32]byte]struct{})
	for h := range setA {
		merged[h] = struct{}{}
	}
	for h := range setB {
		merged[h] = struct{}{}
	}
	uniqueMerged := len(merged)
	totalUnique := uniqueA + uniqueB
	var dedupRatio float64
	if totalUnique > 0 {
		dedupRatio = 1.0 - float64(uniqueMerged)/float64(totalUnique)
	}
	var holeBytesA, holeBytesB uint64
	for _, h := range mA.Holes {
		holeBytesA += h.Size
	}
	for _, h := range mB.Holes {
		holeBytesB += h.Size
	}
	fmt.Printf("manifest A:  %s, %d chunks (%d unique)\n", formatSize(mA.ImageSize), len(mA.Entries), uniqueA)
	fmt.Printf("manifest B:  %s, %d chunks (%d unique)\n", formatSize(mB.ImageSize), len(mB.Entries), uniqueB)
	fmt.Printf("holes A:     %d (%s)\n", len(mA.Holes), formatSize(holeBytesA))
	fmt.Printf("holes B:     %d (%s)\n", len(mB.Holes), formatSize(holeBytesB))
	fmt.Printf("shared:      %d chunks (%s)\n", shared, formatSize(sharedBytes))
	fmt.Printf("only in A:   %d chunks (%s)\n", onlyA, formatSize(onlyABytes))
	fmt.Printf("only in B:   %d chunks (%s)\n", onlyB, formatSize(onlyBBytes))
	fmt.Printf("dedup ratio: %.1f%%\n", dedupRatio*100)
}

// ---------------------------------------------------------------------------
// hole policy — used by `load`
// ---------------------------------------------------------------------------

func holeFillZero(w io.Writer, _, size uint64) error {
	_, err := io.CopyN(w, zeroReader{}, int64(size))
	return err
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// ---------------------------------------------------------------------------
// compile-time reachability for store types
// ---------------------------------------------------------------------------

var _ store.Partition = store.PartitionChunk
