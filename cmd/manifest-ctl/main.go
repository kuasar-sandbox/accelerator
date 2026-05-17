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

	"github.com/fullof-work/mass-sandbox/pkg/manifest"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/codec"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/fetch"
	"github.com/fullof-work/mass-sandbox/pkg/manifest/ingest"
	"github.com/fullof-work/mass-sandbox/pkg/store"
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
  store          Ingest a file into the content store; the manifest blob is
                 uploaded automatically and the hex content key is printed.
  load           Reconstruct (part of) an image from a manifest key (or file).
  get-manifest   Fetch a manifest blob by hex content key.
  info           Print manifest metadata.
  verify         Verify chunk integrity against a manifest.
  diff           Compare two manifests' shared / unique chunks.
  config         Inspect or generate the manifest config file.

Configuration:
  Every command (except 'config generate') reads a YAML config file
  via --manifest-config <path> or the MANIFEST_CONFIG environment
  variable. Flag wins; there is no auto-discovery.
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

func openInput(path string) (*os.File, error) {
	if path == "" || path == "-" {
		return os.Stdin, nil
	}
	return os.Open(path)
}

func createOutput(path string) (*os.File, error) {
	if path == "" || path == "-" {
		return os.Stdout, nil
	}
	return os.Create(path)
}

func inputSize(f *os.File) (uint64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Mode().IsRegular() {
		return uint64(info.Size()), nil
	}
	return 0, nil
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
	input := fs.String("input", "-", "input file (- for stdin)")
	extraSalt := fs.String("extra-salt", "", "optional extra-salt bytes mixed with the store-supplied generation salt")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	detectHoles := fs.Bool("detect-holes", false, "detect filesystem holes in --input via lseek(SEEK_HOLE/SEEK_DATA) and record them as HoleExtents (file input only — ignored for stdin)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	cfg := loadCfg(*gf.configPath)

	// Resolve input + size. Stdin gets buffered to a temp file so the
	// chunker can stream over io.Reader and so detect-holes can call
	// lseek on a real fd (no-op for stdin, but consistent).
	in, err := openInput(*input)
	if err != nil {
		fatal("open input: %v", err)
	}
	defer func() {
		if in != os.Stdin {
			in.Close()
		}
	}()
	size, err := inputSize(in)
	if err != nil {
		fatal("stat input: %v", err)
	}
	if in == os.Stdin {
		data, err := io.ReadAll(in)
		if err != nil {
			fatal("read stdin: %v", err)
		}
		size = uint64(len(data))
		tmp, err := os.CreateTemp("", "manifest-ctl-store-*")
		if err != nil {
			fatal("create temp: %v", err)
		}
		defer os.Remove(tmp.Name())
		if _, err := tmp.Write(data); err != nil {
			tmp.Close()
			fatal("write temp: %v", err)
		}
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			tmp.Close()
			fatal("seek temp: %v", err)
		}
		in = tmp
	}

	// Hole detection (regular files only).
	var detectedHoles []codec.HoleExtent
	if *detectHoles {
		if fi, _ := in.Stat(); fi != nil && fi.Mode().IsRegular() {
			detectedHoles, err = codec.DetectHoles(in, size)
			if err != nil {
				fatal("detect holes: %v", err)
			}
			if !*noProgress {
				fmt.Fprintf(os.Stderr, "detected %d hole extent(s) in input\n", len(detectedHoles))
			}
		} else {
			fmt.Fprintln(os.Stderr, "warn: --detect-holes ignored for non-regular input")
		}
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
	result, err := ing.Ingest(ctx, in, size, ingest.IngestOption{
		Holes:      detectedHoles,
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
	fmt.Fprintf(os.Stderr, "generation:   %s\n", result.Generation)
	fmt.Fprintf(os.Stderr, "manifest key: %s\n", hex.EncodeToString(result.ManifestKey[:]))
}

// ---------------------------------------------------------------------------
// load command
// ---------------------------------------------------------------------------

func cmdLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	keyHex := fs.String("manifest-key", "", "hex content key of the manifest to load (required)")
	output := fs.String("output", "-", "output file (- for stdout)")
	offset := fs.Uint64("offset", 0, "byte offset to start reading")
	length := fs.Uint64("length", 0, "number of bytes to read (0 = remainder)")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	holePolicy := fs.String("hole", "error", "hole policy: error (default, fail on any hole), zero (fill with zero bytes), punch (fallocate PUNCH_HOLE on file output)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	if *keyHex == "" {
		fatal("--manifest-key is required")
	}
	cfg := loadCfg(*gf.configPath)

	fc, err := cfg.NewFetcher(cfg.FetchKeyFunc())
	if err != nil {
		fatal("fetcher: %v", err)
	}
	defer fc.Close()

	key, err := manifest.ParseHexKey(*keyHex)
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	stream, err := fc.Fetch(ctx, key)
	if err != nil {
		fatal("fetch manifest: %v", err)
	}

	m := stream.Manifest()
	readOffset := *offset
	readLength := *length
	if readLength == 0 && m.ImageSize > readOffset {
		readLength = m.ImageSize - readOffset
	}
	if readOffset >= m.ImageSize {
		fatal("offset %d is at or beyond image size %d", readOffset, m.ImageSize)
	}
	if readOffset+readLength > m.ImageSize {
		readLength = m.ImageSize - readOffset
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

	opts := fetch.ReadOptions{}
	if !*noProgress {
		opts.OnProgress = func(done, total int) {
			pct := float64(done) / float64(total) * 100
			fmt.Fprintf(os.Stderr, "\rload: %d/%d chunks (%.1f%%)", done, total, pct)
		}
	}
	holeFn, err := makeHolePolicy(*holePolicy, out)
	if err != nil {
		fatal("%v", err)
	}
	opts.OnHole = holeFn

	if err := stream.WriteTo(ctx, out, readOffset, readLength, opts); err != nil {
		if errors.Is(err, fetch.ErrHitHole) {
			fatal("manifest contains a hole; choose a policy with --hole=zero|punch (default --hole=error rejects)")
		}
		fatal("read: %v", err)
	}
	if !*noProgress {
		fmt.Fprintln(os.Stderr)
	}
	fmt.Fprintf(os.Stderr, "loaded %s from offset %d\n", formatSize(readLength), readOffset)
}

// ---------------------------------------------------------------------------
// get-manifest command — fetch the manifest blob (raw bytes) by key
// ---------------------------------------------------------------------------

func cmdGetManifest(args []string) {
	fs := flag.NewFlagSet("get-manifest", flag.ExitOnError)
	keyHex := fs.String("manifest-key", "", "hex content key (required)")
	output := fs.String("output", "-", "output file (- for stdout)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)
	if *keyHex == "" {
		fatal("--manifest-key is required")
	}
	cfg := loadCfg(*gf.configPath)

	key, err := manifest.ParseHexKey(*keyHex)
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
	manifestPath := fs.String("manifest", "-", "manifest file (- for stdin)")
	fs.Parse(args)

	data, err := readManifestData(*manifestPath)
	if err != nil {
		fatal("read manifest: %v", err)
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
	keyHex := fs.String("manifest-key", "", "hex content key of the manifest to verify (required)")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	gf := addGlobalFlags(fs)
	fs.Parse(args)
	if *keyHex == "" {
		fatal("--manifest-key is required")
	}
	cfg := loadCfg(*gf.configPath)

	fc, err := cfg.NewFetcher(cfg.FetchKeyFunc())
	if err != nil {
		fatal("fetcher: %v", err)
	}
	defer fc.Close()

	key, err := manifest.ParseHexKey(*keyHex)
	if err != nil {
		fatal("%v", err)
	}
	stream, err := fc.Fetch(context.Background(), key)
	if err != nil {
		fatal("fetch manifest: %v", err)
	}

	m := stream.Manifest()
	count := int(m.ChunkCount())
	verified, skipped, failed := 0, 0, 0
	ctx := context.Background()
	buf := make([]byte, m.MaxChunkSize)
	for i, entry := range m.Entries {
		if entry.IsZero {
			skipped++
			if !*noProgress {
				fmt.Fprintf(os.Stderr, "\rverify: %d/%d (skip zero)", i+1, count)
			}
			continue
		}
		// Read this chunk through the fetcher; the fetcher fetches,
		// decrypts, and validates the per-chunk key end-to-end.
		end := entry.Offset + uint64(entry.Size)
		if cap(buf) < int(entry.Size) {
			buf = make([]byte, entry.Size)
		}
		buf = buf[:entry.Size]
		_, err := stream.ReadAt(ctx, buf, entry.Offset)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nchunk %d: read failed: %v\n", i, err)
			failed++
			continue
		}
		_ = end
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
	if failed > 0 {
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

func makeHolePolicy(name string, w io.Writer) (func(io.Writer, uint64, uint64) error, error) {
	switch name {
	case "error":
		return nil, nil
	case "zero":
		return holeFillZero, nil
	case "punch":
		if f, ok := w.(*os.File); ok {
			return holeFillPunch(f), nil
		}
		return nil, fmt.Errorf("--hole=punch requires output to be a regular file (not stdout/pipe)")
	default:
		return nil, fmt.Errorf("unknown --hole policy %q", name)
	}
}

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
