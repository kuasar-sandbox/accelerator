// manifest-ctl provides a CLI for manifest lifecycle operations:
// ingest, fetch, retrieve manifest blobs, inspect, verify, diff.
package main

import (
	"context"
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
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/tailzip"
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
  Commands using remote data or configuration read YAML through
  --manifest-config <path> or MANIFEST_CONFIG. Flag wins; no auto-discovery.
  'config generate', local/stdin 'info', and local-file 'diff' need no config.
  For remote 'info', use manifest://<hex>; bare hex is a local filename.
  The sensitive manifest.key may be omitted from the file and supplied
  via nonempty MANIFEST_KEY instead (a nonempty value overrides the file).
  'config show' does not echo the environment key, but can print a key
  explicitly stored in YAML.
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

func localTarStreamCodec(cfg *manifest.Config, customerKey [32]byte) (tarstream.Codec, bool, error) {
	policy, err := cfg.Crypto.LocalPolicy()
	if err != nil {
		return nil, false, err
	}
	if policy == manifestcrypto.LocalOff {
		return nil, false, nil
	}
	codec, err := manifestcrypto.NewTarStreamCodec(customerKey)
	if err != nil {
		return nil, false, err
	}
	return codec, policy == manifestcrypto.LocalRequired, nil
}

func localReadOptions(codec tarstream.Codec, required bool) []tarstream.ReadOption {
	if codec == nil {
		return nil
	}
	return []tarstream.ReadOption{tarstream.WithCodec(codec, required)}
}

func localWriteOptions(codec tarstream.Codec, required bool) []tarstream.WriteOption {
	if codec == nil {
		return nil
	}
	return []tarstream.WriteOption{tarstream.WithCodec(codec, required)}
}

// ---------------------------------------------------------------------------
// store command
// ---------------------------------------------------------------------------

func cmdStore(args []string) { cmdStoreTransform(args) }

// ---------------------------------------------------------------------------
// load command
// ---------------------------------------------------------------------------

func cmdLoad(args []string) {
	cmdLoadTransform(args)
}

// windowSource exposes [base, base+size) of s as a source of its own.
type windowSource struct {
	s          sparse.Source
	base, size uint64
}

func (w *windowSource) Size() uint64 { return w.size }

func (w *windowSource) RunAt(off, limit uint64) (sparse.Run, error) {
	return (tailzip.Section{Source: w.s, Base: w.base, Length: w.size}).RunAt(off, limit)
}
func (w *windowSource) ReadAt(ctx context.Context, buf []byte, off uint64) (int, error) {
	return (tailzip.Section{Source: w.s, Base: w.base, Length: w.size}).ReadAt(ctx, buf, off)
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

	fc, err := cfg.NewFetcherWithOptions(manifest.FetchOptions{VerifyContent: true})
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
	data, derr := cfg.GetManifestBlobWithOptions(ctx, key, manifest.FetchOptions{VerifyContent: true})
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
	setA := uniqueChunkSizes(mA)
	setB := uniqueChunkSizes(mB)
	var shared, onlyA, onlyB int
	var sharedBytes, onlyABytes, onlyBBytes uint64
	for hash, size := range setA {
		if sizeB, ok := setB[hash]; ok {
			if size != sizeB {
				fatal("chunk %x has conflicting sizes: %d and %d", hash, size, sizeB)
			}
			shared++
			sharedBytes += uint64(size)
		} else {
			onlyA++
			onlyABytes += uint64(size)
		}
	}
	for hash, size := range setB {
		if _, ok := setA[hash]; !ok {
			onlyB++
			onlyBBytes += uint64(size)
		}
	}
	uniqueA := len(setA)
	uniqueB := len(setB)
	uniqueMerged := shared + onlyA + onlyB
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

// Diff compares content-key sets, not repeated positions in an image. Zero
// entries have no Store object; sizes here are logical, not encoded bytes.
func uniqueChunkSizes(m *codec.Manifest) map[[32]byte]uint32 {
	set := make(map[[32]byte]uint32)
	for _, entry := range m.Entries {
		if entry.IsZero {
			continue
		}
		if size, ok := set[entry.CiphertextHash]; ok && size != entry.Size {
			fatal("chunk %x has conflicting sizes: %d and %d", entry.CiphertextHash, size, entry.Size)
		}
		set[entry.CiphertextHash] = entry.Size
	}
	return set
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
