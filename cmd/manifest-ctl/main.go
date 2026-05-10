// manifest-ctl provides a CLI for manifest lifecycle operations: ingest, fetch,
// store/retrieve manifests, inspect, verify, and diff.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
	"github.com/fullof-work/mass-sandbox/pkg/cache/client"
	"github.com/fullof-work/mass-sandbox/pkg/chunker"
	"github.com/fullof-work/mass-sandbox/pkg/config"
	"github.com/fullof-work/mass-sandbox/pkg/crypto"
	"github.com/fullof-work/mass-sandbox/pkg/fetch"
	"github.com/fullof-work/mass-sandbox/pkg/ingest"
	"github.com/fullof-work/mass-sandbox/pkg/manifest"
	"github.com/fullof-work/mass-sandbox/pkg/store"
	storeclient "github.com/fullof-work/mass-sandbox/pkg/store/client"
)

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
	case "put-manifest":
		cmdPutManifest(os.Args[2:])
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
  store          Ingest a file into the content store and produce a manifest
  load           Reconstruct (part of) an image from a manifest
  put-manifest   Store a manifest blob; outputs hex content key
  get-manifest   Retrieve a manifest blob by --key (hex content key)
  info           Display manifest metadata
  verify         Verify chunk integrity against a manifest
  diff           Compare two manifests and report shared/unique chunks
  config         Inspect or generate the manifest config file

Configuration:
  Every command (except 'config generate') needs a YAML config file.
  Provide it via --config <path> or the MANIFEST_CONFIG environment
  variable. Flag wins when both are set. There is no auto-discovery.
`)
}

// ---------------------------------------------------------------------------
// Global flags
// ---------------------------------------------------------------------------

type globalFlags struct {
	configPath *string
	cryptoFake *bool
}

func addGlobalFlags(fs *flag.FlagSet) globalFlags {
	var g globalFlags
	g.configPath = fs.String("config", "", "path to manifest config YAML (overrides MANIFEST_CONFIG env)")
	g.cryptoFake = fs.Bool("crypto-fake", false, "acknowledge use of fake encryption (required when crypto.chunk or crypto.manifest is fake)")
	return g
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

const manifestConfigEnv = "MANIFEST_CONFIG"

func loadConfig(configPath string) (*config.Config, error) {
	cfg, err := config.LoadFromFlagOrEnv(configPath, manifestConfigEnv)
	if err != nil {
		if errors.Is(err, config.ErrConfigNotProvided) {
			return nil, fmt.Errorf("missing manifest config: pass --config <path> or set %s", manifestConfigEnv)
		}
		return nil, err
	}
	return cfg, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

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
	// For stdin or non-regular files, size is unknown.
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

func chunkModeString(m manifest.ChunkMode) string {
	switch m {
	case manifest.ChunkModeFastCDC:
		return "cdc"
	case manifest.ChunkModeFixed:
		return "fixed"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// makeStoreClient dials store-ctl and returns a client that
// implements store.ContentStore. Callers MUST defer client.Close()
// to release the underlying gRPC connections.
//
// The endpoint is required (no longer falls back to a local
// filesystem path); error if cfg.Store.Endpoint is empty.
func makeStoreClient(cfg *config.Config) (*storeclient.Client, error) {
	if cfg.Store.Endpoint == "" {
		return nil, fmt.Errorf("config: store.endpoint is required (set it in the manifest config YAML)")
	}
	pool := cfg.Store.Pool
	if pool <= 0 {
		pool = 4
	}
	timeout := 5 * time.Second
	if cfg.Store.Timeout != "" {
		if d, err := time.ParseDuration(cfg.Store.Timeout); err == nil && d > 0 {
			timeout = d
		}
	}
	return storeclient.New(cfg.Store.Endpoint, pool, timeout)
}

// mixClientSalt combines the store-supplied salt with the caller's
// optional extra-salt (hex string from --salt). Empty extra means
// the server salt is used as-is.
func mixClientSalt(serverSalt [32]byte, extraSalt string) ([32]byte, error) {
	if extraSalt == "" {
		return serverSalt, nil
	}
	raw, err := hex.DecodeString(extraSalt)
	if err != nil {
		return [32]byte{}, fmt.Errorf("invalid --salt hex: %w", err)
	}
	h := sha256.New()
	h.Write(serverSalt[:])
	h.Write(raw)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

func makeChunkEncryptor(cfg *config.Config, cryptoFake bool) (crypto.ChunkEncryptor, error) {
	if cfg.Crypto.Chunk == "fake" && !cryptoFake {
		return nil, fmt.Errorf("crypto-chunk is \"fake\" but --crypto-fake was not set; pass --crypto-fake to acknowledge")
	}
	return crypto.NewChunkEncryptor(cfg.Crypto.Chunk)
}

func makeKeyTableEncryptor(cfg *config.Config, cryptoFake bool) (crypto.KeyTableEncryptor, error) {
	if cfg.Crypto.Manifest == "fake" && !cryptoFake {
		return nil, fmt.Errorf("crypto-manifest is \"fake\" but --crypto-fake was not set; pass --crypto-fake to acknowledge")
	}
	return crypto.NewKeyTableEncryptor(cfg.Crypto.Manifest)
}

func makeChunkerConfig(cfg *config.Config) (chunker.Config, error) {
	cc := chunker.DefaultConfig()
	switch cfg.Chunk.Mode {
	case "cdc":
		cc.Mode = chunker.ModeCDC
	case "fixed":
		cc.Mode = chunker.ModeFixed
	default:
		return cc, fmt.Errorf("unknown chunk mode %q", cfg.Chunk.Mode)
	}
	if cfg.Chunk.CDC.Min != "" {
		v, err := config.ParseSize(cfg.Chunk.CDC.Min)
		if err != nil {
			return cc, err
		}
		cc.CDCMinSize = uint32(v)
	}
	if cfg.Chunk.CDC.Avg != "" {
		v, err := config.ParseSize(cfg.Chunk.CDC.Avg)
		if err != nil {
			return cc, err
		}
		cc.CDCAvgSize = uint32(v)
	}
	if cfg.Chunk.CDC.Max != "" {
		v, err := config.ParseSize(cfg.Chunk.CDC.Max)
		if err != nil {
			return cc, err
		}
		cc.CDCMaxSize = uint32(v)
	}
	if cfg.Chunk.Fixed.Size != "" {
		v, err := config.ParseSize(cfg.Chunk.Fixed.Size)
		if err != nil {
			return cc, err
		}
		cc.FixedSize = uint32(v)
	}
	return cc, nil
}


// unsealKeys decrypts the sealed key table and returns per-chunk
// keys, expanded to a sparse N-length slice (N = total chunk count).
//
// The sealed table contains keys for non-zero entries only — zero
// chunks have no key (they're never encrypted). On unseal we compute
// M = count(entries where !IsZero), validate the flat-key length is
// M*32, and re-distribute keys to the right indices: zero slots get
// a zero [32]byte (never read by the load path; fetch.go:182 skips
// keys[idx] for IsZero entries).
func unsealKeys(m *manifest.Manifest, sealedKT []byte, customerKey [32]byte, kte crypto.KeyTableEncryptor) ([][32]byte, error) {
	flat, err := kte.Unseal(customerKey, sealedKT, manifest.BuildAAD(m))
	if err != nil {
		return nil, fmt.Errorf("unseal key table: %w", err)
	}
	count := int(m.ChunkCount())
	nonZero := 0
	for _, e := range m.Entries {
		if !e.IsZero {
			nonZero++
		}
	}
	if len(flat) != nonZero*32 {
		return nil, fmt.Errorf("key table size mismatch: got %d bytes, want %d (non-zero chunks: %d)", len(flat), nonZero*32, nonZero)
	}
	keys := make([][32]byte, count)
	pos := 0
	for i, e := range m.Entries {
		if e.IsZero {
			continue
		}
		copy(keys[i][:], flat[pos*32:(pos+1)*32])
		pos++
	}
	return keys, nil
}

// makeCacheReader creates a chunk reader for the load path.
// If cache endpoint is configured, returns a wire-protocol client to
// cache-ctl. Otherwise returns a direct store-ctl gRPC reader
// (bypassing the cache layer — useful when there's no cache on this
// node and manifest-ctl talks to store-ctl directly).
//
// Both branches return something that implements cache.Getter.
// The returned io.Closer must be Closed by the caller when done to
// release the underlying connection pool.
func makeCacheReader(cfg *config.Config) (cache.Getter, io.Closer, error) {
	if cfg.Cache.Endpoint != "" {
		timeout := 2 * time.Second
		if cfg.Cache.Timeout != "" {
			if d, err := time.ParseDuration(cfg.Cache.Timeout); err == nil && d > 0 {
				timeout = d
			}
		}
		pool := cfg.Cache.Pool
		if pool <= 0 {
			pool = 4
		}
		c, err := client.NewGetter(cfg.Cache.Endpoint, client.Options{
			Pool:    pool,
			Timeout: timeout,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("cache client: %w", err)
		}
		return c, c, nil
	}

	// Cache bypass: direct store-ctl reads. The store-ctl gRPC client
	// exposes a byte-level Get; NewStoreOrigin wraps it as a
	// cache.Getter suitable for fetch.NewFetcher.
	sc, err := makeStoreClient(cfg)
	if err != nil {
		return nil, nil, err
	}
	return cache.NewStoreOrigin(sc), sc, nil
}

// readManifestData reads manifest bytes from a file or stdin.
func readManifestData(path string) ([]byte, error) {
	if path == "" || path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

// makeHolePolicy translates the --hole flag into a fetch.OnHole
// callback. Each policy:
//
//	error  → nil callback. WriteTo will surface fetch.ErrHitHole.
//	zero   → write `size` zero bytes via io.CopyN.
//	punch  → fallocate(FALLOC_FL_PUNCH_HOLE | KEEP_SIZE) when w is a
//	         regular file with a usable Fd; falls back to zero on
//	         any other writer (stdout / pipe / non-file fd).
func makeHolePolicy(name string, w io.Writer) (func(io.Writer, uint64, uint64) error, error) {
	switch name {
	case "error":
		return nil, nil
	case "zero":
		return holeFillZero, nil
	case "punch":
		f, ok := w.(*os.File)
		if !ok || f == os.Stdout {
			fmt.Fprintln(os.Stderr, "warn: --hole=punch requires a regular file output; falling back to --hole=zero")
			return holeFillZero, nil
		}
		return holeFillPunch(f), nil
	default:
		return nil, fmt.Errorf("unknown --hole policy %q (want: error, zero, punch)", name)
	}
}

// holeFillZero writes `size` zero bytes via io.CopyN, the universal
// fallback usable on any io.Writer.
func holeFillZero(w io.Writer, _, size uint64) error {
	if size == 0 {
		return nil
	}
	if _, err := io.CopyN(w, zeroReader{}, int64(size)); err != nil {
		return fmt.Errorf("hole zero-fill: %w", err)
	}
	return nil
}

// zeroReader is an infinite source of zero bytes; used by holeFillZero.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// putManifestBlob content-addresses the manifest bytes (SHA-256) and
// Puts them into the manifest partition of the store. Shared by the
// `put-manifest` subcommand and the `store --put-manifest` short form.
func putManifestBlob(ctx context.Context, s *storeclient.Client, data []byte) (store.ContentKey, error) {
	key := store.ContentKey(sha256.Sum256(data))
	if _, err := s.Put(ctx, store.PartitionManifest, key, data); err != nil {
		return store.ContentKey{}, fmt.Errorf("put manifest: %w", err)
	}
	return key, nil
}

// getManifestBlob decodes a hex content key and fetches the manifest
// bytes from the cache/store reader. Shared by the `get-manifest`
// subcommand and the `load --get-manifest` short form.
//
// The returned blob must be Released by the caller (so the cache's
// BlobPool can reclaim it). We hand back the underlying cache.Blob
// rather than a copy to avoid a needless allocation on the hot path;
// callers that need long-lived ownership should copy the bytes before
// releasing.
func getManifestBlob(ctx context.Context, reader cache.Getter, keyHex string) (cache.Blob, error) {
	keyBytes, err := hex.DecodeString(keyHex)
	if err != nil || len(keyBytes) != 32 {
		return nil, fmt.Errorf("invalid key: must be 32-byte hex string")
	}
	var key store.ContentKey
	copy(key[:], keyBytes)

	result, blob, err := reader.Get(ctx, store.PartitionManifest, key)
	if err != nil {
		return nil, fmt.Errorf("get manifest: %w", err)
	}
	if result != cache.CacheHit {
		return nil, fmt.Errorf("manifest not found: %s", keyHex)
	}
	return blob, nil
}

// ---------------------------------------------------------------------------
// store command
// ---------------------------------------------------------------------------

func cmdStore(args []string) {
	fs := flag.NewFlagSet("store", flag.ExitOnError)
	input := fs.String("input", "-", "input file (- for stdin)")
	manifestOut := fs.String("manifest", "-", "output manifest file (- for stdout); ignored when --put-manifest is set")
	putManifest := fs.Bool("put-manifest", false, "store the produced manifest into the store and emit its hex content key on stdout (replaces --manifest output)")
	salt := fs.String("salt", "", "extra salt string")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	detectHoles := fs.Bool("detect-holes", false, "detect filesystem holes in --input via lseek(SEEK_HOLE/SEEK_DATA) and record them as HoleExtents (file input only — ignored for stdin)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	cfg, err := loadConfig(*gf.configPath)
	if err != nil {
		fatal("load config: %v", err)
	}

	// Open input.
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
	if size == 0 && (in == os.Stdin || *input == "-" || *input == "") {
		// Reading from stdin: buffer everything to determine size.
		data, err := io.ReadAll(in)
		if err != nil {
			fatal("read stdin: %v", err)
		}
		size = uint64(len(data))
		// Replace in with a temporary file so the ingester can read from it.
		tmpFile, err := os.CreateTemp("", "manifest-ctl-store-*")
		if err != nil {
			fatal("create temp: %v", err)
		}
		defer os.Remove(tmpFile.Name())
		if _, err := tmpFile.Write(data); err != nil {
			tmpFile.Close()
			fatal("write temp: %v", err)
		}
		if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
			tmpFile.Close()
			fatal("seek temp: %v", err)
		}
		in = tmpFile
	}

	// Open a gRPC client to store-ctl. The client implements
	// store.ContentStore, so ingest can use it as its storage sink
	// without knowing anything about gRPC.
	s, err := makeStoreClient(cfg)
	if err != nil {
		fatal("create store client: %v", err)
	}
	defer s.Close()

	// Fetch the server's active-generation salt. Clients combine
	// this with any extra --salt bytes they control to derive the
	// convergent-encryption seed for chunk keys.
	_, serverSalt, err := s.GetSalt(context.Background())
	if err != nil {
		fatal("store.GetSalt: %v", err)
	}
	derivedSalt, err := mixClientSalt(serverSalt, *salt)
	if err != nil {
		fatal("%v", err)
	}
	_ = salt // referenced via derivedSalt

	// Create encryptors.
	chunkEnc, err := makeChunkEncryptor(cfg, *gf.cryptoFake)
	if err != nil {
		fatal("%v", err)
	}
	ktEnc, err := makeKeyTableEncryptor(cfg, *gf.cryptoFake)
	if err != nil {
		fatal("%v", err)
	}

	// Chunker config.
	cc, err := makeChunkerConfig(cfg)
	if err != nil {
		fatal("chunker config: %v", err)
	}

	// Customer key.
	customerKey, err := cfg.CustomerKey()
	if err != nil {
		fatal("customer key: %v", err)
	}

	// Progress callback.
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

	// Detect filesystem holes in the input if requested. Only
	// meaningful for regular files (in is *os.File at this point —
	// stdin was already buffered to a temp file above). Stdin /
	// pipe inputs that bypass the buffering branch can't be detected
	// post-hoc; warn explicitly so the user knows the flag was a no-op.
	var detectedHoles []manifest.HoleExtent
	if *detectHoles {
		if fi, _ := in.Stat(); fi != nil && fi.Mode().IsRegular() {
			detectedHoles, err = manifest.DetectHoles(in, size)
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

	// Ingest.
	ing := ingest.NewIngester(s.Put, chunkEnc, ktEnc)
	ctx := context.Background()
	result, err := ing.Ingest(ctx, in, size, ingest.Config{
		CustomerKey: customerKey,
		Salt:        derivedSalt,
		ChunkConfig: cc,
		OnProgress:  onProgress,
		Holes:       detectedHoles,
	})
	if err != nil {
		fatal("ingest: %v", err)
	}

	if !*noProgress {
		fmt.Fprintf(os.Stderr, "\n")
	}

	// Marshal manifest.
	data, err := manifest.Marshal(result.Manifest, result.SealedKeyTable)
	if err != nil {
		fatal("marshal manifest: %v", err)
	}

	// Sink selection: either Put the manifest into the store (short
	// form, equivalent to `store | put-manifest`) or write it to a
	// file / stdout. When --put-manifest is set the --manifest path
	// is silently ignored.
	var manifestKey store.ContentKey
	if *putManifest {
		manifestKey, err = putManifestBlob(ctx, s, data)
		if err != nil {
			fatal("%v", err)
		}
		fmt.Fprintln(os.Stdout, hex.EncodeToString(manifestKey[:]))
	} else {
		out, err := createOutput(*manifestOut)
		if err != nil {
			fatal("create output: %v", err)
		}
		defer func() {
			if out != os.Stdout {
				out.Close()
			}
		}()
		if _, err := out.Write(data); err != nil {
			fatal("write manifest: %v", err)
		}
	}

	// Summary to stderr.
	fmt.Fprintf(os.Stderr, "image size:     %s\n", formatSize(result.Manifest.ImageSize))
	fmt.Fprintf(os.Stderr, "chunks:         %d (stored %d, dedup %d, zero %d)\n",
		result.Manifest.ChunkCount(), result.StoredChunks, result.DedupChunks, result.ZeroChunks)
	if n := len(result.Manifest.Holes); n > 0 {
		var holeBytes uint64
		for _, h := range result.Manifest.Holes {
			holeBytes += h.Size
		}
		fmt.Fprintf(os.Stderr, "holes:          %d (%s)\n", n, formatSize(holeBytes))
	}
	fmt.Fprintf(os.Stderr, "stored bytes:   %s\n", formatSize(result.StoredBytes))
	fmt.Fprintf(os.Stderr, "manifest bytes: %d\n", len(data))
	if *putManifest {
		fmt.Fprintf(os.Stderr, "manifest key:   %s\n", hex.EncodeToString(manifestKey[:]))
	}
}

// ---------------------------------------------------------------------------
// load command
// ---------------------------------------------------------------------------

func cmdLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	manifestPath := fs.String("manifest", "-", "manifest file (- for stdin); ignored when --get-manifest is set")
	getManifest := fs.String("get-manifest", "", "hex content key; when set, fetches the manifest from the store instead of reading a file (replaces --manifest input)")
	output := fs.String("output", "-", "output file (- for stdout)")
	offset := fs.Uint64("offset", 0, "byte offset to start reading")
	length := fs.Uint64("length", 0, "number of bytes to read (0 = remainder)")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	holePolicy := fs.String("hole", "error", "hole policy: error (default, fail on any hole), zero (fill with zero bytes), punch (fallocate PUNCH_HOLE on file output)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	cfg, err := loadConfig(*gf.configPath)
	if err != nil {
		fatal("load config: %v", err)
	}

	// Cache/store reader: shared between manifest fetch (when
	// --get-manifest is set) and chunk fetch later in this function,
	// so we open it once here.
	reader, readerCloser, err := makeCacheReader(cfg)
	if err != nil {
		fatal("create cache reader: %v", err)
	}
	defer readerCloser.Close()

	// Source selection: either GET the manifest by key from the store
	// (short form, equivalent to `get-manifest | load`) or read from a
	// file / stdin. When --get-manifest is set the --manifest path is
	// silently ignored.
	var mData []byte
	if *getManifest != "" {
		blob, err := getManifestBlob(context.Background(), reader, *getManifest)
		if err != nil {
			fatal("%v", err)
		}
		// Copy out: Unmarshal + downstream fetcher outlive the blob,
		// and the underlying pool may recycle the buffer after Release.
		mData = append([]byte(nil), blob.Bytes()...)
		blob.Release()
	} else {
		mData, err = readManifestData(*manifestPath)
		if err != nil {
			fatal("read manifest: %v", err)
		}
	}

	m, sealedKT, err := manifest.Unmarshal(mData)
	if err != nil {
		fatal("unmarshal manifest: %v", err)
	}

	// Unseal key table.
	customerKey, err := cfg.CustomerKey()
	if err != nil {
		fatal("customer key: %v", err)
	}

	ktEnc, err := makeKeyTableEncryptor(cfg, *gf.cryptoFake)
	if err != nil {
		fatal("%v", err)
	}

	keys, err := unsealKeys(m, sealedKT, customerKey, ktEnc)
	if err != nil {
		fatal("unseal keys: %v", err)
	}

	// Encryptor for decryption.
	chunkEnc, err := makeChunkEncryptor(cfg, *gf.cryptoFake)
	if err != nil {
		fatal("%v", err)
	}

	// reader / readerCloser opened earlier (at the top of the
	// function) so --get-manifest can share it.
	fetcher := fetch.NewFetcher(m, keys, reader, chunkEnc)

	// Determine read range.
	readOffset := *offset
	readLength := *length
	if readLength == 0 {
		if m.ImageSize > readOffset {
			readLength = m.ImageSize - readOffset
		}
	}
	if readOffset >= m.ImageSize {
		fatal("offset %d is at or beyond image size %d", readOffset, m.ImageSize)
	}
	if readOffset+readLength > m.ImageSize {
		readLength = m.ImageSize - readOffset
	}

	// Open output.
	out, err := createOutput(*output)
	if err != nil {
		fatal("create output: %v", err)
	}
	defer func() {
		if out != os.Stdout {
			out.Close()
		}
	}()

	// Stream output via concurrent fetcher.
	ctx := context.Background()

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

	if err := fetcher.WriteTo(ctx, out, readOffset, readLength, opts); err != nil {
		if errors.Is(err, fetch.ErrHitHole) {
			fatal("manifest contains a hole; choose a policy with --hole=zero|punch (default --hole=error rejects)")
		}
		fatal("fetch: %v", err)
	}

	if !*noProgress {
		fmt.Fprintf(os.Stderr, "\n")
	}
	fmt.Fprintf(os.Stderr, "loaded %s from offset %d\n", formatSize(readLength), readOffset)
}

// ---------------------------------------------------------------------------
// put-manifest command
// ---------------------------------------------------------------------------

func cmdPutManifest(args []string) {
	fs := flag.NewFlagSet("put-manifest", flag.ExitOnError)
	input := fs.String("input", "-", "manifest file to store (- for stdin)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	cfg, err := loadConfig(*gf.configPath)
	if err != nil {
		fatal("load config: %v", err)
	}

	data, err := readManifestData(*input)
	if err != nil {
		fatal("read input: %v", err)
	}

	s, err := makeStoreClient(cfg)
	if err != nil {
		fatal("create store client: %v", err)
	}
	defer s.Close()

	key, err := putManifestBlob(context.Background(), s, data)
	if err != nil {
		fatal("%v", err)
	}

	// Output the hex-encoded content key (mirrors how chunk operations identify content).
	fmt.Fprintln(os.Stdout, hex.EncodeToString(key[:]))
}

// ---------------------------------------------------------------------------
// get-manifest command
// ---------------------------------------------------------------------------

func cmdGetManifest(args []string) {
	fs := flag.NewFlagSet("get-manifest", flag.ExitOnError)
	keyHex := fs.String("key", "", "hex-encoded content key (required)")
	output := fs.String("output", "-", "output file (- for stdout)")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	if *keyHex == "" {
		fmt.Fprintf(os.Stderr, "error: --key is required\n")
		fs.Usage()
		os.Exit(1)
	}

	cfg, err := loadConfig(*gf.configPath)
	if err != nil {
		fatal("load config: %v", err)
	}

	reader, readerCloser, err := makeCacheReader(cfg)
	if err != nil {
		fatal("create cache reader: %v", err)
	}
	defer readerCloser.Close()

	blob, err := getManifestBlob(context.Background(), reader, *keyHex)
	if err != nil {
		fatal("%v", err)
	}
	defer blob.Release()

	out, err := createOutput(*output)
	if err != nil {
		fatal("create output: %v", err)
	}
	defer func() {
		if out != os.Stdout {
			out.Close()
		}
	}()

	if _, err := out.Write(blob.Bytes()); err != nil {
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

	m, sealedKT, err := manifest.Unmarshal(data)
	if err != nil {
		fatal("unmarshal manifest: %v", err)
	}

	count := m.ChunkCount()

	// Aggregate per-entry stats: total bytes, zero count, zero bytes.
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

	// Chunk-size distribution over data chunks (excludes zero chunks
	// whose Size is synthetic). Surfaces CDC quality signals: if
	// many chunks pile at the configured max, the chunker is making
	// forced cuts (poor boundary discovery → poor dedup).
	printChunkDistribution(m.Entries, m.MinChunkSize, m.MaxChunkSize)

	fmt.Printf("key table:     %d bytes (sealed)\n", len(sealedKT))
	fmt.Printf("manifest size: %d bytes\n", len(data))
}

// printChunkDistribution renders a single-line percentile table for
// data-chunk sizes:
//
//	min(N)  P1  P5  P25  P50  P75  P95  P99  max(N)
//
// where N at min/max is the count of chunks pinned at that exact
// size. The (P25, P75) inter-quartile range and (P5, P95) outer
// span give a quick read on distribution shape; pile-ups at the
// configured CDC max get a follow-up callout because they signal
// poor boundary discovery (forced cuts → poor dedup).
//
// Zero-chunks are excluded — their Size is synthetic, no signal.
// Empty input produces no output (existing min/max/avg lines remain).
//
// configuredMin/configuredMax gate the "forced cuts" callout so
// fixed-chunking (where every chunk is exactly the configured size
// by design) doesn't trigger a meaningless warning.
func printChunkDistribution(entries []manifest.ChunkEntry, configuredMin, configuredMax uint32) {
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

// percentileU32 returns the p-th percentile of sorted with simple
// nearest-rank semantics. p ∈ [0, 1].
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

// shortSize is a compact size formatter for tabular use: 64K / 1.5M /
// 2.0G — about half the width of formatSize. Keeps the percentile
// row narrow enough to fit on a typical 120-char terminal.
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
	manifestPath := fs.String("manifest", "-", "manifest file (- for stdin)")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	gf := addGlobalFlags(fs)
	fs.Parse(args)

	cfg, err := loadConfig(*gf.configPath)
	if err != nil {
		fatal("load config: %v", err)
	}

	// Read manifest.
	mData, err := readManifestData(*manifestPath)
	if err != nil {
		fatal("read manifest: %v", err)
	}

	m, sealedKT, err := manifest.Unmarshal(mData)
	if err != nil {
		fatal("unmarshal manifest: %v", err)
	}

	// Unseal key table.
	customerKey, err := cfg.CustomerKey()
	if err != nil {
		fatal("customer key: %v", err)
	}

	ktEnc, err := makeKeyTableEncryptor(cfg, *gf.cryptoFake)
	if err != nil {
		fatal("%v", err)
	}

	keys, err := unsealKeys(m, sealedKT, customerKey, ktEnc)
	if err != nil {
		fatal("unseal keys: %v", err)
	}

	// Encryptor for re-encrypt verification.
	chunkEnc, err := makeChunkEncryptor(cfg, *gf.cryptoFake)
	if err != nil {
		fatal("%v", err)
	}

	// Store for fetching ciphertext.
	s, err := makeStoreClient(cfg)
	if err != nil {
		fatal("create store client: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	count := int(m.ChunkCount())
	var verified, skipped, failed int

	for i, entry := range m.Entries {
		if entry.IsZero {
			skipped++
			if !*noProgress {
				fmt.Fprintf(os.Stderr, "\rverify: %d/%d (skip zero)", i+1, count)
			}
			continue
		}

		// Fetch ciphertext from store.
		found, ciphertext, err := s.Get(ctx, store.PartitionChunk, store.ContentKey(entry.CiphertextHash))
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nchunk %d: fetch failed: %v\n", i, err)
			failed++
			continue
		}
		if !found {
			fmt.Fprintf(os.Stderr, "\nchunk %d: not found in store\n", i)
			failed++
			continue
		}

		// Decrypt to verify the per-chunk key is correct.
		_, err = chunkEnc.Decrypt(keys[i], ciphertext)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nchunk %d: decrypt failed: %v\n", i, err)
			failed++
			continue
		}

		verified++
		if !*noProgress {
			fmt.Fprintf(os.Stderr, "\rverify: %d/%d", i+1, count)
		}
	}

	if !*noProgress {
		fmt.Fprintf(os.Stderr, "\n")
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

	pathA := args[0]
	pathB := args[1]

	dataA, err := os.ReadFile(pathA)
	if err != nil {
		fatal("read manifest-a: %v", err)
	}
	dataB, err := os.ReadFile(pathB)
	if err != nil {
		fatal("read manifest-b: %v", err)
	}

	mA, _, err := manifest.Unmarshal(dataA)
	if err != nil {
		fatal("unmarshal manifest-a: %v", err)
	}
	mB, _, err := manifest.Unmarshal(dataB)
	if err != nil {
		fatal("unmarshal manifest-b: %v", err)
	}

	// Build hash sets (excluding zero chunks).
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

	// Count shared, only-A, only-B.
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

	// Unique hashes for dedup ratio.
	uniqueA := len(setA)
	uniqueB := len(setB)

	// Merged unique set.
	merged := make(map[[32]byte]struct{})
	for h := range setA {
		merged[h] = struct{}{}
	}
	for h := range setB {
		merged[h] = struct{}{}
	}
	uniqueMerged := len(merged)

	// Dedup ratio: 1 - (unique merged / (uniqueA + uniqueB)).
	totalUnique := uniqueA + uniqueB
	var dedupRatio float64
	if totalUnique > 0 {
		dedupRatio = 1.0 - float64(uniqueMerged)/float64(totalUnique)
	}

	// Hole summaries are independent of the chunk-level dedup view —
	// holes contain no data and thus contribute nothing to the
	// shared/unique chunk hash set. Reported for visibility only.
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
