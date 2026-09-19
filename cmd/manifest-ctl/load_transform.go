package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/transfer"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tailzip"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

const maxTransferTail = 64 << 20

type locationFlags map[string]string

func (m *locationFlags) String() string { return manifest.RefLocations(*m).String() }
func (m *locationFlags) Set(value string) error {
	if *m == nil {
		*m = locationFlags{}
	}
	return manifest.RefLocations(*m).Set(value)
}

type transferIO struct {
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}

func defaultTransferIO() transferIO { return transferIO{os.Stdin, os.Stdout, os.Stderr} }
func cmdLoadTransform(args []string) {
	if err := runTransfer(context.Background(), args, false, defaultTransferIO()); err != nil {
		fatal("load: %v", err)
	}
}
func cmdStoreTransform(args []string) {
	if err := runTransfer(context.Background(), args, true, defaultTransferIO()); err != nil {
		fatal("store: %v", err)
	}
}

// runTransfer owns every input and output until validation has completed.
// Returning errors here lets cleanup run before the command-level exit.
func runTransfer(ctx context.Context, args []string, storeCommand bool, streams transferIO) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	command := "load"
	if storeCommand {
		command = "store"
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(streams.Stderr)
	var output, mode, name string
	var storeOut bool
	var offset, length uint64
	if !storeCommand {
		fs.StringVar(&output, "output", "-", "output file (- for stdout)")
		fs.StringVar(&mode, "output-mode", "", "none, tarstream, or bundle")
		fs.StringVar(&name, "name", "image", "tarstream entry name")
		fs.BoolVar(&storeOut, "store", false, "also store final logical content")
		fs.Uint64Var(&offset, "offset", 0, "offset after layering")
		fs.Uint64Var(&length, "length", 0, "length (0 = remainder)")
	} else {
		output = "-"
		mode = "none"
		name = "image"
		storeOut = true
	}
	extra := fs.String("extra-salt", "", "extra bytes for Bundle/Store key derivation")
	strip := fs.Bool("strip-tail", false, "strip each input tail before layering")
	extract := fs.String("extract-tail", "", "save the original first input tail")
	appendPath := fs.String("append-tail", "", "append a validated tail after the window")
	noProgress := fs.Bool("no-progress", false, "suppress progress output")
	locations := locationFlags{}
	fs.Var(&locations, "ref-location", "name=file:///absolute/path (repeatable)")
	gf := addGlobalFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	refs := fs.Args()
	if storeCommand && len(refs) == 0 {
		refs = []string{"-"}
	}
	if len(refs) == 0 || (storeCommand && len(refs) != 1) {
		return fmt.Errorf("%s: invalid number of input references", command)
	}
	if mode == "" {
		mode = "tarstream"
		if storeOut {
			mode = "none"
		}
	}
	if mode != "none" && mode != "tarstream" && mode != "bundle" {
		return fmt.Errorf("invalid --output-mode %q", mode)
	}
	if mode == "none" && !storeOut && *extract == "" {
		return errors.New("none output requires --store or --extract-tail")
	}
	if *extra != "" && mode != "bundle" && !storeOut {
		return errors.New("extra-salt requires Bundle output or --store")
	}
	if *extract == "-" {
		return errors.New("extract-tail requires a file path")
	}
	if mode == "none" && output != "-" && output != "" {
		return errors.New("none output cannot use --output")
	}
	if mode != "none" && (output == "-" || output == "") {
		if f, ok := streams.Stdout.(*os.File); ok {
			if info, err := f.Stat(); err != nil {
				return err
			} else if info.Mode()&os.ModeCharDevice != 0 {
				return errors.New("refusing binary output to a terminal; use --output")
			}
		}
	}
	outputPaths := []string{}
	if mode != "none" && output != "-" && output != "" {
		outputPaths = append(outputPaths, output)
	}
	if *extract != "" {
		outputPaths = append(outputPaths, *extract)
	}
	for i, path := range outputPaths {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("output already exists: %s: %w", path, os.ErrExist)
		} else if !os.IsNotExist(err) {
			return err
		}
		for _, prior := range outputPaths[:i] {
			if samePath(path, prior) {
				return errors.New("output paths alias each other")
			}
		}
		if *appendPath != "" && samePath(path, *appendPath) {
			return errors.New("output aliases append-tail input")
		}
	}
	var newTail []byte
	if *appendPath != "" {
		var err error
		newTail, err = readBoundedTail(*appendPath, maxTransferTail)
		if err != nil {
			return err
		}
		if err = tailzip.Validate(newTail, tailzip.Options{}); err != nil {
			return fmt.Errorf("append tail: %w", err)
		}
	}
	cfg, err := manifest.LoadConfig(*gf.configPath, manifestConfigEnv)
	if err != nil {
		return err
	}
	customer, err := cfg.CustomerKey()
	if err != nil {
		return err
	}
	defer clear(customer[:])
	reader, err := transfer.NewReader(cfg, customer, manifest.RefLocations(locations))
	if err != nil {
		return err
	}
	localCodec, localRequired := reader.LocalCodec()
	var opened []*transfer.Opened
	var outputFinishes []func(error) error
	sourceClosed := false
	closeSources := func() error {
		if sourceClosed {
			return nil
		}
		sourceClosed = true
		var err error
		for i := len(opened) - 1; i >= 0; i-- {
			err = errors.Join(err, opened[i].Close())
		}
		return errors.Join(err, reader.Close())
	}
	defer func() {
		retErr = errors.Join(retErr, closeSources())
		for i := len(outputFinishes) - 1; i >= 0; i-- {
			retErr = errors.Join(retErr, outputFinishes[i](retErr))
		}
	}()
	var src sparse.Source
	var firstTail []byte
	var sequential *capturedInput
	layers := make([]fetch.Stream, 0, len(refs))
	for i, raw := range refs {
		if raw == "-" {
			if len(refs) != 1 {
				return errors.New("stdin must be the sole input")
			}
			original, _, err := tarstream.SourceFrom(streams.Stdin, "", reader.ReadOptions()...)
			if err != nil {
				return fmt.Errorf("stdin tarstream: %w", err)
			}
			sequential, err = newCapturedInput(original, (*strip || *extract != "" || *appendPath != ""))
			if err != nil {
				return err
			}
			if *appendPath != "" && sequential.boundary < original.Size() && !*strip {
				return errors.New("append-tail on an existing tail requires --strip-tail")
			}
			src = sequential
			if *strip {
				src, err = tailzip.Prefix(src, sequential.boundary)
				if err != nil {
					return err
				}
			}
			break
		}
		layer, err := reader.Open(ctx, raw)
		if err != nil {
			return fmt.Errorf("open %q: %w", raw, err)
		}
		opened = append(opened, layer)
		for _, path := range outputPaths {
			if layer.Path() != "" && samePath(path, layer.Path()) {
				return errors.New("output aliases input")
			}
		}
		view := sparse.Source(layer)
		if *strip || *extract != "" && i == 0 || *appendPath != "" {
			bounds, err := tailzip.Locate(sourceReaderAt{ctx, layer}, int64(layer.Size()), tailzip.Options{})
			if err != nil && !errors.Is(err, tailzip.ErrNotFound) {
				return fmt.Errorf("input tail: %w", err)
			}
			if errors.Is(err, tailzip.ErrNotFound) {
				boundary, _, _ := layer.PayloadCommitment()
				if boundary < layer.Size() {
					return errors.New("declared metadata tail is missing or malformed")
				}
			}

			if err == nil {
				boundary, _, _ := layer.PayloadCommitment()
				if boundary < layer.Size() && uint64(bounds.Offset) != boundary {
					return errors.New("ZIP tail disagrees with declared payload boundary")
				}
				if *appendPath != "" && !*strip {
					return errors.New("append-tail on an existing tail requires --strip-tail")
				}
				if i == 0 && *extract != "" {
					if bounds.Size > int64(maxTransferTail-len(newTail)) {
						return errors.New("retained tails exceed memory budget")
					}
					firstTail = make([]byte, bounds.Size)
					if n, err := layer.ReadAt(ctx, firstTail, uint64(bounds.Offset)); err != nil {
						return err
					} else if n != len(firstTail) {
						return io.ErrUnexpectedEOF
					}
				}
				if *strip {
					view, err = tailzip.Prefix(layer, uint64(bounds.Offset))
					if err != nil {
						return err
					}
				}
			}
		}
		layers = append(layers, &sourceStream{Source: view})
	}
	if sequential == nil {
		if len(layers) == 1 {
			src = layers[0]
		} else {
			src = fetch.NewLayered(layers...)
		}
		if *extract != "" && len(firstTail) == 0 {
			return errors.New("first input has no tail")
		}
	} else if len(newTail)+len(sequential.tail) > maxTransferTail {
		return errors.New("retained tails exceed memory budget")
	}
	if offset > src.Size() || offset == src.Size() && src.Size() != 0 {
		return fmt.Errorf("offset %d is at or beyond size %d", offset, src.Size())
	}
	size := length
	if size == 0 || size > src.Size()-offset {
		size = src.Size() - offset
	}
	if offset != 0 || size != src.Size() {
		src = &windowSource{s: src, base: offset, size: size}
	}
	if len(newTail) > 0 {
		src, err = tailzip.Append(src, newTail, tailzip.Options{})
		if err != nil {
			return err
		}
	}
	// A Manifest stores logical bytes/holes, not its former carrier boundary.
	// Reconstruct the split of a valid relative suffix for an unchanged root.
	// Existing tarstream declarations, explicit windows and layer transforms
	// retain their own authoritative boundaries.
	if mode == "tarstream" && sequential == nil && len(opened) == 1 && !*strip && len(newTail) == 0 && offset == 0 && size == opened[0].Size() {
		if _, declared := opened[0].Source.(tarstream.IdentityProvider); !declared {
			probe := &optionalTailReader{ctx: ctx, source: src}
			tail, tailErr := tailzip.Locate(probe, int64(src.Size()), tailzip.Options{})
			if probe.err != nil {
				return probe.err
			}
			if tailErr == nil {
				src = payloadBoundary{Source: src, boundary: uint64(tail.Offset)}
			}
		}
	}
	var outputWriter io.Writer
	if mode != "none" {
		if output == "-" || output == "" {
			outputWriter = streams.Stdout
		} else {
			var finish func(error) error
			outputWriter, finish, err = openDirectOutput(output)
			if err != nil {
				return err
			}
			outputFinishes = append(outputFinishes, finish)
		}
	}
	var tailWriter io.Writer
	if *extract != "" {
		var finish func(error) error
		tailWriter, finish, err = openDirectOutput(*extract)
		if err != nil {
			return err
		}
		outputFinishes = append(outputFinishes, finish)
	}
	finishInput := func() error {
		if sequential != nil {
			if err := sequential.Finish(ctx); err != nil {
				return err
			}
			if *strip || *extract != "" || *appendPath != "" {
				if len(sequential.tail) > 0 {
					if err := tailzip.Validate(sequential.tail, tailzip.Options{}); err != nil {
						return err
					}
				}
			}
			firstTail = sequential.tail
		}
		if storeOut || mode != "none" {
			for _, layer := range opened {
				if err := layer.Verify(ctx); err != nil {
					return err
				}
			}
		}
		if tailWriter != nil {
			if len(firstTail) == 0 {
				return errors.New("first input has no tail")
			}
			if err := writeAll(tailWriter, firstTail); err != nil {
				return err
			}
		}
		if err := closeSources(); err != nil {
			return err
		}
		for _, finish := range outputFinishes {
			if err := finish(nil); err != nil {
				return err
			}
		}
		return nil
	}
	opts := transfer.WriteOptions{Mode: mode, Output: outputWriter, Name: name, Store: storeOut, ExtraSalt: []byte(*extra), Codec: localCodec, RequireLocalEncryption: localRequired, BeforeCommit: finishInput}
	if !*noProgress {
		opts.OnProgress = func(processed, total uint64) {
			fmt.Fprintf(streams.Stderr, "\r%s: %s / %s", command, formatSize(processed), formatSize(total))
		}
	}
	result, err := transfer.Write(ctx, cfg, customer, src, opts)
	if err != nil {
		return err
	}
	if storeOut {
		target := streams.Stdout
		if mode != "none" && (output == "-" || output == "") {
			target = streams.Stderr
		}
		if _, err := fmt.Fprintln(target, manifest.HexKey(result.StoreKey)); err != nil {
			return err
		}
	}
	if mode == "bundle" {
		if _, err := fmt.Fprintf(streams.Stderr, "bundle root: %s\n", manifest.HexKey(result.BundleKey)); err != nil {
			return err
		}
	}
	if !*noProgress {
		if _, err = fmt.Fprintln(streams.Stderr); err != nil {
			return err
		}
	}
	if storeOut {
		stats := result.Stats
		encoding := "raw"
		if stats.ManifestCompressed {
			encoding = "snappy"
		}
		_, err = fmt.Fprintf(streams.Stderr, "image size:   %s\nstored bytes: %s\nchunks:       stored=%d dedup=%d zero=%d\ncompression:  raw=%d snappy=%d logical=%s encoded=%s saved=%s\nmanifest:     encoding=%s logical=%s stored=%s\nmanifest key: %s\n",
			formatSize(src.Size()), formatSize(stats.StoredBytes), stats.StoredChunks, stats.DedupChunks, stats.ZeroChunks, stats.RawChunks, stats.CompressedChunks, formatSize(stats.LogicalChunkBytes), formatSize(stats.EncodedChunkBytes), formatSize(stats.CompressionSavedBytes), encoding, formatSize(stats.ManifestLogicalBytes), formatSize(stats.ManifestStoredBytes), manifest.HexKey(result.StoreKey))
	} else {
		_, err = fmt.Fprintf(streams.Stderr, "%s: %s logical bytes\n", command, formatSize(src.Size()))
	}
	return err
}

func readBoundedTail(path string, limit int) (body []byte, retErr error) {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > int64(limit) {
		return nil, errors.New("tail must be a bounded regular file")
	}
	body, err = io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(body) > limit {
		return nil, errors.New("tail exceeds memory budget")
	}
	return body, nil
}
func samePath(a, b string) bool {
	canonical := func(s string) string {
		p, err := filepath.Abs(s)
		if err != nil {
			return s
		}
		if q, err := filepath.EvalSymlinks(p); err == nil {
			return q
		}
		if q, err := filepath.EvalSymlinks(filepath.Dir(p)); err == nil {
			return filepath.Join(q, filepath.Base(p))
		}
		return p
	}
	return canonical(a) == canonical(b)
}
func writeAll(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}

// Each final output is pinned until ownership-safe cleanup or successful close.
func openDirectOutput(path string) (*os.File, func(error) error, error) {
	if path == "" || path == "-" {
		return os.Stdout, func(err error) error { return err }, nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, nil, err
	}
	owned, err := f.Stat()
	if err != nil {
		return nil, nil, errors.Join(err, f.Close())
	}
	var once sync.Once
	var completed error
	finish := func(prior error) error {
		once.Do(func() {
			current, statErr := os.Lstat(path)
			same := statErr == nil && os.SameFile(owned, current)
			if statErr != nil {
				prior = errors.Join(prior, statErr)
			} else if !same {
				prior = errors.Join(prior, errors.New("output pathname changed"))
			}
			var guard *os.File
			if same {
				guard, err = os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
				if err != nil {
					prior = errors.Join(prior, err)
				} else {
					info, e := guard.Stat()
					if e != nil || !os.SameFile(owned, info) {
						prior = errors.Join(prior, e, errors.New("output guard changed"))
						same = false
					}
				}
			}
			if prior != nil && same {
				current, e := os.Lstat(path)
				if e == nil && os.SameFile(owned, current) {
					prior = errors.Join(prior, os.Remove(path))
				}
			}
			closeErr := f.Close()
			prior = errors.Join(prior, closeErr)
			if closeErr != nil && same && guard != nil {
				current, e := os.Lstat(path)
				if e == nil && os.SameFile(owned, current) {
					prior = errors.Join(prior, os.Remove(path))
				}
			}
			if guard != nil {
				prior = errors.Join(prior, guard.Close())
			}
			completed = prior
		})
		return completed
	}
	return f, finish, nil
}

type sourceReaderAt struct {
	ctx context.Context
	s   sparse.Source
}

func (r sourceReaderAt) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	return r.s.ReadAt(r.ctx, b, uint64(off))
}

type sourceStream struct{ sparse.Source }

func (*sourceStream) Close() error { return nil }
func (s *sourceStream) PayloadCommitment() (uint64, [32]byte, bool) {
	if p, ok := s.Source.(tarstream.IdentityProvider); ok {
		return p.PayloadCommitment()
	}
	return s.Size(), [32]byte{}, false
}
func (s *sourceStream) TarStreamDigest(name string) ([32]byte, bool) {
	if p, ok := s.Source.(tarstream.IdentityProvider); ok {
		return p.TarStreamDigest(name)
	}
	return [32]byte{}, false
}

// payloadBoundary annotates geometry without retaining or changing any bytes.
type payloadBoundary struct {
	sparse.Source
	boundary uint64
}

func (s payloadBoundary) PayloadCommitment() (uint64, [32]byte, bool) {
	return s.boundary, [32]byte{}, false
}
func (s payloadBoundary) TarStreamDigest(string) ([32]byte, bool) { return [32]byte{}, false }

type optionalTailReader struct {
	ctx    context.Context
	source sparse.Source
	err    error
}

func (r *optionalTailReader) ReadAt(p []byte, off int64) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if off < 0 {
		return 0, io.EOF
	}
	n, err := r.source.ReadAt(r.ctx, p, uint64(off))
	if err != nil && err != io.EOF {
		r.err = err
	}
	return n, err
}
