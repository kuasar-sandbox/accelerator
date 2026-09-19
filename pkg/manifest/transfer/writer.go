package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

type Result struct {
	StoreKey, BundleKey store.ContentKey
	Stats               ingest.Result
}

// transferWriter fans out already encoded objects. Only the final Manifest is
// retained until input verification and requested file finalization succeed.
type transferWriter struct {
	admission store.WriteAdmission
	target    ingest.StoreWriter
	local     *bundle.Writer
	workers   int
	root      store.ContentKey
	manifest  []byte
}

func (w *transferWriter) AdmitWrite(ctx context.Context) (store.WriteAdmission, error) {
	return w.admission, ctx.Err()
}
func (w *transferWriter) PoolSize() int { return w.workers }
func (w *transferWriter) Put(ctx context.Context, a store.WriteAdmission, p store.Partition, key store.ContentKey, data []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if a != w.admission {
		return false, errors.New("write admission changed")
	}
	if p == store.PartitionManifest {
		if len(w.manifest) != 0 {
			return false, errors.New("transfer expects one root Manifest")
		}
		if w.local != nil {
			if _, err := w.local.Put(ctx, a, p, key, data); err != nil {
				return false, err
			}
		}
		w.root = key
		if w.target != nil {
			w.manifest = bytes.Clone(data)
		}
		return true, nil
	}
	if w.local != nil {
		if _, err := w.local.Put(ctx, a, p, key, data); err != nil {
			return false, err
		}
	}
	if w.target != nil {
		return w.target.Put(ctx, a, p, key, data)
	}
	return true, nil
}
func (w *transferWriter) PutChunkOrdered(ctx context.Context, a store.WriteAdmission, key store.ContentKey, data []byte, ordinal uint64) (bool, error) {
	if a != w.admission {
		return false, errors.New("write admission changed")
	}
	if w.local != nil {
		if _, err := w.local.PutChunkOrdered(ctx, a, key, data, ordinal); err != nil {
			return false, err
		}
	}
	if w.target != nil {
		return w.target.Put(ctx, a, store.PartitionChunk, key, data)
	}
	return true, nil
}
func (w *transferWriter) commit(ctx context.Context) (bool, error) {
	if w.target == nil {
		return true, nil
	}
	if len(w.manifest) == 0 {
		return false, errors.New("missing final Manifest")
	}
	fresh, err := w.target.Put(ctx, w.admission, store.PartitionManifest, w.root, w.manifest)
	clear(w.manifest)
	w.manifest = nil
	return fresh, err
}

func writeTransfer(ctx context.Context, cfg *manifest.Config, key [32]byte, src sparse.Source, mode string, out io.Writer, name string, storeOut bool, extra []byte, codec tarstream.Codec, required bool, finish func() error, progress func(uint64, uint64)) (result Result, retErr error) {
	if src == nil {
		return result, errors.New("source is required")
	}
	var client *storeclient.Client
	var writer *transferWriter
	var ing ingest.Ingester
	if storeOut || mode == "bundle" {
		admission := store.WriteAdmission{}
		var err error
		workers := max(1, runtime.GOMAXPROCS(0))
		if storeOut {
			if cfg.Store.Endpoint == "" {
				return result, errors.New("store.endpoint is required")
			}
			var timeout time.Duration
			if cfg.Store.Timeout != "" {
				timeout, err = time.ParseDuration(cfg.Store.Timeout)
				if err != nil {
					return result, err
				}
			}
			client, err = storeclient.New(cfg.Store.Endpoint, cfg.Store.Pool, timeout)
			if err != nil {
				return result, err
			}
			defer func() { retErr = errors.Join(retErr, client.Close()) }()
			if cfg.Manifest.WriteGeneration != "" {
				admission, err = client.AdmitWriteFor(ctx, store.Generation(cfg.Manifest.WriteGeneration))
			} else {
				admission, err = client.AdmitWrite(ctx)
			}
			workers = min(workers, max(1, client.PoolSize()))
		} else {
			admission, err = cfg.WriteAdmission(ctx)
		}
		if err != nil {
			return result, err
		}
		writer = &transferWriter{admission: admission, workers: workers}
		if client != nil {
			writer.target = client
		}
		defer func() { clear(writer.manifest) }()
		var saltFn ingest.ExtraSaltFunc
		if len(extra) > 0 {
			saltFn = func() ([]byte, error) { return extra, nil }
		}
		ing, err = cfg.NewIngesterWithWriter(func() ([32]byte, error) { return key, nil }, saltFn, writer)
		if err != nil {
			return result, err
		}
		if mode == "bundle" {
			if out == nil {
				return result, errors.New("Bundle output writer is required")
			}
			writer.local, err = bundle.NewWriter(out, admission, bundle.WriterOptions{Concurrency: workers})
			if err != nil {
				return result, err
			}
		}
		ingestSource := src
		var pipeReader *io.PipeReader
		var outputDone chan error
		if mode == "tarstream" && storeOut {
			var pipeWriter *io.PipeWriter
			pipeReader, pipeWriter = io.Pipe()
			outputDone = make(chan error, 1)
			go func() {
				_, _, writeErr := tarstream.WriteTo(ctx, io.MultiWriter(out, pipeWriter), name, src, writeOptions(codec, required)...)
				_ = pipeWriter.CloseWithError(writeErr)
				outputDone <- writeErr
			}()
			ingestSource, _, err = tarstream.SourceFrom(pipeReader, "", readOptions(codec, required)...)
			if err != nil {
				_ = pipeReader.CloseWithError(err)
				return result, errors.Join(err, <-outputDone)
			}
		}
		written, err := ing.Ingest(ctx, ingestSource, ingest.IngestOption{OnProgress: progress})
		if pipeReader != nil {
			_ = pipeReader.CloseWithError(err)
			err = errors.Join(err, <-outputDone)
		}
		if err != nil {
			return result, err
		}
		result.Stats = *written
		if mode == "bundle" {
			if err := writer.local.Finalize(written.ManifestKey); err != nil {
				return result, err
			}
			result.BundleKey = written.ManifestKey
		}
		if storeOut {
			result.StoreKey = written.ManifestKey
		}
	}
	if mode == "tarstream" && !storeOut {
		if out == nil {
			return result, errors.New("tarstream output writer is required")
		}
		outputSource := src
		if progress != nil {
			outputSource = &progressSource{Source: src, report: progress}
		}
		if _, _, err := tarstream.WriteTo(ctx, out, name, outputSource, writeOptions(codec, required)...); err != nil {
			return result, fmt.Errorf("write tarstream: %w", err)
		}
		if progress != nil {
			progress(src.Size(), src.Size())
		}
	}
	if finish != nil {
		if err := finish(); err != nil {
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if writer != nil {
		fresh, err := writer.commit(ctx)
		if err != nil {
			return result, err
		}
		if !fresh {
			result.Stats.StoredBytes -= result.Stats.ManifestStoredBytes
		}
	}
	return result, nil
}

// WriteOptions selects destinations for one logical source. BeforeCommit runs
// after file encoding and before the Store root is published; callers use it
// to finish source verification and close their owned output files.
type WriteOptions struct {
	Mode                   string
	Output                 io.Writer
	Name                   string
	Store                  bool
	ExtraSalt              []byte
	Codec                  tarstream.Codec
	RequireLocalEncryption bool
	BeforeCommit           func() error
	OnProgress             func(uint64, uint64)
}

// Write consumes a sparse source with bounded encoding work. Store+Bundle
// shares physical objects; Store+tarstream uses a bounded streaming pipe.
func Write(ctx context.Context, cfg *manifest.Config, key [32]byte, src sparse.Source, opts WriteOptions) (Result, error) {
	if cfg == nil {
		return Result{}, errors.New("manifest configuration is required")
	}
	if opts.Mode != "none" && opts.Mode != "bundle" && opts.Mode != "tarstream" {
		return Result{}, errors.New("invalid output mode")
	}
	if opts.Mode != "none" && opts.Output == nil {
		return Result{}, errors.New("output writer is required")
	}
	return writeTransfer(ctx, cfg, key, src, opts.Mode, opts.Output, opts.Name, opts.Store, opts.ExtraSalt, opts.Codec, opts.RequireLocalEncryption, opts.BeforeCommit, opts.OnProgress)
}

// progressSource reports successful forward data consumption, not metadata
// RunAt probes. The enclosing writer reports completion across holes/zeroes.
type progressSource struct {
	sparse.Source
	report    func(uint64, uint64)
	processed uint64
}

func (s *progressSource) progress(end uint64) {
	if end > s.processed {
		s.processed = end
		s.report(end, s.Size())
	}
}
func (s *progressSource) PayloadCommitment() (uint64, [32]byte, bool) {
	if p, ok := s.Source.(tarstream.IdentityProvider); ok {
		return p.PayloadCommitment()
	}
	return s.Size(), [32]byte{}, false
}
func (s *progressSource) TarStreamDigest(name string) ([32]byte, bool) {
	if p, ok := s.Source.(tarstream.IdentityProvider); ok {
		return p.TarStreamDigest(name)
	}
	return [32]byte{}, false
}
func (s *progressSource) RunAt(off, limit uint64) (sparse.Run, error) {
	run, err := s.Source.RunAt(off, limit)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, errors.New("nil progress source run")
	}
	return progressRun{Run: run, source: s}, nil
}
func (s *progressSource) ReadAt(ctx context.Context, p []byte, off uint64) (int, error) {
	n, err := s.Source.ReadAt(ctx, p, off)
	if n > 0 {
		s.progress(off + uint64(n))
	}
	return n, err
}

type progressRun struct {
	sparse.Run
	source *progressSource
}

func (r progressRun) ReadAt(ctx context.Context, p []byte, inner uint64) (int, error) {
	n, err := r.Run.ReadAt(ctx, p, inner)
	if n > 0 {
		r.source.progress(r.Offset() + inner + uint64(n))
	}
	return n, err
}
