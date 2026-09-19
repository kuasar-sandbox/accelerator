package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

type Reader struct {
	cfg       *manifest.Config
	key       [32]byte
	locations manifest.RefLocations
	codec     tarstream.Codec
	required  bool
	remote    manifest.FetcherCloser
}

func NewReader(cfg *manifest.Config, key [32]byte, locations manifest.RefLocations) (*Reader, error) {
	if cfg == nil {
		return nil, errors.New("manifest configuration is required")
	}
	codec, required, err := LocalCodec(cfg, key)
	if err != nil {
		return nil, err
	}
	return &Reader{cfg: cfg, key: key, locations: locations, codec: codec, required: required}, nil
}
func (r *Reader) ReadOptions() []tarstream.ReadOption {
	return readOptions(r.codec, r.required)
}
func (r *Reader) Close() error {
	clear(r.key[:])
	if r.remote != nil {
		return r.remote.Close()
	}
	return nil
}
func (r *Reader) remoteFetcher() (manifest.FetcherCloser, error) {
	if r.remote == nil {
		var err error
		r.remote, err = r.cfg.NewFetcher()
		if err != nil {
			return nil, err
		}
	}
	return r.remote, nil
}

type Opened struct {
	sparse.Source
	path     string
	finish   func(context.Context) error
	closeFn  func() error
	once     sync.Once
	closeErr error
}

func (s *Opened) Close() error {
	s.once.Do(func() {
		if s.closeFn != nil {
			s.closeErr = s.closeFn()
		}
	})
	return s.closeErr
}
func (s *Opened) Verify(ctx context.Context) error {
	if s.finish != nil {
		return s.finish(ctx)
	}
	return ctx.Err()
}
func (s *Opened) PayloadCommitment() (uint64, [32]byte, bool) {
	if p, ok := s.Source.(tarstream.IdentityProvider); ok {
		return p.PayloadCommitment()
	}
	return s.Size(), [32]byte{}, false
}
func (s *Opened) TarStreamDigest(name string) ([32]byte, bool) {
	if p, ok := s.Source.(tarstream.IdentityProvider); ok {
		return p.TarStreamDigest(name)
	}
	return [32]byte{}, false
}

func (r *Reader) Open(ctx context.Context, raw string) (*Opened, error) {
	var ref manifest.Ref
	var err error
	switch {
	case strings.Contains(raw, "://"):
		ref, err = manifest.ParseRef(raw)
		if err != nil {
			return nil, err
		}
	default:
		if key, e := manifest.ParseHexKey(raw); e == nil {
			ref = manifest.Ref{Scheme: manifest.RefSchemeManifest, Path: manifest.HexKey(key)}
		} else {
			ref = manifest.Ref{Scheme: manifest.RefSchemeFile, Path: raw}
		}
	}
	if ref.Scheme == manifest.RefSchemeManifest {
		remote, err := r.remoteFetcher()
		if err != nil {
			return nil, err
		}
		key, err := manifest.ParseHexKey(ref.Path)
		if err != nil {
			return nil, err
		}
		stream, err := remote.OpenManifest(ctx, key)
		if err != nil {
			return nil, err
		}
		return &Opened{Source: stream, closeFn: stream.Close}, nil
	}
	path := ref.Path
	if ref.Location != "" {
		base, ok := r.locations[ref.Location]
		if !ok {
			return nil, fmt.Errorf("missing location %q", ref.Location)
		}
		path = filepath.Join(base, path)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Opened, error) { return nil, errors.Join(err, file.Close()) }
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		return fail(errors.New("input ref must resolve to a regular random-access file"))
	}
	var magic [4]byte
	n, probeErr := file.ReadAt(magic[:], 0)
	if probeErr != nil && probeErr != io.EOF {
		return fail(probeErr)
	}
	isBundle := ref.DigestScheme == "manifest" || n == 4 && bytes.Equal(magic[:], []byte{'P', 'K', 3, 4})
	if isBundle {
		if ref.DigestScheme != "" && ref.DigestScheme != "manifest" {
			return fail(errors.New("Bundle requires a Manifest selector"))
		}
		reader, err := bundle.NewReader(file, info.Size())
		if err != nil {
			return fail(err)
		}
		failBundle := func(err error) (*Opened, error) { return fail(errors.Join(err, reader.Close())) }
		var key [32]byte
		if ref.Digest != "" {
			key, err = manifest.ParseHexKey(ref.Digest)
			if err != nil {
				return failBundle(err)
			}
		} else {
			keys := reader.ManifestKeys()
			if len(keys) != 1 {
				return failBundle(errors.New("multi-Manifest Bundle requires @manifest selector"))
			}
			key = keys[0]
		}
		_, decryptor, err := manifestcrypto.New(r.cfg.Crypto)
		if err != nil {
			return failBundle(err)
		}
		verify := ref.DigestScheme == "manifest" || r.cfg.Manifest.VerifyContent == nil || *r.cfg.Manifest.VerifyContent
		local := fetch.NewFetcherWithOptions(r.key, reader.Getter(), decryptor, fetch.Options{VerifyContent: verify})
		// An explicitly selected root belongs to the current Bundle. Every Chunk
		// is served from that selected source, never from a different Bundle.
		scope := bundle.NewManifestFetcher(reader, local, nil)
		stream, err := scope.OpenRootManifest(ctx, key)
		if err != nil {
			return failBundle(err)
		}
		return &Opened{Source: stream, path: path, closeFn: func() error { return errors.Join(stream.Close(), reader.Close(), file.Close()) }, finish: func(ctx context.Context) error {
			if !verify {
				return ctx.Err()
			}
			return bundle.VerifyExactManifests(ctx, bundle.ExactManifest{Key: key, Reader: reader}, nil, r.key, decryptor, bundle.VerifyOptions{})
		}}, nil
	}
	options := r.ReadOptions()
	if ref.DigestScheme != "" {
		options = append(options, tarstream.WithExpectedDigest(ref.DigestScheme, ref.Digest))
	}
	src, _, err := tarstream.SourceAt(file, info.Size(), "", options...)
	if err != nil {
		return fail(err)
	}
	if src.Size() > math.MaxInt64 {
		return fail(errors.New("logical source is too large for random-access carrier operations"))
	}
	closeSource := func() error {
		var err error
		if c, ok := src.(io.Closer); ok {
			err = c.Close()
		}
		return errors.Join(err, file.Close())
	}
	return &Opened{Source: src, path: path, closeFn: closeSource, finish: func(ctx context.Context) (retErr error) {
		if ref.DigestScheme == "" && r.cfg.Manifest.VerifyContent != nil && !*r.cfg.Manifest.VerifyContent {
			return ctx.Err()
		}
		verified, _, err := tarstream.SourceFrom(io.NewSectionReader(file, 0, info.Size()), "", options...)
		if err != nil {
			return err
		}
		if c, ok := verified.(io.Closer); ok {
			defer func() { retErr = errors.Join(retErr, c.Close()) }()
		}
		return consumeTransferSource(ctx, verified)
	}}, nil
}

func consumeTransferSource(ctx context.Context, src sparse.Source) error {
	buffer := make([]byte, 64<<10)
	for off := uint64(0); off < src.Size(); {
		if err := ctx.Err(); err != nil {
			return err
		}
		run, err := src.RunAt(off, src.Size()-off)
		if err != nil {
			return err
		}
		if run == nil || run.Offset() != off || run.End() <= off || run.End() > src.Size() {
			return errors.New("invalid source run")
		}
		if run.Kind() == sparse.Data {
			for at := off; at < run.End(); {
				size := min(uint64(len(buffer)), run.End()-at)
				n, err := run.ReadAt(ctx, buffer[:size], at-off)
				if err != nil {
					return err
				}
				if n != int(size) {
					return io.ErrUnexpectedEOF
				}
				at += size
			}
		}
		off = run.End()
	}
	return nil
}

// LocalCodec returns the fixed codec policy used by this reader.
func (r *Reader) LocalCodec() (tarstream.Codec, bool) { return r.codec, r.required }

// Path reports the opened local path, or empty for a remote Manifest.
func (s *Opened) Path() string { return s.path }
