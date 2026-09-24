package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util"
	"github.com/kuasar-sandbox/accelerator/internal/util/obstat"
	"github.com/kuasar-sandbox/accelerator/pkg/cache"
	cacheserver "github.com/kuasar-sandbox/accelerator/pkg/cache/server"
	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storefs "github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3/sdkclient"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
	"google.golang.org/grpc"
)

// Run owns a complete Store serving lifecycle.
func Run(ctx context.Context, cfg Config, opts Options) error {
	if ctx == nil {
		return errors.New("store: nil context")
	}
	if err := ctx.Err(); err != nil {
		if err == context.Canceled {
			return nil
		}
		return err
	}
	cfg, err := prepareConfig(cfg, opts)
	if err != nil {
		return err
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	source, closeSource := opts.Generations, nopClose
	if source == nil {
		source, closeSource, err = builtInGenerationSource(ctx, cfg, opts.GenerationCredentials)
		if err != nil {
			return startupError(ctx, err)
		}
	}
	input, err := newGenerationInput(ctx, source)
	if err != nil {
		return startupError(ctx, errors.Join(fmt.Errorf("store: initial generations: %w", err), closeSource()))
	}
	if err = ctx.Err(); err != nil {
		return startupError(ctx, errors.Join(err, closeSource()))
	}
	backend, closeBackend, err := openBackend(ctx, cfg, opts.ObjectCredentials)
	if err != nil {
		return startupError(ctx, errors.Join(err, closeSource()))
	}
	closeOwned := func() error { return errors.Join(closeBackend(), closeSource()) }
	if err = ctx.Err(); err != nil {
		return startupError(ctx, errors.Join(err, closeOwned()))
	}
	srv, err := storeserver.New(storeserver.Options{Backend: backend, Generations: input.view, VerifyKey: cfg.VerifyKey()})
	if err != nil {
		return startupError(ctx, errors.Join(err, closeOwned()))
	}
	gl, err := util.Listen(cfg.Listen)
	if err != nil {
		return startupError(ctx, errors.Join(err, closeOwned()))
	}
	var wl net.Listener
	if cfg.CacheListen != "" {
		wl, err = util.Listen(cfg.CacheListen)
		if err != nil {
			return startupError(ctx, errors.Join(err, gl.Close(), closeOwned()))
		}
	}
	closeListeners := func() error {
		var werr error
		if wl != nil {
			werr = wl.Close()
		}
		return errors.Join(gl.Close(), werr)
	}
	if err = ctx.Err(); err != nil {
		return startupError(ctx, errors.Join(err, closeListeners(), closeOwned()))
	}
	gs := grpc.NewServer(grpc.WaitForHandlers(true))
	pb.RegisterStoreServer(gs, srv)
	var ws *cacheserver.WireServer
	if wl != nil {
		tier := readOnlyTier{cache.NewStoreOrigin(storeGetter{srv})}
		ws = cacheserver.NewWireServer(cacheserver.NewCacheHandler(tier, nil), 120*time.Second, 0)
	}
	runCtx, cancel := context.WithCancel(ctx)
	var observers sync.WaitGroup
	observers.Add(2)
	go func() { defer observers.Done(); input.run(runCtx, opts.Reload, log) }()
	go func() {
		defer observers.Done()
		obstat.RunAdaptive(runCtx, cfg.StatsIntervalDur(), sampler(srv), func(f string, a ...any) { log.Info(fmt.Sprintf(f, a...)) })
	}()
	type result struct {
		name string
		err  error
	}
	results := make(chan result, 2)
	n := 1
	go func() { results <- result{"grpc", gs.Serve(gl)} }()
	if ws != nil {
		n++
		go func() { results <- result{"cache", ws.Serve(wl)} }()
	}
	snap := input.current()
	log.Info("store started", "listen", cfg.Listen, "backend", cfg.Backend, "generation", snap[len(snap)-1], "verify", cfg.VerifyKey(), "direct_io", cfg.Backend == "fs" && cfg.FS.DirectIO)
	var out error
	done := 0
	select {
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			out = ctx.Err()
		}
	case r := <-results:
		done++
		out = serveError(r.name, r.err)
	}
	cancel()
	var stops sync.WaitGroup
	stops.Add(1)
	go func() { defer stops.Done(); gs.Stop() }()
	if ws != nil {
		stops.Add(1)
		go func() { defer stops.Done(); ws.GracefulStop() }()
	}
	stops.Wait()
	if ws != nil {
		ws.Wait()
	}
	for done < n {
		r := <-results
		done++
		if !expectedStop(r.err) {
			out = errors.Join(out, serveError(r.name, r.err))
		}
	}
	observers.Wait()
	out = errors.Join(out, closeOwned())
	log.Info("store stopped", "error", out)
	return out
}

func serveError(name string, err error) error {
	if err == nil {
		return fmt.Errorf("store: %s serve stopped unexpectedly", name)
	}
	return fmt.Errorf("store: %s serve: %w", name, err)
}
func expectedStop(err error) bool {
	return err == nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed)
}
func startupError(ctx context.Context, err error) error {
	if ctx.Err() == context.Canceled && canceledLeaves(err) {
		return nil
	}
	return err
}
func canceledLeaves(err error) bool {
	if err == nil {
		return true
	}
	if e, ok := err.(interface{ Unwrap() []error }); ok {
		v := e.Unwrap()
		if len(v) == 0 {
			return false
		}
		for _, x := range v {
			if !canceledLeaves(x) {
				return false
			}
		}
		return true
	}
	if e, ok := err.(interface{ Unwrap() error }); ok {
		x := e.Unwrap()
		return x != nil && canceledLeaves(x)
	}
	return err == context.Canceled
}

func openBackend(ctx context.Context, cfg Config, credentials CredentialsProvider) (storeserver.Backend, func() error, error) {
	if cfg.Backend == "fs" {
		b, err := storefs.New(storefs.Config{Root: cfg.FS.Root, DirectIO: cfg.FS.DirectIO})
		return b, nopClose, err
	}
	c, err := sdkclient.New(ctx, sdkclient.Config{Endpoint: cfg.S3.Endpoint, Region: cfg.S3.Region, Bucket: cfg.S3.Bucket, PathStyle: cfg.S3PathStyle(), AccessKey: cfg.S3.AccessKey, SecretKey: cfg.S3.SecretKey, Credentials: credentials, TLS: sdkclient.TLSConfig{CACert: tlsCACert(cfg.S3.TLS), InsecureSkipVerify: tlsInsecure(cfg.S3.TLS)}})
	if err != nil {
		return nil, nopClose, err
	}
	timeout, _ := cfg.S3OpTimeout()
	b, err := stores3.New(ctx, c, stores3.Config{Bucket: cfg.S3.Bucket, Prefix: cfg.S3.Prefix, MaxInflight: cfg.S3.MaxInflight, OpTimeout: timeout, MaxObjectSize: cfg.S3.MaxObjectSize})
	if err != nil {
		return nil, nopClose, errors.Join(err, c.Close())
	}
	return b, c.Close, nil
}
func nopClose() error { return nil }

type storeGetter struct{ s *storeserver.Server }

func (g storeGetter) Get(ctx context.Context, p pkgstore.Partition, k pkgstore.ContentKey) (bool, []byte, error) {
	return g.s.GetObject(ctx, p, k)
}

type readOnlyTier struct{ cache.Getter }

func (readOnlyTier) Fill(context.Context, pkgstore.Partition, pkgstore.ContentKey, []byte) error {
	return errors.New("store cache listener is read-only")
}
func (readOnlyTier) RejectsWrites() bool { return true }

func sampler(s *storeserver.Server) func(float64) (string, bool) {
	prev := s.Stats()
	return func(elapsed float64) (string, bool) {
		cur := s.Stats()
		gets, puts, admits := cur.GetN-prev.GetN, cur.PutN-prev.PutN, cur.AdmitN-prev.AdmitN
		if gets+puts+admits == 0 && cur.Inflight == 0 {
			prev = cur
			return "", false
		}
		gh, ph := cur.GetHist.Sub(prev.GetHist), cur.PutHist.Sub(prev.PutHist)
		var b strings.Builder
		fmt.Fprintf(&b, "store stat | get %s/s", obstat.FmtCount(float64(gets)/elapsed))
		if gets > 0 {
			fmt.Fprintf(&b, " %.0f%%hit %s p50 %s/p99 %s/max %s", pct(cur.GetHits-prev.GetHits, gets), obstat.FmtBytesPerSec(float64(cur.GetBytes-prev.GetBytes)/elapsed), obstat.FmtNs(gh.P50()), obstat.FmtNs(gh.P99()), obstat.FmtNs(gh.MaxNs))
		}
		fmt.Fprintf(&b, " · put %s/s", obstat.FmtCount(float64(puts)/elapsed))
		if puts > 0 {
			fmt.Fprintf(&b, " %s dedup %.0f%% p50 %s/p99 %s/max %s", obstat.FmtBytesPerSec(float64(cur.PutBytes-prev.PutBytes)/elapsed), pct(cur.PutDedup-prev.PutDedup, puts), obstat.FmtNs(ph.P50()), obstat.FmtNs(ph.P99()), obstat.FmtNs(ph.MaxNs))
		}
		if admits > 0 {
			fmt.Fprintf(&b, " · admit %s/s", obstat.FmtCount(float64(admits)/elapsed))
		}
		fmt.Fprintf(&b, " | inflight %d", cur.Inflight)
		if e := cur.ErrN - prev.ErrN; e > 0 {
			fmt.Fprintf(&b, " | err %d", e)
		}
		prev = cur
		return b.String(), true
	}
}
func pct(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b) * 100
}
