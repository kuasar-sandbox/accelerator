package store

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
	storeconfig "github.com/kuasar-sandbox/accelerator/pkg/store/config"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3/sdkclient"
)

// Config is the declarative Store configuration shared with store-ctl.
type Config = storeconfig.Config

// Credentials is the provider-neutral credential value accepted by Store.
type Credentials = sdkclient.Credentials

// CredentialsProvider supplies credentials for one explicitly bound purpose.
type CredentialsProvider = sdkclient.CredentialsProvider

// GenerationSource is an authoritative source of complete, oldest-to-newest
// generation lists. A zero RefreshInterval disables periodic refresh.
type GenerationSource struct {
	Load            func(context.Context) ([]pkgstore.Generation, error)
	RefreshInterval time.Duration
}

// Options contains the process-local inputs to the Store application.
type Options struct {
	Logger                *slog.Logger
	Reload                <-chan struct{}
	Generations           *GenerationSource
	ObjectCredentials     CredentialsProvider
	GenerationCredentials CredentialsProvider
}

// generationInput owns refresh scheduling for one authoritative source. Calls
// to load happen only on the run goroutine, so timer and explicit refreshes can
// never overlap. The source and its callback are borrowed.
type generationInput struct {
	load     func(context.Context) ([]pkgstore.Generation, error)
	interval time.Duration

	loadMu   sync.Mutex
	mu       sync.RWMutex
	snapshot []pkgstore.Generation
}

func newGenerationInput(ctx context.Context, source *GenerationSource) (*generationInput, error) {
	if ctx == nil {
		return nil, errors.New("store: nil generation context")
	}
	if source == nil {
		return nil, errors.New("store: generation source is required")
	}
	if source.Load == nil {
		return nil, errors.New("store: generation source Load is required")
	}
	if source.RefreshInterval < 0 {
		return nil, errors.New("store: generation refresh interval must not be negative")
	}
	g := &generationInput{load: source.Load, interval: source.RefreshInterval}
	if err := g.refresh(ctx); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *generationInput) refresh(ctx context.Context) error {
	g.loadMu.Lock()
	defer g.loadMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	next, err := g.load(ctx)
	if err != nil {
		return err
	}
	// Copy before validation and publication so validation and the active
	// snapshot cannot be changed through caller-owned backing storage.
	next = append([]pkgstore.Generation(nil), next...)
	if err := pkgstore.ValidateGenerations(next); err != nil {
		return err
	}
	g.mu.Lock()
	g.snapshot = next
	g.mu.Unlock()
	return nil
}

func (g *generationInput) current() []pkgstore.Generation {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]pkgstore.Generation(nil), g.snapshot...)
}

// run refreshes until ctx is cancelled. It returns only after any synchronous
// Load already in progress has returned; consequently its return is the
// controller's quiescence boundary.
func (g *generationInput) run(ctx context.Context, reload <-chan struct{}) {
	var ticker *time.Ticker
	var ticks <-chan time.Time
	if g.interval > 0 {
		ticker = time.NewTicker(g.interval)
		ticks = ticker.C
		defer ticker.Stop()
	}

	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-reload:
			if !ok {
				reload = nil
				continue
			}
			if ctx.Err() == nil {
				_ = g.refresh(ctx) // Retain the last valid snapshot on failure.
			}
		case <-ticks:
			if ctx.Err() == nil {
				_ = g.refresh(ctx) // Retain the last valid snapshot on failure.
			}
		}
	}
}
