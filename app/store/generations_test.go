package store

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pkgstore "github.com/kuasar-sandbox/accelerator/pkg/store"
)

// Compile-time checks intentionally spell out the complete public contract.
var (
	_ Config
	_ Credentials
	_ CredentialsProvider
	_ = Options{
		Logger:                nil,
		Reload:                nil,
		Generations:           &GenerationSource{},
		ObjectCredentials:     nil,
		GenerationCredentials: nil,
	}
)

func source(load func(context.Context) ([]pkgstore.Generation, error), interval time.Duration) *GenerationSource {
	return &GenerationSource{Load: load, RefreshInterval: interval}
}

func TestGenerationInputInitialLoadAndCopyIsolation(t *testing.T) {
	returned := []pkgstore.Generation{"one", "two"}
	g, err := newGenerationInput(context.Background(), source(func(context.Context) ([]pkgstore.Generation, error) {
		return returned, nil
	}, 0))
	if err != nil {
		t.Fatal(err)
	}
	returned[0] = "caller-mutated"
	got := g.current()
	got[1] = "reader-mutated"
	if again := g.current(); again[0] != "one" || again[1] != "two" {
		t.Fatalf("active snapshot was mutated: %v", again)
	}
}

func TestGenerationInputViewDoesNotAllocate(t *testing.T) {
	g, err := newGenerationInput(context.Background(), source(func(context.Context) ([]pkgstore.Generation, error) {
		return []pkgstore.Generation{"one"}, nil
	}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = g.view() }); allocs != 0 {
		t.Fatalf("view allocations = %v, want 0", allocs)
	}
}

func TestGenerationInputRejectsInitialFailures(t *testing.T) {
	want := errors.New("load failed")
	for name, src := range map[string]*GenerationSource{
		"nil source":        nil,
		"nil load":          {},
		"negative interval": {Load: func(context.Context) ([]pkgstore.Generation, error) { return []pkgstore.Generation{"one"}, nil }, RefreshInterval: -1},
		"load error":        source(func(context.Context) ([]pkgstore.Generation, error) { return nil, want }, 0),
		"empty":             source(func(context.Context) ([]pkgstore.Generation, error) { return nil, nil }, 0),
		"invalid": source(func(context.Context) ([]pkgstore.Generation, error) {
			return []pkgstore.Generation{"bad generation"}, nil
		}, 0),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newGenerationInput(context.Background(), src); err == nil {
				t.Fatal("expected initial load error")
			}
		})
	}
}

func TestGenerationInputRefreshFailureRetainsSnapshot(t *testing.T) {
	var calls atomic.Int32
	g, err := newGenerationInput(context.Background(), source(func(context.Context) ([]pkgstore.Generation, error) {
		if calls.Add(1) == 1 {
			return []pkgstore.Generation{"one"}, nil
		}
		return []pkgstore.Generation{"bad generation"}, nil
	}, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.refresh(context.Background()); err == nil {
		t.Fatal("expected invalid refresh")
	}
	if got := g.current(); len(got) != 1 || got[0] != "one" {
		t.Fatalf("snapshot after failed refresh = %v", got)
	}
}

func TestGenerationInputRefreshFailureLogsToInstanceLogger(t *testing.T) {
	var calls atomic.Int32
	refreshed := make(chan struct{})
	g, err := newGenerationInput(context.Background(), source(func(context.Context) ([]pkgstore.Generation, error) {
		if calls.Add(1) == 1 {
			return []pkgstore.Generation{"one"}, nil
		}
		close(refreshed)
		return nil, errors.New("instance refresh failure")
	}, 0))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	reload := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.run(ctx, reload, logger); close(done) }()
	reload <- struct{}{}
	<-refreshed
	cancel()
	<-done
	if got := g.current(); len(got) != 1 || got[0] != "one" {
		t.Fatalf("snapshot after failed refresh = %v", got)
	}
	if !strings.Contains(logs.String(), "instance refresh failure") {
		t.Fatalf("instance log = %q", logs.String())
	}
}

func TestGenerationInputSerializesTimerAndReload(t *testing.T) {
	var active, maximum, calls atomic.Int32
	entered := make(chan struct{}, 8)
	release := make(chan struct{}, 8)
	load := func(context.Context) ([]pkgstore.Generation, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		call := calls.Add(1)
		if call > 1 {
			entered <- struct{}{}
			<-release
		}
		return []pkgstore.Generation{"one"}, nil
	}
	g, err := newGenerationInput(context.Background(), source(load, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	reload := make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.run(ctx, reload, nil); close(done) }()
	<-entered
	reload <- struct{}{}
	time.Sleep(10 * time.Millisecond)
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent Load calls = %d", maximum.Load())
	}
	release <- struct{}{}
	<-entered
	// Cancel before releasing the final load so a pending tick cannot start
	// another load that has no matching release.
	cancel()
	release <- struct{}{}
	<-done
}

func TestGenerationInputClosedReloadDoesNotSpin(t *testing.T) {
	var calls atomic.Int32
	g, err := newGenerationInput(context.Background(), source(func(context.Context) ([]pkgstore.Generation, error) {
		calls.Add(1)
		return []pkgstore.Generation{"one"}, nil
	}, 0))
	if err != nil {
		t.Fatal(err)
	}
	reload := make(chan struct{})
	close(reload)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.run(ctx, reload, nil); close(done) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	if got := calls.Load(); got != 1 {
		t.Fatalf("Load calls with closed Reload = %d, want 1", got)
	}
}

func TestGenerationInputCancellationWaitsAndPreventsAnotherLoad(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	g, err := newGenerationInput(context.Background(), source(func(context.Context) ([]pkgstore.Generation, error) {
		if calls.Add(1) == 2 {
			close(started)
			<-release
		}
		return []pkgstore.Generation{"one"}, nil
	}, 0))
	if err != nil {
		t.Fatal(err)
	}
	reload := make(chan struct{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.run(ctx, reload, nil); close(done) }()
	reload <- struct{}{}
	<-started
	reload <- struct{}{}
	cancel()
	select {
	case <-done:
		t.Fatal("run returned while Load was still in progress")
	default:
	}
	close(release)
	<-done
	if got := calls.Load(); got != 2 {
		t.Fatalf("Load calls after cancellation = %d, want 2", got)
	}
}
