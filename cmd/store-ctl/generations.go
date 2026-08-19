package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
	"github.com/kuasar-sandbox/accelerator/pkg/store/s3/sdkclient"
)

const (
	maxGenerationListBytes = 256 << 10
	generationCASRetries   = 5
)

var errGenerationSourceUninitialised = errors.New("generation source is uninitialised (run `store-ctl init`)")

// generationSource is intentionally the complete internal read contract.
type generationSource interface {
	Load(ctx context.Context) ([]store.Generation, error)
}

type generationSourceKind uint8

const (
	generationSourceConfig generationSourceKind = iota + 1
	generationSourceFile
	generationSourceS3
)

type generationSourceHandle struct {
	source   generationSource
	kind     generationSourceKind
	interval time.Duration
	file     *fileGenerationSource
	s3       *s3GenerationSource
}

func openGenerationSource(ctx context.Context, cfg *Config, configPath string) (*generationSourceHandle, error) {
	g := cfg.Generations
	switch {
	case g.configSet:
		source := &configGenerationSource{path: configPath}
		return &generationSourceHandle{source: source, kind: generationSourceConfig}, nil
	case g.File != nil:
		source := &fileGenerationSource{path: g.File.Path}
		return &generationSourceHandle{source: source, kind: generationSourceFile, interval: cfg.GenerationRefreshInterval(), file: source}, nil
	case g.S3 != nil:
		client, err := sdkclient.New(ctx, sdkclient.Config{
			Endpoint:  g.S3.Endpoint,
			Region:    g.S3.Region,
			Bucket:    g.S3.Bucket,
			PathStyle: g.S3.pathStyle(),
			AccessKey: g.S3.AccessKey,
			SecretKey: g.S3.SecretKey,
		})
		if err != nil {
			return nil, fmt.Errorf("generation s3 client: %w", err)
		}
		source := &s3GenerationSource{client: client, key: g.S3.Key}
		return &generationSourceHandle{source: source, kind: generationSourceS3, interval: cfg.GenerationRefreshInterval(), s3: source}, nil
	default:
		return nil, errors.New("store-ctl: no generation source configured")
	}
}

type configGenerationSource struct{ path string }

func (s *configGenerationSource) Load(_ context.Context) ([]store.Generation, error) {
	cfg, err := LoadConfig(s.path, false)
	if err != nil {
		return nil, err
	}
	if cfg.Generations == nil || !cfg.Generations.configSet {
		return nil, errors.New("generation config source changed type")
	}
	return validateGenerationStrings(cfg.Generations.Config)
}

type fileGenerationSource struct{ path string }

func (s *fileGenerationSource) Load(_ context.Context) ([]store.Generation, error) {
	body, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: missing %s", errGenerationSourceUninitialised, s.path)
		}
		return nil, fmt.Errorf("read generation file %s: %w", s.path, err)
	}
	return parseGenerationList(body)
}

type generationObjectClient interface {
	Get(context.Context, string) ([]byte, *stores3.ObjectMeta, error)
	Put(context.Context, string, []byte, stores3.PutOptions) (string, error)
	Delete(context.Context, string) error
}

type s3GenerationSource struct {
	client generationObjectClient
	key    string
}

func (s *s3GenerationSource) Load(ctx context.Context) ([]store.Generation, error) {
	gens, _, err := s.loadWithETag(ctx)
	return gens, err
}

func (s *s3GenerationSource) loadWithETag(ctx context.Context) ([]store.Generation, string, error) {
	body, meta, err := s.client.Get(ctx, s.key)
	if errors.Is(err, stores3.ErrNotFound) {
		return nil, "", fmt.Errorf("%w: missing s3://.../%s", errGenerationSourceUninitialised, s.key)
	}
	if err != nil {
		return nil, "", fmt.Errorf("read generation object %s: %w", s.key, err)
	}
	if meta == nil || meta.ETag == "" {
		return nil, "", fmt.Errorf("generation object %s returned no ETag", s.key)
	}
	gens, err := parseGenerationList(body)
	if err != nil {
		return nil, "", err
	}
	return gens, meta.ETag, nil
}

func parseGenerationList(body []byte) ([]store.Generation, error) {
	if len(body) > maxGenerationListBytes {
		return nil, fmt.Errorf("generation list is %d bytes, maximum is %d", len(body), maxGenerationListBytes)
	}
	var generations []store.Generation
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			generations = append(generations, store.Generation(line))
		}
	}
	if err := store.ValidateGenerations(generations); err != nil {
		return nil, err
	}
	return generations, nil
}

func validateGenerationStrings(names []string) ([]store.Generation, error) {
	generations := make([]store.Generation, len(names))
	for i, name := range names {
		generations[i] = store.Generation(name)
	}
	if err := store.ValidateGenerations(generations); err != nil {
		return nil, err
	}
	return generations, nil
}

func renderGenerationList(generations []store.Generation) []byte {
	var builder strings.Builder
	for _, generation := range generations {
		builder.WriteString(string(generation))
		builder.WriteByte('\n')
	}
	return []byte(builder.String())
}

func cloneGenerations(generations []store.Generation) []store.Generation {
	return append([]store.Generation(nil), generations...)
}

type generationManager struct {
	handle  *generationSourceHandle
	current atomic.Value // []store.Generation, immutable after Store
}

func newGenerationManager(ctx context.Context, handle *generationSourceHandle) (*generationManager, error) {
	generations, err := handle.source.Load(ctx)
	if err != nil {
		return nil, err
	}
	if err := store.ValidateGenerations(generations); err != nil {
		return nil, err
	}
	manager := &generationManager{handle: handle}
	manager.current.Store(cloneGenerations(generations))
	return manager, nil
}

// Current returns the immutable current list. Callers must not modify it.
func (m *generationManager) Current() []store.Generation {
	return m.current.Load().([]store.Generation)
}

func (m *generationManager) Refresh(ctx context.Context) error {
	generations, err := m.handle.source.Load(ctx)
	if err != nil {
		return err
	}
	if err := store.ValidateGenerations(generations); err != nil {
		return err
	}
	m.current.Store(cloneGenerations(generations))
	return nil
}

func (m *generationManager) Run(ctx context.Context, hup <-chan os.Signal, logError func(string, ...any)) {
	var ticker *time.Ticker
	var ticks <-chan time.Time
	if m.handle.interval > 0 {
		ticker = time.NewTicker(m.handle.interval)
		ticks = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if err := m.Refresh(ctx); err != nil {
				logError("generation refresh failed; keeping previous list: %v", err)
			}
		case <-hup:
			if err := m.Refresh(ctx); err != nil {
				logError("generation SIGHUP refresh failed; keeping previous list: %v", err)
			}
		}
	}
}

func (h *generationSourceHandle) readOnly() bool { return h.kind == generationSourceConfig }

func (h *generationSourceHandle) initialise(ctx context.Context, generation store.Generation) error {
	generations := []store.Generation{generation}
	if err := store.ValidateGenerations(generations); err != nil {
		return err
	}
	switch h.kind {
	case generationSourceConfig:
		return errors.New("generation config source is read-only; update the main YAML")
	case generationSourceFile:
		return h.file.create(generations)
	case generationSourceS3:
		_, err := h.s3.client.Put(ctx, h.s3.key, renderGenerationList(generations), stores3.PutOptions{
			IfNoneMatch: "*",
			ContentType: "text/plain",
		})
		if errors.Is(err, stores3.ErrPreconditionFailed) {
			return errors.New("generation source is already initialised")
		}
		return err
	default:
		return errors.New("unknown generation source")
	}
}

type generationTransform func([]store.Generation) ([]store.Generation, error)

func appendGeneration(current []store.Generation, generation store.Generation) ([]store.Generation, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return nil, err
	}
	for _, existing := range current {
		if existing == generation {
			return nil, fmt.Errorf("generation %q already exists", generation)
		}
	}
	return append(current, generation), nil
}

func removeGeneration(current []store.Generation, target store.Generation) ([]store.Generation, error) {
	index := -1
	for i, generation := range current {
		if generation == target {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, fmt.Errorf("generation %q is not in the source list", target)
	}
	if index == len(current)-1 {
		return nil, fmt.Errorf("cannot purge the last (write) generation %q", target)
	}
	return append(current[:index], current[index+1:]...), nil
}

func (h *generationSourceHandle) mutate(ctx context.Context, transform generationTransform) error {
	switch h.kind {
	case generationSourceConfig:
		return errors.New("generation config source is read-only; update the main YAML")
	case generationSourceFile:
		current, err := h.file.Load(ctx)
		if err != nil {
			return err
		}
		next, err := transform(cloneGenerations(current))
		if err != nil {
			return err
		}
		if err := store.ValidateGenerations(next); err != nil {
			return err
		}
		return h.file.replace(next)
	case generationSourceS3:
		for attempt := 0; attempt < generationCASRetries; attempt++ {
			current, etag, err := h.s3.loadWithETag(ctx)
			if err != nil {
				return err
			}
			next, err := transform(cloneGenerations(current))
			if err != nil {
				return err
			}
			if err := store.ValidateGenerations(next); err != nil {
				return err
			}
			_, err = h.s3.client.Put(ctx, h.s3.key, renderGenerationList(next), stores3.PutOptions{
				IfMatch:     etag,
				ContentType: "text/plain",
			})
			if errors.Is(err, stores3.ErrPreconditionFailed) {
				continue
			}
			if err != nil {
				return fmt.Errorf("update generation object: %w", err)
			}
			return nil
		}
		return fmt.Errorf("generation s3 update exceeded %d CAS retries", generationCASRetries)
	default:
		return errors.New("unknown generation source")
	}
}

func (h *generationSourceHandle) remove(ctx context.Context) error {
	switch h.kind {
	case generationSourceConfig:
		return errors.New("generation config source is read-only; main YAML left unchanged")
	case generationSourceFile:
		if err := os.Remove(h.file.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return syncGenerationDirectory(filepath.Dir(h.file.path))
	case generationSourceS3:
		return h.s3.client.Delete(ctx, h.s3.key)
	default:
		return errors.New("unknown generation source")
	}
}

func (s *fileGenerationSource) create(generations []store.Generation) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return errors.New("generation source is already initialised")
		}
		return err
	}
	success := false
	defer func() {
		file.Close()
		if !success {
			_ = os.Remove(s.path)
		}
	}()
	if _, err := file.Write(renderGenerationList(generations)); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	success = true
	return syncGenerationDirectory(filepath.Dir(s.path))
}

func (s *fileGenerationSource) replace(generations []store.Generation) error {
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, ".generations-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}
	if err := temp.Chmod(0o644); err != nil {
		cleanup()
		return err
	}
	if _, err := temp.Write(renderGenerationList(generations)); err != nil {
		cleanup()
		return err
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return syncGenerationDirectory(directory)
}

func syncGenerationDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
