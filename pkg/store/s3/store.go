package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/kuasar-sandbox/accelerator/internal/util/optrace"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

const defaultMaxObjectSize = 16 << 20

var (
	ErrKeyMismatch       = errors.New("s3: content key mismatch")
	ErrCommitKeyMismatch = errors.New("s3: commit key differs from open key")
)

// Config controls only the S3 object data plane. Generation list metadata is
// configured and loaded independently by store-ctl.
type Config struct {
	Bucket        string
	Prefix        string
	MaxInflight   int
	OpTimeout     time.Duration
	MaxObjectSize int64
}

// Store operates on exactly the generation supplied to each method.
type Store struct {
	client  s3Client
	prefix  string
	maxSize int64
	timeout time.Duration
	sem     chan struct{}
	tr      *optrace.Tracer
}

// New constructs a data-plane Store without loading generation metadata. The
// context parameter is retained for source compatibility and is not stored.
func New(_ context.Context, client s3Client, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("s3: nil s3 client")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 64
	}
	if cfg.MaxObjectSize <= 0 {
		cfg.MaxObjectSize = defaultMaxObjectSize
	}
	return &Store{
		client:  client,
		prefix:  normalisePrefix(cfg.Prefix),
		maxSize: cfg.MaxObjectSize,
		timeout: cfg.OpTimeout,
		sem:     make(chan struct{}, cfg.MaxInflight),
		tr:      optrace.FromEnv("store-ctl"),
	}, nil
}

func (s *Store) Get(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return false, nil, fmt.Errorf("s3: %w", err)
	}
	body, _, err := s.boundedGet(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
		return s.client.Get(ctx, s.objectKey(partition, generation, key))
	})
	if errors.Is(err, ErrNotFound) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("s3: get generation=%s: %w", generation, err)
	}
	return true, body, nil
}

func (s *Store) Exists(ctx context.Context, generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (bool, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return false, fmt.Errorf("s3: %w", err)
	}
	var expected int64
	if expectedSize != nil {
		expected = *expectedSize
		if expected < 0 {
			return false, fmt.Errorf("s3: negative expected size %d", expected)
		}
	}
	meta, err := s.boundedHead(ctx, func(ctx context.Context) (*ObjectMeta, error) {
		return s.client.Head(ctx, s.objectKey(partition, generation, key))
	})
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("s3: head generation=%s: %w", generation, err)
	}
	if meta == nil {
		return false, fmt.Errorf("s3: head generation=%s returned no metadata", generation)
	}
	if expectedSize != nil && meta.Size != expected {
		return false, nil
	}
	return true, nil
}

func (s *Store) OpenPut(generation store.Generation, partition store.Partition, key store.ContentKey, expectedSize *int64) (store.PutHandle, error) {
	if err := store.ValidateGeneration(generation); err != nil {
		return nil, fmt.Errorf("s3: %w", err)
	}
	h := &putHandle{
		store:      s,
		generation: generation,
		partition:  partition,
		key:        key,
		ctx:        context.Background(),
	}
	if expectedSize != nil {
		if *expectedSize < 0 {
			return nil, fmt.Errorf("s3: negative expected size %d", *expectedSize)
		}
		h.hasExpected = true
		h.expected = *expectedSize
	}
	return h, nil
}

func (s *Store) objectKey(partition store.Partition, generation store.Generation, key store.ContentKey) string {
	hexKey := hex.EncodeToString(key[:])
	return path.Join(s.prefix, string(partition), string(generation), hexKey[:2], hexKey[2:4], hexKey)
}

func (s *Store) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, s.timeout)
}

func (s *Store) boundedGet(parent context.Context, fn func(context.Context) ([]byte, *ObjectMeta, error)) ([]byte, *ObjectMeta, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return nil, nil, parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("s3.get")()
	ctx, cancel := s.opContext(parent)
	defer cancel()
	body, meta, err := fn(ctx)
	if err == nil && meta != nil && meta.Size > s.maxSize {
		return nil, nil, fmt.Errorf("s3: object size %d exceeds max %d", meta.Size, s.maxSize)
	}
	return body, meta, err
}

func (s *Store) boundedHead(parent context.Context, fn func(context.Context) (*ObjectMeta, error)) (*ObjectMeta, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return nil, parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("s3.head")()
	ctx, cancel := s.opContext(parent)
	defer cancel()
	return fn(ctx)
}

func (s *Store) boundedPut(parent context.Context, key string, body []byte, opts PutOptions) (string, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return "", parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("s3.put")()
	ctx, cancel := s.opContext(parent)
	defer cancel()
	return s.client.Put(ctx, key, body, opts)
}

func normalisePrefix(prefix string) string { return strings.Trim(prefix, "/") }
