package obs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
	"github.com/kuasar-sandbox/sandbox-accelerator/internal/util/optrace"
)

// metaGenerationsKey is the in-bucket path of the generations meta
// object. Mirrors the fs backend's __meta/generations file.
const metaGenerationsKey = "__meta/generations"

// defaultMaxObjectSize caps a single Get/Put. 16 MiB covers all our
// CDC-max chunks (1 MiB) and manifest blobs (typically <1 MiB) with
// generous headroom; an unexpectedly larger object is a sign of
// corruption or misconfiguration, not a legitimate workload.
const defaultMaxObjectSize = 16 << 20

// ErrKeyMismatch is returned by PutHandle.Commit when the
// caller-claimed key disagrees with the server's re-hash.
var ErrKeyMismatch = errors.New("obs: key mismatch")

// ErrUninitialised is returned by New when the bucket prefix has no
// __meta/generations object. Recovery: run `store-ctl init`.
var ErrUninitialised = errors.New("obs: store uninitialised (run `store-ctl init`)")

// ErrAlreadyInitialised is returned by Init when the bucket prefix
// already carries an __meta/generations object. Init never overwrites
// existing meta — caller must Wipe first.
var ErrAlreadyInitialised = errors.New("obs: store already initialised")

// Config controls Store construction. The schema mirrors fs.Config
// — generation lifecycle is owned by store-ctl admin commands
// (init / rollout / purge), not by serve config — plus the
// OBS-specific endpoint/auth and tunables for the defensive wrapper.
type Config struct {
	// Bucket is the OBS bucket name. Required.
	Bucket string

	// Prefix is an optional in-bucket key prefix (e.g. "store/")
	// allowing multi-tenant sharing. Trailing slash optional;
	// normalised internally.
	Prefix string

	// VerifyKey controls whether Put re-hashes the incoming data
	// and rejects key-mismatch.
	VerifyKey bool

	// MaxInflight bounds concurrent S3 calls. 0 → 64.
	MaxInflight int

	// OpTimeout is the per-call wall-clock budget enforced by the
	// Store on top of the caller's ctx. 0 → 10 seconds. Strictly
	// shorter than gRPC server timeouts so OBS stalls surface as
	// store-ctl errors rather than gRPC deadline exceeded.
	OpTimeout time.Duration

	// MaxObjectSize bounds Get response bytes. 0 → 16 MiB.
	MaxObjectSize int64

	// MetaCASRetries caps the CAS retry loop on Rollout / Drop. 0 → 5.
	MetaCASRetries int
}

// Store is an obs-backed implementation of server.Backend. Safe for
// concurrent use; sem channel + per-call ctx timeout protect against
// runaway calls.
type Store struct {
	client    s3Client
	bucket    string
	prefix    string // never trailing-slash internally; joined per call
	verifyKey bool

	maxObjSize int64
	opTimeout  time.Duration
	sem        chan struct{}
	metaTries  int
	tr         *optrace.Tracer

	mu       sync.RWMutex
	gens     []string // newest-first
	active   string
	metaETag string // last observed; used for If-Match on Rollout / Drop
}

// New opens an existing OBS-backed store. The bucket+prefix must
// already carry a valid __meta/generations object — call Init first
// for a fresh bucket. Returns ErrUninitialised when the meta object
// is absent so callers can give a clear remediation hint.
//
// The ctx scopes only the bootstrap (meta load); steady-state calls
// use their own caller-supplied contexts.
func New(ctx context.Context, client s3Client, cfg Config) (*Store, error) {
	s, err := newStore(client, cfg)
	if err != nil {
		return nil, err
	}
	if err := s.loadGenerations(ctx); err != nil {
		return nil, fmt.Errorf("obs: load generations: %w", err)
	}
	return s, nil
}

// Init creates a fresh OBS-backed store at cfg.Bucket+cfg.Prefix,
// writing a single-line __meta/generations object containing
// `generation`. Refuses to clobber an existing meta object — caller
// must Wipe first if intentional.
//
// The ctx scopes the single conditional PutObject call.
func Init(ctx context.Context, client s3Client, cfg Config, generation string) error {
	if generation == "" {
		return errors.New("obs: generation is required")
	}
	s, err := newStore(client, cfg)
	if err != nil {
		return err
	}
	body := renderGenerations([]string{generation})
	_, err = s.boundedPut(ctx, s.metaKey(), body, PutOptions{
		IfNoneMatch: "*",
		ContentType: "text/plain",
	})
	if errors.Is(err, ErrPreconditionFailed) {
		return fmt.Errorf("%w: %s/%s", ErrAlreadyInitialised, cfg.Bucket, s.prefix)
	}
	if err != nil {
		return fmt.Errorf("obs: init meta: %w", err)
	}
	return nil
}

// newStore constructs a Store with config defaults applied but does
// not contact OBS. Used by both New (which then loads meta) and
// Init (which then writes meta). Centralises the validation /
// defaulting so the two entry points stay symmetric.
func newStore(client s3Client, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("obs: nil s3 client")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("obs: bucket is required")
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 64
	}
	// cfg.OpTimeout <= 0 means "no per-op deadline": an op is bounded
	// only by the caller's context (cancellation / client disconnect),
	// not an arbitrary number. An operator opts into a finite budget
	// explicitly via obs.op_timeout. opWithTimeout() honours 0 = none.
	if cfg.MaxObjectSize <= 0 {
		cfg.MaxObjectSize = defaultMaxObjectSize
	}
	if cfg.MetaCASRetries <= 0 {
		cfg.MetaCASRetries = 5
	}
	return &Store{
		client:     client,
		bucket:     cfg.Bucket,
		prefix:     normalisePrefix(cfg.Prefix),
		verifyKey:  cfg.VerifyKey,
		maxObjSize: cfg.MaxObjectSize,
		opTimeout:  cfg.OpTimeout,
		sem:        make(chan struct{}, cfg.MaxInflight),
		metaTries:  cfg.MetaCASRetries,
		tr:         optrace.FromEnv("store-ctl"),
	}, nil
}

// ActiveGeneration returns the generation Put writes to.
func (s *Store) ActiveGeneration() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// Generations returns all known generations newest-first.
func (s *Store) Generations() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.gens))
	copy(out, s.gens)
	return out
}

// Get walks generations newest-first.
func (s *Store) Get(ctx context.Context, partition store.Partition, key store.ContentKey) (bool, []byte, error) {
	s.mu.RLock()
	gens := s.gens
	s.mu.RUnlock()

	for _, gen := range gens {
		body, _, err := s.bounded(ctx, func(ctx context.Context) ([]byte, *ObjectMeta, error) {
			return s.client.Get(ctx, s.objectKey(partition, gen, key))
		})
		if err == nil {
			return true, body, nil
		}
		if errors.Is(err, ErrNotFound) {
			continue
		}
		return false, nil, fmt.Errorf("obs: get gen=%s: %w", gen, err)
	}
	return false, nil, nil
}

// Put short-circuits on dedup, otherwise streams to a single
// PutObject via the in-memory PutHandle.
func (s *Store) Put(ctx context.Context, partition store.Partition, key store.ContentKey, data []byte) (bool, error) {
	if s.Exists(partition, key) {
		return false, nil
	}
	h, err := s.OpenPut(partition)
	if err != nil {
		return false, err
	}
	if _, err := h.Write(data); err != nil {
		_ = h.Abort()
		return false, err
	}
	verifyDigest := key
	if s.verifyKey {
		verifyDigest = store.ContentKey(sha256.Sum256(data))
	}
	return h.Commit(key, verifyDigest)
}

// Exists is a HEAD on the active-generation key only. Net cost is a
// single round-trip; cheaper than Get and avoids transferring data.
func (s *Store) Exists(partition store.Partition, key store.ContentKey) bool {
	active := s.ActiveGeneration()
	_, err := s.boundedHead(context.Background(), func(ctx context.Context) (*ObjectMeta, error) {
		return s.client.Head(ctx, s.objectKey(partition, active, key))
	})
	return err == nil
}

// OpenPut returns an in-memory streaming PutHandle. Writes accumulate
// in a bytes.Buffer; Commit invokes a single PutObject. The buffer
// is bounded by maxObjSize at Commit-time so a runaway client can't
// OOM the daemon.
func (s *Store) OpenPut(partition store.Partition) (store.PutHandle, error) {
	return &putHandle{
		store:     s,
		partition: partition,
	}, nil
}

// objectKey returns the in-bucket path for (partition, gen, key).
// Layout matches fs.Store's filesystem layout.
func (s *Store) objectKey(partition store.Partition, gen string, key store.ContentKey) string {
	h := hex.EncodeToString(key[:])
	return path.Join(s.prefix, string(partition), gen, h[:2], h[2:4], h)
}

// metaKey returns the in-bucket path of the generations meta object.
func (s *Store) metaKey() string {
	return path.Join(s.prefix, metaGenerationsKey)
}

// opCtx applies the per-op deadline only when one is configured.
// s.opTimeout <= 0 means "no deadline": the op is bounded solely by
// the caller's context (cancellation / client disconnect), never an
// arbitrary number.
func (s *Store) opCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if s.opTimeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, s.opTimeout)
}

// bounded wraps an s3 Get-style call with the semaphore + timeout.
// Returns whatever the inner call returns.
func (s *Store) bounded(parent context.Context, fn func(ctx context.Context) ([]byte, *ObjectMeta, error)) ([]byte, *ObjectMeta, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return nil, nil, parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("obs.get")()
	ctx, cancel := s.opCtx(parent)
	defer cancel()
	body, meta, err := fn(ctx)
	if err == nil && meta != nil && meta.Size > s.maxObjSize {
		return nil, nil, fmt.Errorf("obs: object size %d exceeds max %d", meta.Size, s.maxObjSize)
	}
	return body, meta, err
}

// boundedHead is the HEAD-only variant of bounded.
func (s *Store) boundedHead(parent context.Context, fn func(ctx context.Context) (*ObjectMeta, error)) (*ObjectMeta, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return nil, parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("obs.head")()
	ctx, cancel := s.opCtx(parent)
	defer cancel()
	return fn(ctx)
}

// boundedPut wraps the PutObject call with sem + timeout.
func (s *Store) boundedPut(parent context.Context, key string, body []byte, opts PutOptions) (string, error) {
	select {
	case s.sem <- struct{}{}:
	case <-parent.Done():
		return "", parent.Err()
	}
	defer func() { <-s.sem }()
	defer s.tr.Begin("obs.put")()
	ctx, cancel := s.opCtx(parent)
	defer cancel()
	return s.client.Put(ctx, key, body, opts)
}

// normalisePrefix strips any leading/trailing slash so path.Join
// produces a canonical key.
func normalisePrefix(p string) string {
	return strings.Trim(p, "/")
}

