// Package obs implements a store-ctl Backend backed by S3-compatible
// object storage (used in production with OBS via its
// S3-compatible endpoint).
//
// The package is intentionally split across three concerns so the
// core logic can be unit-tested without dragging in a real S3 SDK:
//
//   - client.go (this file): the minimal s3Client interface that
//     hides whichever SDK we end up using and the small data types
//     it needs (ObjectMeta, PutOptions, sentinel errors).
//   - obs.go:    Store implementation — generations meta load/init,
//     Get/Put/Exists/OpenPut, generation-walk on Get,
//     defensive wrapper (timeout + semaphore + size limit).
//   - puthandle.go: streaming-Put session that buffers in memory
//     and uploads on Commit via s3Client.Put.
//
// Production code wires *s3client.Client (a thin aws-sdk-go-v2/s3
// adapter living in a sub-package so it doesn't pollute this
// package's dependency footprint) into the s3Client interface; tests
// inject a fake.
package obs

import (
	"context"
	"errors"
)

// Sentinel errors. The s3Client implementations normalise SDK-
// specific error types to these so the upper layer doesn't need to
// reflect on smithy.APIError or the backend's custom error types.
var (
	// ErrNotFound is returned by Head/Get when the key is absent.
	// Distinct from "transport failure" so callers can short-circuit
	// without misclassifying genuine 5xx as miss.
	ErrNotFound = errors.New("obs: object not found")

	// ErrPreconditionFailed maps to HTTP 412 — the conditional-put
	// header (If-None-Match: *  or  If-Match: <etag>) was violated.
	// Used for compare-and-swap on the generations meta object.
	ErrPreconditionFailed = errors.New("obs: precondition failed")
)

// ObjectMeta is the small subset of S3 HEAD response we use. The
// full SDK response includes 30+ fields; we only need size + ETag
// for content-length verification and CAS.
type ObjectMeta struct {
	Size int64
	ETag string // includes the surrounding quotes per S3 convention
}

// PutOptions carries optional conditional-put headers and the few
// content-* fields we need. Unset string fields = "no header sent".
type PutOptions struct {
	// IfNoneMatch == "*" maps to "If-None-Match: *" — succeed only
	// when the key does NOT already exist. Used at meta-init.
	IfNoneMatch string

	// IfMatch == <etag> maps to "If-Match: <etag>" — succeed only
	// when the current object still has this exact etag. Used at
	// meta-rotation to detect concurrent writers.
	IfMatch string

	// ContentType is the application-level MIME hint. Optional.
	ContentType string
}

// s3Client is the storage-side dependency the obs.Store relies on.
// Real production wiring uses an aws-sdk-go-v2/s3-backed implementation
// (sub-package pkg/store/obs/s3client). Tests inject a fake.
//
// Implementations MUST:
//
//   - return obs.ErrNotFound for HEAD/GET on a missing key (NOT a
//     wrapped SDK error)
//   - return obs.ErrPreconditionFailed for 412 from PUT
//   - respect ctx cancellation/deadline (don't trust SDK auto-timeout)
//   - close any response body before returning to caller
type s3Client interface {
	// Head returns the object's metadata or ErrNotFound.
	Head(ctx context.Context, key string) (*ObjectMeta, error)

	// Get returns the full body and the metadata. Implementations
	// must enforce the configured maxObjectSize (defensive bound
	// against an unexpectedly large object exhausting memory).
	Get(ctx context.Context, key string) ([]byte, *ObjectMeta, error)

	// Put writes body under key. Returns the new object's etag on
	// success. opts.IfNoneMatch / IfMatch enforce CAS semantics
	// when set.
	Put(ctx context.Context, key string, body []byte, opts PutOptions) (etag string, err error)

	// List walks objects under prefix in lexicographic order,
	// invoking visit with each key. The bool return from visit lets
	// callers stop early; List propagates that as nil. Implementations
	// must paginate transparently — store-ctl never sees raw S3
	// continuation tokens.
	List(ctx context.Context, prefix string, visit func(key string) bool) error

	// Delete removes a single key. ErrNotFound on absent keys is OK
	// (idempotent); other errors propagate.
	Delete(ctx context.Context, key string) error
}
