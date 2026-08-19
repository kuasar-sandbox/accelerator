package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
)

// fakeS3 is an in-memory s3Client for unit tests. It models just
// enough of S3 for our use:
//
//   - per-key body + etag map
//   - HEAD/GET return ErrNotFound on miss
//   - PUT supports If-None-Match: *  (create-only) and If-Match: <etag>
//     (CAS); both surface ErrPreconditionFailed on violation
//   - call counters for lazy assertions in tests
//
// Concurrency: a single mutex is enough — tests don't drive enough
// volume to need finer locking, and we want deterministic ordering
// for the CAS tests.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]fakeObject
	hits    struct {
		head atomic.Int64
		get  atomic.Int64
		put  atomic.Int64
	}
	// failNext lets a test inject one transport failure to verify
	// retry / error-propagation paths.
	failNextGet  error
	failNextHead error
	failNextPut  error
	headHook     func(context.Context)
}

type fakeObject struct {
	body []byte
	etag string
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string]fakeObject{}}
}

func (f *fakeS3) Head(ctx context.Context, key string) (*ObjectMeta, error) {
	f.hits.head.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.headHook != nil {
		f.headHook(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.failNextHead != nil {
		err := f.failNextHead
		f.failNextHead = nil
		return nil, err
	}
	obj, ok := f.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return &ObjectMeta{Size: int64(len(obj.body)), ETag: obj.etag}, nil
}

func (f *fakeS3) Get(_ context.Context, key string) ([]byte, *ObjectMeta, error) {
	f.hits.get.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNextGet != nil {
		err := f.failNextGet
		f.failNextGet = nil
		return nil, nil, err
	}
	obj, ok := f.objects[key]
	if !ok {
		return nil, nil, ErrNotFound
	}
	body := append([]byte(nil), obj.body...)
	return body, &ObjectMeta{Size: int64(len(obj.body)), ETag: obj.etag}, nil
}

func (f *fakeS3) Put(_ context.Context, key string, body []byte, opts PutOptions) (string, error) {
	f.hits.put.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNextPut != nil {
		err := f.failNextPut
		f.failNextPut = nil
		return "", err
	}
	existing, ok := f.objects[key]
	if opts.IfNoneMatch == "*" && ok {
		return "", ErrPreconditionFailed
	}
	if opts.IfMatch != "" {
		if !ok || existing.etag != opts.IfMatch {
			return "", ErrPreconditionFailed
		}
	}
	etag := computeETag(body)
	f.objects[key] = fakeObject{
		body: append([]byte(nil), body...),
		etag: etag,
	}
	return etag, nil
}

// computeETag mimics S3 etag-of-non-multipart-object: hex of MD5
// over the body. Tests compare etags for identity, not contents,
// so any deterministic per-bytes function works; using sha256 + hex
// avoids pulling in crypto/md5.
func computeETag(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%q", hex.EncodeToString(sum[:8]))
}

func (f *fakeS3) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.objects))
	for k := range f.objects {
		out = append(out, k)
	}
	return out
}

// List implements s3Client. Iterates the in-memory map filtered by
// prefix. Order is lexicographic to match real S3 ListObjects.
func (f *fakeS3) List(_ context.Context, prefix string, visit func(key string) bool) error {
	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if prefix == "" || hasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	f.mu.Unlock()
	// Sort for determinism (keys() above doesn't sort; real S3 does).
	sortStrings(keys)
	for _, k := range keys {
		if !visit(k) {
			return nil
		}
	}
	return nil
}

// Delete implements s3Client.
func (f *fakeS3) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func hasPrefix(s, p string) bool {
	if len(s) < len(p) {
		return false
	}
	for i := 0; i < len(p); i++ {
		if s[i] != p[i] {
			return false
		}
	}
	return true
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
