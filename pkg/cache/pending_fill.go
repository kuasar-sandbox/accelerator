package cache

import (
	"context"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type fillKey struct {
	partition store.Partition
	key       store.ContentKey
}

// pendingFills indexes the successful Blobs already owned by actual Fill
// goroutines for one stable target. It is not a read-flight table: misses never
// wait here, and a record cannot outlive its Fill. The mutex protects Clone
// against retirement of that same Blob handle; payload I/O happens outside it.
type pendingFills struct {
	mu    sync.Mutex
	blobs map[fillKey]Blob
}

// begin returns the fill goroutine's owned handle, or nil if an equivalent fill
// is already in progress. A nil receiver preserves independent fill behavior.
func (f *pendingFills) begin(p store.Partition, key store.ContentKey, blob Blob) Blob {
	if f == nil {
		return blob.Clone()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := fillKey{partition: p, key: key}
	if f.blobs[k] != nil {
		return nil
	}
	if f.blobs == nil {
		f.blobs = make(map[fillKey]Blob)
	}
	cloned := blob.Clone()
	f.blobs[k] = cloned
	return cloned
}

// acquire gives a caller its own handle to an already successful payload. A
// normal tier hit bypasses this table entirely; no record means keep reading.
func (f *pendingFills) acquire(ctx context.Context, p store.Partition, key store.ContentKey) (Blob, error) {
	if f == nil {
		return nil, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	blob := f.blobs[fillKey{partition: p, key: key}]
	if blob == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return blob.Clone(), nil
}

func (f *pendingFills) finish(p store.Partition, key store.ContentKey, blob Blob) {
	if f != nil {
		f.mu.Lock()
		delete(f.blobs, fillKey{partition: p, key: key})
		f.mu.Unlock()
	}
	// Removal excludes any future Clone of this handle. Previously returned
	// handles retain their own references, independently of fill completion.
	blob.Release()
}
