package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// putHandle buffers the streamed bytes in memory and uploads once
// on Commit. For our chunk sizes (≤1 MiB) and manifests (typically
// tens of KiB) the buffer never gets large; for the rare oversized
// payload Commit enforces the configured maxObjSize.
//
// The "buffer + single PutObject" approach is the right tradeoff
// for content-addressed stores: total bytes per Put are bounded by
// the chunker's max-chunk setting, so multipart upload's complexity
// (extra round-trips, AbortMultipartUpload cleanup paths) buys
// nothing here. If we ever land snapshot bytes via store-ctl
// directly (not chunked), revisit.
type putHandle struct {
	store     *Store
	partition store.Partition
	buf       bytes.Buffer
	committed bool
	aborted   bool
}

func (h *putHandle) Write(p []byte) (int, error) {
	if h.committed || h.aborted {
		return 0, errors.New("s3: write after commit/abort")
	}
	return h.buf.Write(p)
}

// Commit uploads the buffered bytes via a single PutObject.
// Order of checks:
//
//  1. handle state must be open
//  2. caller-claimed key must match verifyDigest (server-verified)
//  3. buffered size must be within maxObjSize
//  4. dedup short-circuit: HEAD active-generation key; hit → noop
//  5. PutObject; success → mark committed, return (true, nil)
//
// On any failure the handle is left in committed=true state with
// the buffer dropped, so a subsequent Abort is a no-op (matching
// the contract documented in store.PutHandle).
func (h *putHandle) Commit(key store.ContentKey, verifyDigest store.ContentKey) (bool, error) {
	if h.committed || h.aborted {
		return false, errors.New("s3: commit on already-terminated handle")
	}
	defer func() { h.committed = true; h.buf.Reset() }()

	if verifyDigest != key {
		return false, ErrKeyMismatch
	}
	if int64(h.buf.Len()) > h.store.maxObjSize {
		return false, fmt.Errorf("s3: payload size %d exceeds max %d", h.buf.Len(), h.store.maxObjSize)
	}

	// Last-chance dedup. Two writers racing on the same key both
	// reach Commit; whichever lands first wins, the other observes
	// the resulting object and short-circuits. (server-side Exists
	// short-circuit before OpenPut catches the easy case; this
	// guards the narrow "Exists missed → both opened → both
	// streamed" window.)
	if h.store.Exists(h.partition, key) {
		return false, nil
	}

	active := h.store.ActiveGeneration()
	objKey := h.store.objectKey(h.partition, active, key)
	body := append([]byte(nil), h.buf.Bytes()...)
	if _, err := h.store.boundedPut(context.Background(), objKey, body, PutOptions{
		ContentType: "application/octet-stream",
	}); err != nil {
		return false, fmt.Errorf("s3: put %s: %w", objKey, err)
	}
	return true, nil
}

func (h *putHandle) Abort() error {
	if h.committed {
		return nil
	}
	h.aborted = true
	h.buf.Reset()
	return nil
}

// computeDigest is a small helper kept here (not in s3.go) so
// puthandle's contract — "verifyDigest != key ⇒ reject" — stays
// next to the reject site. fs.Store keeps the same idiom.
func computeDigest(data []byte) store.ContentKey {
	return store.ContentKey(sha256.Sum256(data))
}

// silence — kept for future; remove with streaming Get.
var _ = computeDigest
