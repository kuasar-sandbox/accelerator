package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type putHandle struct {
	store       *Store
	generation  store.Generation
	partition   store.Partition
	key         store.ContentKey
	hasExpected bool
	expected    int64
	ctx         context.Context
	buf         bytes.Buffer
	terminated  bool
}

// SetContext lets the gRPC server bind backend work to the stream context
// without widening the shared PutHandle interface.
func (h *putHandle) SetContext(ctx context.Context) {
	if ctx != nil && !h.terminated {
		h.ctx = ctx
	}
}

func (h *putHandle) Write(p []byte) (int, error) {
	if h.terminated {
		return 0, errors.New("s3: write after commit/abort")
	}
	if int64(len(p)) > h.store.maxSize-int64(h.buf.Len()) {
		return 0, fmt.Errorf("s3: payload exceeds max object size %d", h.store.maxSize)
	}
	if h.hasExpected && int64(len(p)) > h.expected-int64(h.buf.Len()) {
		return 0, fmt.Errorf("s3: payload exceeds expected size %d", h.expected)
	}
	return h.buf.Write(p)
}

func (h *putHandle) Commit(key store.ContentKey, verifyDigest store.ContentKey) (bool, error) {
	if h.terminated {
		return false, errors.New("s3: commit on terminated handle")
	}
	h.terminated = true
	defer h.buf.Reset()
	if key != h.key {
		return false, ErrCommitKeyMismatch
	}
	if h.hasExpected && int64(h.buf.Len()) != h.expected {
		return false, fmt.Errorf("s3: payload size %d, expected %d", h.buf.Len(), h.expected)
	}
	if verifyDigest != h.key {
		return false, ErrKeyMismatch
	}

	expected := (*int64)(nil)
	if h.hasExpected {
		value := h.expected
		expected = &value
	}
	exists, err := h.store.Exists(h.ctx, h.generation, h.partition, h.key, expected)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	objectKey := h.store.objectKey(h.partition, h.generation, h.key)
	body := append([]byte(nil), h.buf.Bytes()...)
	if _, err := h.store.boundedPut(h.ctx, objectKey, body, PutOptions{ContentType: "application/octet-stream"}); err != nil {
		return false, fmt.Errorf("s3: put %s: %w", objectKey, err)
	}
	return true, nil
}

func (h *putHandle) Abort() error {
	if h.terminated {
		return nil
	}
	h.terminated = true
	h.buf.Reset()
	return nil
}
