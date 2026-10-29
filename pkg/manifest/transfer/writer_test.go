package transfer

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

type endpointTarget struct {
	endpoint string
	putCalls int
}

func (t *endpointTarget) AdmitWrite(context.Context) (store.WriteAdmission, error) {
	return store.WriteAdmission{}, nil
}

func (t *endpointTarget) Put(context.Context, store.WriteAdmission, store.Partition, store.ContentKey, []byte) (bool, error) {
	t.putCalls++
	return true, nil
}

func (t *endpointTarget) DedupIndexEndpoint() string { return t.endpoint }

func (t *endpointTarget) ValidateDedupIndexMarker(context.Context, store.ContentKey) (bool, error) {
	return true, nil
}

func TestTransferWriterTargetOnlyIsUnorderedAndSkipsDuplicatePut(t *testing.T) {
	target := &endpointTarget{endpoint: "unix:///test/store"}
	writer := &transferWriter{admission: store.WriteAdmission{Generation: "G1"}, target: target}
	if writer.RequiresOrderedChunks() {
		t.Fatal("target-only transfer reported ordered chunks")
	}
	if got := writer.DedupIndexEndpoint(); got != target.endpoint {
		t.Fatalf("endpoint = %q, want %q", got, target.endpoint)
	}
	key := store.ContentKey{1, 2, 3}
	skipped, err := writer.SkipChunkOrdered(context.Background(), writer.admission, key, 42, 7)
	if err != nil || !skipped {
		t.Fatalf("SkipChunkOrdered = %v, %v", skipped, err)
	}
	if target.putCalls != 0 {
		t.Fatalf("target Put calls = %d, want 0", target.putCalls)
	}
}

func TestTransferWriterWithLocalBundleRequiresOrdering(t *testing.T) {
	writer := &transferWriter{
		admission: store.WriteAdmission{Generation: "G1"},
		local:     &bundle.Writer{},
		target:    &endpointTarget{endpoint: "unix:///test/store"},
	}
	if !writer.RequiresOrderedChunks() {
		t.Fatal("bundle-backed transfer did not require ordered chunks")
	}
	if got := writer.DedupIndexEndpoint(); got != "" {
		t.Fatalf("bundle-backed transfer persistent endpoint = %q, want empty", got)
	}
	configured := &transferWriter{admission: store.WriteAdmission{Generation: "G1"}, ordered: true}
	if !configured.RequiresOrderedChunks() {
		t.Fatal("configured bundle transfer did not require ordered chunks before local setup")
	}
}

func TestTransferWriterSkipChecksContext(t *testing.T) {
	writer := &transferWriter{admission: store.WriteAdmission{Generation: "G1"}, target: &endpointTarget{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	skipped, err := writer.SkipChunkOrdered(ctx, writer.admission, store.ContentKey{1}, 1, 0)
	if !errors.Is(err, context.Canceled) || skipped {
		t.Fatalf("SkipChunkOrdered canceled = %v, %v; want false, context.Canceled", skipped, err)
	}
}
