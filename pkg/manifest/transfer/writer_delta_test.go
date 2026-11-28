package transfer

import (
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
)

// The below four methods is the fake store used for testing.
// aliasTarget is a store writer that records what is forwarded to it.
type aliasTarget struct {
	resolved   []store.ContentKey
	represents []store.ContentKey
}

func (*aliasTarget) AdmitWrite(context.Context) (store.WriteAdmission, error) {
	return store.WriteAdmission{Generation: "G1"}, nil
}
func (*aliasTarget) Put(context.Context, store.WriteAdmission, store.Partition, store.ContentKey, []byte) (bool, error) {
	return true, nil
}
func (a *aliasTarget) Resolve(_ context.Context, _ store.Generation, h store.ContentKey) (store.Resolved, error) {
	a.resolved = append(a.resolved, h)
	return store.Resolved{Resolution: store.ResolutionIndependent}, nil
}
func (a *aliasTarget) PutRepresenting(_ context.Context, _ store.WriteAdmission, _ store.ContentKey, _ []byte, r store.ContentKey) (store.PutResult, error) {
	a.represents = append(a.represents, r)
	return store.PutResult{IsNew: true, Represented: store.RepresentedWon}, nil
}

var (
	_ ingest.Resolver           = (*transferWriter)(nil)
	_ ingest.RepresentingWriter = (*transferWriter)(nil)
)

// TestTransferWriterForwardsAliasOperationsToTheStore checks that a writer with a
// store passes Resolve and PutRepresenting through unchanged. Resolve must reach
// the store with the chunk's H_ind. PutRepresenting must reach it with the
// chunk being represented, not the delta object's own key. A PutRepresenting
// under a different admission than the writer's must be refused before anything
// is forwarded.
func TestTransferWriterForwardsAliasOperationsToTheStore(t *testing.T) {
	target := &aliasTarget{}
	w := &transferWriter{admission: store.WriteAdmission{Generation: "G1"}, target: target}

	r, err := w.Resolve(context.Background(), "G1", store.ContentKey{1})
	if err != nil || r.Resolution != store.ResolutionIndependent || len(target.resolved) != 1 {
		t.Fatalf("Resolve = %+v, %v", r, err)
	}
	res, err := w.PutRepresenting(context.Background(), w.admission, store.ContentKey{2}, []byte("x"), store.ContentKey{3})
	if err != nil || res.Represented != store.RepresentedWon || len(target.represents) != 1 || target.represents[0] != (store.ContentKey{3}) {
		t.Fatalf("PutRepresenting = %+v, %v", res, err)
	}
	if _, err := w.PutRepresenting(context.Background(), store.WriteAdmission{Generation: "G2"}, store.ContentKey{2}, nil, store.ContentKey{3}); err == nil {
		t.Fatal("a changed admission must be refused")
	}
}

// TestTransferWriterWithoutStoreCannotCoordinate checks that a writer with no
// store, or a Bundle writer, refuses both calls. Resolve must return
// store.ErrUnsupported, which the ingester reads as "cannot coordinate" and turns
// delta compression off. PutRepresenting must fail, because there is no store to
// record the alias in and a Bundle must never create a new delta.
func TestTransferWriterWithoutStoreCannotCoordinate(t *testing.T) {
	for name, w := range map[string]*transferWriter{
		"no store": {},
		"bundle":   {target: &aliasTarget{}, bundle: true},
	} {
		if _, err := w.Resolve(context.Background(), "G1", store.ContentKey{1}); !errors.Is(err, store.ErrUnsupported) {
			t.Errorf("%s: Resolve err = %v, want ErrUnsupported", name, err)
		}
		if _, err := w.PutRepresenting(context.Background(), w.admission, store.ContentKey{2}, nil, store.ContentKey{3}); err == nil {
			t.Errorf("%s: PutRepresenting must fail", name)
		}
	}
}
