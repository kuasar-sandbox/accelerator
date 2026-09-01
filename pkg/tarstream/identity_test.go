package tarstream

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type segmentedDenseSource struct {
	sparse.Source
	payloadSize uint64
}

func (s segmentedDenseSource) TarStreamDigest(string) ([32]byte, bool) { return [32]byte{}, false }
func (s segmentedDenseSource) PayloadCommitment() (uint64, [32]byte, bool) {
	return s.payloadSize, [32]byte{}, false
}

func TestCarrierDigestDerivesReplacementTailFromPayloadCommitment(t *testing.T) {
	payload := []byte("payload")
	oldTail := []byte("old-tail")
	newTail := []byte("replacement-tail")
	oldBody := append(append([]byte(nil), payload...), oldTail...)
	oldSource := segmentedDenseSource{
		Source: sparse.Dense(bytes.NewReader(oldBody), uint64(len(oldBody))), payloadSize: uint64(len(payload)),
	}
	var oldArtifact bytes.Buffer
	oldScheme, oldDigest, err := WriteTo(context.Background(), &oldArtifact, "snapshot", oldSource)
	if err != nil {
		t.Fatal(err)
	}
	opened, _, err := SourceAt(bytes.NewReader(oldArtifact.Bytes()), int64(oldArtifact.Len()), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	payloadSize, payloadDigest, ok := opened.(IdentityProvider).PayloadCommitment()
	if !ok || payloadSize != uint64(len(payload)) {
		t.Fatalf("payload commitment = %d/%t", payloadSize, ok)
	}
	derived, err := ComposeDigest("snapshot", uint64(len(payload)+len(newTail)), payloadSize, payloadDigest, newTail)
	if err != nil {
		t.Fatal(err)
	}

	newBody := append(append([]byte(nil), payload...), newTail...)
	newSource := segmentedDenseSource{
		Source: sparse.Dense(bytes.NewReader(newBody), uint64(len(newBody))), payloadSize: uint64(len(payload)),
	}
	var newArtifact bytes.Buffer
	_, actual, err := WriteTo(context.Background(), &newArtifact, "snapshot", newSource)
	if err != nil {
		t.Fatal(err)
	}
	if actual != digestHex(derived) {
		t.Fatalf("derived digest = %x, encoded digest = %s", derived, actual)
	}
	provider, ok := opened.(IdentityProvider)
	if !ok {
		t.Fatal("opened carrier does not provide its identity")
	}
	if _, ok := provider.TarStreamDigest("overlay"); ok {
		t.Fatal("carrier identity was reused for a different payload name")
	}
	scheme, digest, err := CarrierDigest("snapshot", opened)
	if err != nil || scheme != oldScheme || digest != oldDigest {
		t.Fatalf("CarrierDigest = %s:%s, %v; want %s:%s", scheme, digest, err, oldScheme, oldDigest)
	}
	scheme, digest, err = CarrierDigest("snapshot", opened, WithCodec(&optionTestCodec{}, false))
	if err != nil || scheme != DigestSchemeHMAC || digest != oldDigest {
		t.Fatalf("keyed CarrierDigest = %s:%s, %v", scheme, digest, err)
	}
}

func TestCarrierDigestRejectsSparseMetadataTail(t *testing.T) {
	body := make([]byte, 16)
	source, err := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), []sparse.Extent{{Offset: 12, Size: 4}})
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	_, _, err = WriteTo(context.Background(), &artifact, "snapshot", segmentedDenseSource{Source: source, payloadSize: 8})
	if err == nil {
		t.Fatal("WriteTo accepted a sparse metadata tail")
	}
}

func TestCarrierIdentityRejectsTamperedBoundaryAndCommitment(t *testing.T) {
	body := []byte("payload-metadata-tail")
	source := segmentedDenseSource{
		Source: sparse.Dense(bytes.NewReader(body), uint64(len(body))), payloadSize: uint64(len("payload")),
	}
	var encoded bytes.Buffer
	if _, _, err := WriteTo(context.Background(), &encoded, "snapshot", source); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		bodyOffset int
		atOpen     bool
	}{
		{name: "payload-boundary", bodyOffset: 7, atOpen: true},
		{name: "payload-commitment", bodyOffset: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact := append([]byte(nil), encoded.Bytes()...)
			markerBody := len(artifact) - 3*512
			artifact[markerBody+tc.bodyOffset] ^= 0x01
			_, _, atErr := SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "snapshot")
			if tc.atOpen && !errors.Is(atErr, ErrInvalidCanonicalTarstream) {
				t.Fatalf("SourceAt error = %v, want invalid canonical tarstream", atErr)
			}
			if !tc.atOpen && atErr != nil {
				t.Fatalf("SourceAt unexpectedly read payload commitment: %v", atErr)
			}
			opened, _, err := SourceFrom(bytes.NewReader(artifact), "snapshot")
			if err == nil {
				_, err = opened.ReadAt(context.Background(), make([]byte, len(body)), 0)
			}
			if !errors.Is(err, ErrDigestMismatch) && !errors.Is(err, ErrInvalidCanonicalTarstream) {
				t.Fatalf("full sequential validation error = %v", err)
			}
		})
	}
}
