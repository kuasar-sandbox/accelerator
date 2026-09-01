package tarstream

import (
	stdtar "archive/tar"
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

type canonicalNameSource struct {
	sparse.Source
	want string
	got  string
}

func (s *canonicalNameSource) TarStreamDigest(name string) ([32]byte, bool) {
	s.got = name
	return [32]byte{1}, name == s.want
}

func (s *canonicalNameSource) PayloadCommitment() (uint64, [32]byte, bool) {
	return 0, [32]byte{}, false
}

func TestCarrierDigestNormalizesNameBeforeQueryingProvider(t *testing.T) {
	source := &canonicalNameSource{Source: sparse.Dense(bytes.NewReader(nil), 0), want: "snapshot"}
	if _, _, err := CarrierDigest("/./snapshot", source); err != nil {
		t.Fatal(err)
	}
	if source.got != "snapshot" {
		t.Fatalf("provider name = %q, want snapshot", source.got)
	}
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
		atOpenErr  error
	}{
		{name: "payload-boundary", bodyOffset: 7, atOpenErr: ErrInvalidCanonicalTarstream},
		{name: "payload-commitment", bodyOffset: 8, atOpenErr: ErrDigestMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact := append([]byte(nil), encoded.Bytes()...)
			markerBody := len(artifact) - 3*512
			artifact[markerBody+tc.bodyOffset] ^= 0x01
			_, _, atErr := SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "snapshot")
			if !errors.Is(atErr, tc.atOpenErr) {
				t.Fatalf("SourceAt error = %v, want %v", atErr, tc.atOpenErr)
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

func TestCanonicalCarrierRejectsPrecedingArchiveEntries(t *testing.T) {
	var carrier bytes.Buffer
	_, digest, err := WriteTo(context.Background(), &carrier, "snapshot", sparse.Dense(bytes.NewReader([]byte("payload")), 7))
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []stdtar.Header{
		{Name: "uncommitted", Mode: 0o644, Size: 1},
		{Name: "directory/", Typeflag: stdtar.TypeDir, Mode: 0o755},
		{Name: "link", Typeflag: stdtar.TypeSymlink, Linkname: "elsewhere", Mode: 0o777},
	} {
		t.Run(header.Name, func(t *testing.T) {
			var prefix bytes.Buffer
			writer := stdtar.NewWriter(&prefix)
			if err := writer.WriteHeader(&header); err != nil {
				t.Fatal(err)
			}
			if header.Size != 0 {
				if _, err := writer.Write(bytes.Repeat([]byte{'x'}, int(header.Size))); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}
			artifact := append(prefix.Bytes(), carrier.Bytes()...)
			if _, _, err := SourceAt(bytes.NewReader(artifact), int64(len(artifact)), "snapshot", WithExpectedDigest(DigestScheme, digest)); !errors.Is(err, ErrInvalidCanonicalTarstream) {
				t.Fatalf("SourceAt error = %v, want invalid canonical tarstream", err)
			}
			if _, _, err := SourceFrom(bytes.NewReader(artifact), "snapshot", WithExpectedDigest(DigestScheme, digest)); !errors.Is(err, ErrInvalidCanonicalTarstream) {
				t.Fatalf("SourceFrom error = %v, want invalid canonical tarstream", err)
			}
		})
	}
}
