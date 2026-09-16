package remote

import (
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// TestReferrerArtifactShape pins the wire shape of the referrer artifact.
// Regressions here break real registries in ways the in-memory test registry
// cannot catch: a lost subject means compliant registries never index the
// referrer (the fallback tag hides it), and a lost empty layer means
// registries that require at least one layer (e.g. SWR) reject the manifest
// with MANIFEST_INVALID.
func TestReferrerArtifactShape(t *testing.T) {
	subject := v1.Descriptor{
		MediaType: types.OCIManifestSchema1,
		Digest:    v1.Hash{Algorithm: "sha256", Hex: "7a1734ab15ab0859f0c822a8dcca04a8fa6da006e019272affbc3f1cfacb6a14"},
		Size:      1571,
	}
	img, err := referrerArtifact(map[string]string{
		AnnOwner:   "owner-x test",
		AnnID:      "085b6760c6f3309ab0acf2b2ad2cf173a3ec272c73a26e8da69c60dfecc468d3",
		AnnValidAt: "2026-09-15T10:00:00Z 2026-09-16T10:00:00Z",
	}, subject)
	if err != nil {
		t.Fatal(err)
	}
	m, err := img.Manifest()
	if err != nil {
		t.Fatal(err)
	}

	// The subject must survive the mutation chain: mutate wrappers overwrite
	// Subject (mutate.image.compute assigns it unconditionally), so appending
	// the empty layer after Subject drops it.
	if m.Subject == nil {
		t.Fatal("subject lost: the artifact would never be indexed as a referrer")
	}
	if m.Subject.Digest != subject.Digest {
		t.Fatalf("subject digest = %v, want %v", m.Subject.Digest, subject.Digest)
	}

	// Exactly the OCI 1.1 empty descriptor layer, for registries that reject
	// zero-layer manifests.
	if len(m.Layers) != 1 {
		t.Fatalf("layers = %d, want exactly the empty descriptor layer", len(m.Layers))
	}
	layer := m.Layers[0]
	if want := "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"; layer.Digest.String() != want {
		t.Fatalf("empty layer digest = %v, want %v", layer.Digest, want)
	}
	if layer.MediaType != emptyLayerMediaType {
		t.Fatalf("empty layer media type = %v, want %v", layer.MediaType, emptyLayerMediaType)
	}
	if layer.Size != 2 {
		t.Fatalf("empty layer size = %d, want 2", layer.Size)
	}

	// OCI image manifest media type (subject semantics) and the artifactType
	// carried as the config media type.
	if m.MediaType != types.OCIManifestSchema1 {
		t.Fatalf("manifest media type = %v, want OCI image manifest", m.MediaType)
	}
	if m.Config.MediaType != types.MediaType(RefererArtifactType) {
		t.Fatalf("config media type = %v, want %v (artifactType carrier)", m.Config.MediaType, RefererArtifactType)
	}

	// Owner annotations must survive the chain.
	if m.Annotations[AnnOwner] != "owner-x test" {
		t.Fatalf("owner annotation = %q, want it preserved", m.Annotations[AnnOwner])
	}
}
