package tarstream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestPlaintextWriteGolden(t *testing.T) {
	var sparseBody [16384]byte
	copy(sparseBody[:], "HEAD")
	copy(sparseBody[12288:], "TAIL")
	tests := []struct {
		name         string
		body         []byte
		holes        []sparse.Extent
		wantLen      int
		wantArtifact string
		wantDigest   string
	}{
		{name: "dense", body: []byte("hello"), wantLen: 3584, wantArtifact: "c62a59a4c823dded3d9dae58c26c32b46b90bddcd251c761b313aebdeee0bd2a", wantDigest: "07229965d664d8814acac7566dddf8bce2dceabaf0f01e08c2c867e247e56d50"},
		{name: "empty", wantLen: 3072, wantArtifact: "cf050789196f8d7323f416612172613c86f75c4d017d591e5abc070ad2e3890b", wantDigest: "5cf5d14f26c0b4a26a3fb367370d1a6bf5a2734d298e2425b9dd4a30443df025"},
		{name: "hole", body: make([]byte, 8192), holes: []sparse.Extent{{Offset: 0, Size: 8192}}, wantLen: 3584, wantArtifact: "db1c978f874851b35e47781715f25c97e78548db76f0e9d325ec6725ec858e67", wantDigest: "0e05d11945eca0faba1bce258636ec41be5781d0c63acd4dd9e5ecc9bc39beaa"},
		{name: "sparse/image", body: sparseBody[:], holes: []sparse.Extent{{Offset: 4096, Size: 8192}}, wantLen: 11776, wantArtifact: "28c77eacb535d820e57a0f63f825b9e51c8601db20d1b6c3cb4986c0a594fcec", wantDigest: "0b53dddf972680d23957f95ff7ebbfadd9c317c334840db7ff262d57f79b5d9d"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source, err := sparse.NewSource(bytes.NewReader(tc.body), uint64(len(tc.body)), tc.holes)
			if err != nil {
				t.Fatal(err)
			}
			var artifact bytes.Buffer
			scheme, digest, err := WriteTo(context.Background(), &artifact, tc.name, source)
			if err != nil {
				t.Fatal(err)
			}
			if scheme != DigestSchemeSHA256 || digest != tc.wantDigest {
				t.Fatalf("digest = %s:%s, want sha256:%s", scheme, digest, tc.wantDigest)
			}
			if artifact.Len() != tc.wantLen {
				t.Fatalf("artifact length = %d, want %d", artifact.Len(), tc.wantLen)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(artifact.Bytes())); got != tc.wantArtifact {
				t.Fatalf("artifact SHA-256 = %s, want %s", got, tc.wantArtifact)
			}
		})
	}
}
