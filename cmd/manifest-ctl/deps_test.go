package main

import (
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
)

// summarizeDeltas feeds the delta lines of manifest-ctl info.
// Three delta entries total 900 bytes. The repeated external base counts
// once, and the base that is also an entry is not external. A version 1
// manifest has no delta table, so its summary is zero.
func TestSummarizeDeltas(t *testing.T) {
	m := &codec.Manifest{Version: codec.Version2, Entries: []codec.ChunkEntry{
		{Offset: 0, Size: 100, CiphertextHash: [32]byte{1}},
		{Offset: 100, Size: 200, CiphertextHash: [32]byte{2}, IsDelta: true},
		{Offset: 300, Size: 300, CiphertextHash: [32]byte{3}, IsDelta: true},
		{Offset: 600, Size: 400, CiphertextHash: [32]byte{4}, IsDelta: true},
		{Offset: 1000, Size: 50, IsZero: true},
	}, Deltas: []codec.DeltaRef{
		{Entry: 1, BaseSize: 1000, Base: [32]byte{1}},    // base is also entry 0
		{Entry: 2, BaseSize: 2000, Base: [32]byte{0xB1}}, // outside the manifest
		{Entry: 3, BaseSize: 2000, Base: [32]byte{0xB1}}, // same base twice
	}, ImageSize: 1050}
	s := summarizeDeltas(m)
	if s.entries != 3 || s.logicalBytes != 900 {
		t.Fatalf("entries %d bytes %d", s.entries, s.logicalBytes)
	}
	if s.bases != 2 || s.externalBases != 1 || s.baseBytes != 3000 {
		t.Fatalf("bases %d external %d bytes %d", s.bases, s.externalBases, s.baseBytes)
	}
	if v1 := summarizeDeltas(&codec.Manifest{Version: codec.Version1}); v1 != (deltaSummary{}) {
		t.Fatalf("Version1 summary %+v", v1)
	}
}
