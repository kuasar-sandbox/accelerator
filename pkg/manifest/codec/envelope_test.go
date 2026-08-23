package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/klauspost/compress/s2"
)

func TestManifestEnvelopeRawAndS2RoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		manifest   *Manifest
		sealed     []byte
		encoding   byte
	}{
		{
			name: "raw",
			manifest: &Manifest{Version: Version1, ChunkMode: ChunkModeFixed},
			sealed: bytes.Repeat([]byte{0xa7}, 31),
			encoding: ManifestEncodingRaw,
		},
		{
			name: "s2",
			manifest: repeatedManifest(4096),
			sealed: bytes.Repeat([]byte{0x44}, 4096),
			encoding: ManifestEncodingS2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physical, err := Marshal(tc.manifest, tc.sealed)
			if err != nil {
				t.Fatal(err)
			}
			if string(physical[:4]) != "MANI" {
				t.Fatalf("magic = %q, want MANI", physical[:4])
			}
			if physical[4] != tc.encoding {
				t.Fatalf("encoding = %#x, want %#x", physical[4], tc.encoding)
			}
			got, gotSealed, err := Unmarshal(physical)
			if err != nil {
				t.Fatal(err)
			}
			if got.Version != tc.manifest.Version || got.ChunkMode != tc.manifest.ChunkMode || got.ImageSize != tc.manifest.ImageSize || len(got.Entries) != len(tc.manifest.Entries) {
				t.Fatalf("manifest mismatch: got %+v want %+v", got, tc.manifest)
			}
			if !bytes.Equal(gotSealed, tc.sealed) {
				t.Fatal("sealed key table mismatch")
			}
		})
	}
}

func TestManifestEnvelopeRejectsLegacyAndMalformed(t *testing.T) {
	t.Parallel()
	legacy := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(legacy[:4], Magic)
	legacy[4] = Version1
	if _, _, err := Unmarshal(legacy); err == nil {
		t.Fatal("legacy [magic][version] representation was accepted")
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "short magic", data: []byte("MAN")},
		{name: "unknown encoding", data: append([]byte("MANI"), 0xff)},
		{name: "truncated s2", data: append([]byte("MANI\x01"), 0xff)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Unmarshal(tc.data); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestManifestEnvelopeRejectsOversizedDecodedLength(t *testing.T) {
	t.Parallel()
	declared := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(declared, uint64(MaxManifestDecodedSize)+1)
	physical := append([]byte("MANI\x01"), declared[:n]...)
	if _, _, err := Unmarshal(physical); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("error = %v, want ErrManifestTooLarge", err)
	}
}

func TestManifestEnvelopeRejectsS2LengthMismatchAndCorruption(t *testing.T) {
	t.Parallel()
	payload := make([]byte, HeaderSize-4)
	payload[0] = Version1
	encoded := s2.Encode(nil, payload)
	encoded[len(encoded)-1] ^= 0xff
	physical := append([]byte("MANI\x01"), encoded...)
	if _, _, err := Unmarshal(physical); err == nil {
		t.Fatal("corrupt S2 payload was accepted")
	}
}

func TestManifestEntrySizeBounds(t *testing.T) {
	t.Parallel()
	m := &Manifest{
		Version:      Version1,
		ChunkMode:    ChunkModeFixed,
		ImageSize:    4096,
		MaxChunkSize: 2048,
		Entries:      []ChunkEntry{{Offset: 0, Size: 4096}},
	}
	if _, err := Marshal(m, nil); !errors.Is(err, ErrBadChunkSize) {
		t.Fatalf("Marshal error = %v, want ErrBadChunkSize", err)
	}
}

func repeatedManifest(chunks int) *Manifest {
	entries := make([]ChunkEntry, chunks)
	for i := range entries {
		entries[i] = ChunkEntry{Offset: uint64(i) * 4096, Size: 4096, CiphertextHash: [32]byte{0x42}}
	}
	return &Manifest{
		Version:      Version1,
		ChunkMode:    ChunkModeFixed,
		ImageSize:    uint64(chunks) * 4096,
		MinChunkSize: 4096,
		MaxChunkSize: 4096,
		Entries:      entries,
	}
}
