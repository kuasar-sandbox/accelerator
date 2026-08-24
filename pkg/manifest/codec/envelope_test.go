package codec

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"testing"

	"github.com/golang/snappy"
)

func TestManifestEnvelopeRawAndSnappyRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		manifest *Manifest
		sealed   []byte
		encoding byte
	}{
		{
			name:     "raw",
			manifest: &Manifest{Version: Version1, ChunkMode: ChunkModeFixed},
			sealed:   bytes.Repeat([]byte{0xa7}, 31),
			encoding: ManifestEncodingRaw,
		},
		{
			name:     "snappy",
			manifest: repeatedManifest(4096),
			sealed:   bytes.Repeat([]byte{0x44}, 4096),
			encoding: ManifestEncodingSnappy,
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

func TestCanonicalManifestGoldenVectors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		m        *Manifest
		sealed   []byte
		wantLen  int
		wantHash string
	}{
		{
			name:     "raw",
			m:        &Manifest{Version: Version1, ChunkMode: ChunkModeFixed},
			sealed:   []byte("golden-key-table"),
			wantLen:  81,
			wantHash: "370dfe11d9e94f7b912fc16aba44ba65299f2a5876916029989a58cd55a3925a",
		},
		{
			name:     "snappy",
			m:        repeatedManifest(100),
			sealed:   bytes.Repeat([]byte{0x5a}, 4096),
			wantLen:  758,
			wantHash: "790db113aecd8bae58651be4f1c51b98b36fbe662e3c744c0a62da6f97dde4f6",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physical, err := Marshal(tc.m, tc.sealed)
			if err != nil {
				t.Fatal(err)
			}
			if len(physical) != tc.wantLen {
				t.Fatalf("physical length = %d, want %d", len(physical), tc.wantLen)
			}
			hash := sha256.Sum256(physical)
			if got := hex.EncodeToString(hash[:]); got != tc.wantHash {
				t.Fatalf("physical hash = %s, want %s", got, tc.wantHash)
			}
		})
	}
}

func TestManifestEnvelopeRejectsLegacyAndMalformed(t *testing.T) {
	t.Parallel()
	actualLegacy := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(actualLegacy[:4], Magic)
	actualLegacy[4] = Version1
	intendedLegacy := make([]byte, HeaderSize)
	copy(intendedLegacy, "MANI")
	intendedLegacy[4] = Version1
	for _, legacy := range [][]byte{actualLegacy, intendedLegacy} {
		if _, _, err := Unmarshal(legacy); err == nil {
			t.Fatalf("legacy [magic][version] representation %x was accepted", legacy[:5])
		}
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "short magic", data: []byte("MAN")},
		{name: "unknown encoding", data: append([]byte("MANI"), 0xff)},
		{name: "truncated snappy", data: append([]byte("MANI\x01"), 0xff)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Unmarshal(tc.data); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestManifestEnvelopeRejectsRawDecodedSizeOverHardLimit(t *testing.T) {
	physical := make([]byte, MaxManifestDecodedSize+2)
	copy(physical, "MANI")
	physical[4] = ManifestEncodingRaw
	if _, _, err := Unmarshal(physical); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("error = %v, want ErrManifestTooLarge", err)
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

func TestManifestEnvelopeRejectsSnappyLengthMismatchAndCorruption(t *testing.T) {
	t.Parallel()
	payload := make([]byte, HeaderSize-4)
	payload[0] = Version1
	encoded := snappy.Encode(nil, payload)
	encoded = encoded[:len(encoded)-1]
	physical := append([]byte("MANI\x01"), encoded...)
	if _, _, err := Unmarshal(physical); err == nil {
		t.Fatal("corrupt Snappy payload was accepted")
	}
}

func TestManifestEnvelopeRejectsShortSnappyBeforeLogicalParse(t *testing.T) {
	physical := append([]byte("MANI\x01"), snappy.Encode(nil, []byte{Version1})...)
	if _, _, err := Unmarshal(physical); !errors.Is(err, ErrTooShort) {
		t.Fatalf("error = %v, want ErrTooShort", err)
	}
}

func TestManifestEnvelopeRejectsNonCanonicalSnappyBenefit(t *testing.T) {
	raw, err := Marshal(&Manifest{Version: Version1, ChunkMode: ChunkModeFixed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if raw[4] != ManifestEncodingRaw {
		t.Fatalf("fixture encoding = %#x, want RAW", raw[4])
	}
	physical := append([]byte("MANI\x01"), snappy.Encode(nil, raw[5:])...)
	if _, _, err := Unmarshal(physical); !errors.Is(err, ErrBadEncoding) {
		t.Fatalf("error = %v, want ErrBadEncoding", err)
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

func TestManifestEnvelopeRejectsTableBoundsOverflowAndTrailingData(t *testing.T) {
	t.Parallel()
	base, err := Marshal(&Manifest{Version: Version1, ChunkMode: ChunkModeFixed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if base[4] != ManifestEncodingRaw {
		t.Fatalf("fixture encoding = %d, want RAW", base[4])
	}

	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "entry table out of bounds",
			mutate: func(data []byte) []byte {
				binary.LittleEndian.PutUint32(data[17:21], math.MaxUint32)
				return data
			},
		},
		{
			name: "hole table out of bounds",
			mutate: func(data []byte) []byte {
				binary.LittleEndian.PutUint32(data[45:49], math.MaxUint32)
				return data
			},
		},
		{
			name: "key table addition overflows",
			mutate: func(data []byte) []byte {
				binary.LittleEndian.PutUint64(data[37:45], math.MaxUint64)
				return data
			},
		},
		{
			name:   "trailing physical payload",
			mutate: func(data []byte) []byte { return append(data, 0) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physical := tc.mutate(append([]byte(nil), base...))
			if _, _, err := Unmarshal(physical); err == nil {
				t.Fatal("malformed table bounds were accepted")
			}
		})
	}
}

func TestManifestEnvelopeRejectsOverflowingEntryGeometry(t *testing.T) {
	t.Parallel()
	physical, err := Marshal(&Manifest{
		Version:      Version1,
		ChunkMode:    ChunkModeFixed,
		ImageSize:    4,
		MinChunkSize: 4,
		MaxChunkSize: 4,
		Entries:      []ChunkEntry{{Size: 4}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if physical[4] != ManifestEncodingRaw {
		t.Fatalf("fixture encoding = %d, want RAW", physical[4])
	}
	// A logical entry begins at offset 64; the physical envelope adds one
	// byte overall because it replaces four magic bytes with five bytes.
	binary.LittleEndian.PutUint64(physical[65:73], math.MaxUint64-1)
	if _, _, err := Unmarshal(physical); !errors.Is(err, ErrBadGeometry) {
		t.Fatalf("error = %v, want ErrBadGeometry", err)
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
