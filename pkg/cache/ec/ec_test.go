package ec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kuasar-sandbox/sandbox-accelerator/pkg/store"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	enc, err := newEncoder(4, 1)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name string
		data []byte
	}{
		{"small", []byte("hello")},
		{"exact-multiple", bytes.Repeat([]byte("A"), 256)},
		{"odd-size", bytes.Repeat([]byte("B"), 1000)},
		{"large-256k", bytes.Repeat([]byte("C"), 256*1024)},
		{"single-byte", []byte{0x42}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			shards, err := enc.Encode(tc.data)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(shards) != 5 {
				t.Fatalf("expected 5 shards, got %d", len(shards))
			}

			got, err := enc.Decode(shards)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !bytes.Equal(got, tc.data) {
				t.Fatalf("roundtrip mismatch: got %d bytes, want %d", len(got), len(tc.data))
			}
		})
	}
}

func TestDecodeOneMissing(t *testing.T) {
	enc, err := newEncoder(4, 1)
	if err != nil {
		t.Fatal(err)
	}

	original := bytes.Repeat([]byte("X"), 1024)
	shards, err := enc.Encode(original)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate losing each shard one at a time.
	for missing := 0; missing < 5; missing++ {
		t.Run(fmt.Sprintf("missing-%d", missing), func(t *testing.T) {
			damaged := make([][]byte, 5)
			for i := range shards {
				if i == missing {
					damaged[i] = nil
				} else {
					damaged[i] = append([]byte{}, shards[i]...)
				}
			}

			got, err := enc.Decode(damaged)
			if err != nil {
				t.Fatalf("Decode with shard %d missing: %v", missing, err)
			}
			if !bytes.Equal(got, original) {
				t.Fatalf("mismatch with shard %d missing", missing)
			}
		})
	}
}

func TestDecodeTwoMissing(t *testing.T) {
	enc, err := newEncoder(4, 1)
	if err != nil {
		t.Fatal(err)
	}

	original := bytes.Repeat([]byte("Y"), 512)
	shards, err := enc.Encode(original)
	if err != nil {
		t.Fatal(err)
	}

	// Lose 2 shards — should fail (only 1 parity).
	damaged := make([][]byte, 5)
	for i := range shards {
		damaged[i] = append([]byte{}, shards[i]...)
	}
	damaged[0] = nil
	damaged[1] = nil

	_, err = enc.Decode(damaged)
	if err == nil {
		t.Fatal("expected error with 2 missing shards, got nil")
	}
}

func TestRouterLocateN(t *testing.T) {
	peers := []Peer{
		{ID: "a", Endpoint: "10.0.1.1:7070"},
		{ID: "b", Endpoint: "10.0.1.2:7070"},
		{ID: "c", Endpoint: "10.0.1.3:7070"},
		{ID: "d", Endpoint: "10.0.1.4:7070"},
		{ID: "e", Endpoint: "10.0.1.5:7070"},
	}
	router, err := newRouter(peers)
	if err != nil {
		t.Fatal(err)
	}

	var testKey store.ContentKey
	copy(testKey[:], []byte("test-key-padding-to-32-bytes!!!!"))
	ids, err := router.LocateN(testKey, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 5 {
		t.Fatalf("expected 5 peers, got %d", len(ids))
	}

	// All IDs should be distinct.
	seen := make(map[string]bool)
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate peer: %s", id)
		}
		seen[id] = true
		if router.Endpoint(id) == "" {
			t.Fatalf("no endpoint for peer %s", id)
		}
	}
}

func TestRouterStability(t *testing.T) {
	peers := []Peer{
		{ID: "a", Endpoint: "10.0.1.1:7070"},
		{ID: "b", Endpoint: "10.0.1.2:7070"},
		{ID: "c", Endpoint: "10.0.1.3:7070"},
		{ID: "d", Endpoint: "10.0.1.4:7070"},
		{ID: "e", Endpoint: "10.0.1.5:7070"},
	}
	router, err := newRouter(peers)
	if err != nil {
		t.Fatal(err)
	}

	// Same key should always map to the same peers.
	var stableKey store.ContentKey
	copy(stableKey[:], []byte("stable-key-pad-to-32-bytes!!!!!"))
	ids1, _ := router.LocateN(stableKey, 5)
	ids2, _ := router.LocateN(stableKey, 5)
	for i := range ids1 {
		if ids1[i] != ids2[i] {
			t.Fatalf("unstable routing at index %d: %s vs %s", i, ids1[i], ids2[i])
		}
	}
}
