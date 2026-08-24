package codec

import "testing"

func FuzzManifestUnmarshal(f *testing.F) {
	for _, m := range []*Manifest{
		{Version: Version1, ChunkMode: ChunkModeFixed},
		repeatedManifest(128),
	} {
		physical, err := Marshal(m, nil)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(physical)
	}
	f.Add([]byte("MANI\xff"))
	f.Add([]byte("INAM\x01"))

	f.Fuzz(func(t *testing.T, physical []byte) {
		if len(physical) > 2<<20 {
			t.Skip()
		}
		m, _, err := Unmarshal(physical)
		if err == nil {
			if err := m.ValidateGeometry(); err != nil {
				t.Fatalf("successful Unmarshal returned invalid geometry: %v", err)
			}
		}
	})
}
