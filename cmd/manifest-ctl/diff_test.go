package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// Run the actual CLI path in a subprocess so fatal errors remain observable.
func TestManifestDiffHelperProcess(t *testing.T) {
	if os.Getenv("KUASAR_MANIFEST_DIFF_HELPER") != "1" {
		return
	}
	cmdDiff(os.Args[len(os.Args)-2:])
	os.Exit(0)
}

func diffFixture(t *testing.T, entries []codec.ChunkEntry, hole uint64) string {
	t.Helper()
	m := &codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFastCDC,
		MinChunkSize: 1, MaxChunkSize: 1 << 20, Entries: append([]codec.ChunkEntry(nil), entries...)}
	for i := range m.Entries {
		m.Entries[i].Offset = m.ImageSize
		m.ImageSize += uint64(m.Entries[i].Size)
	}
	if hole > 0 {
		m.Holes = []sparse.Extent{{Offset: m.ImageSize, Size: hole}}
		m.ImageSize += hole
	}
	data, err := codec.Marshal(m, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fixture.manifest")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runDiff(t *testing.T, a, b string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestManifestDiffHelperProcess$", "--", a, b)
	cmd.Env = append(os.Environ(), "KUASAR_MANIFEST_DIFF_HELPER=1")
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func TestManifestDiffUniqueHashesAndSymmetry(t *testing.T) {
	x := codec.ChunkEntry{Size: 4096, CiphertextHash: [32]byte{1}}
	a := codec.ChunkEntry{Size: 8192, CiphertextHash: [32]byte{2}}
	b := codec.ChunkEntry{Size: 16384, CiphertextHash: [32]byte{3}}
	zero := codec.ChunkEntry{Size: 4096, CiphertextHash: x.CiphertextHash, IsZero: true}
	pathA := diffFixture(t, []codec.ChunkEntry{x, x, a, zero}, 4096)
	pathB := diffFixture(t, []codec.ChunkEntry{x, x, x, b, b, zero}, 8192)
	for _, reverse := range []bool{false, true} {
		left, right, sizeA, sizeB := pathA, pathB, "8.0 KiB", "16.0 KiB"
		if reverse {
			left, right, sizeA, sizeB = right, left, sizeB, sizeA
		}
		output, err := runDiff(t, left, right)
		if err != nil {
			t.Fatalf("diff: %v\n%s", err, output)
		}
		for _, want := range []string{
			"shared:      1 chunks (4.0 KiB)",
			"only in A:   1 chunks (" + sizeA + ")",
			"only in B:   1 chunks (" + sizeB + ")",
			"dedup ratio: 25.0%",
		} {
			if !strings.Contains(output, want+"\n") {
				t.Errorf("reverse=%v missing %q:\n%s", reverse, want, output)
			}
		}
		if strings.Count(output, "(2 unique)") != 2 {
			t.Errorf("unique count includes duplicates or zero entries:\n%s", output)
		}
	}
}

func TestManifestDiffEmptyChunkSets(t *testing.T) {
	zero := diffFixture(t, []codec.ChunkEntry{{Size: 4096, IsZero: true}}, 4096)
	holes := diffFixture(t, nil, 8192)
	empty := diffFixture(t, nil, 0)
	for _, pair := range [][2]string{{zero, holes}, {holes, zero}, {empty, empty}} {
		output, err := runDiff(t, pair[0], pair[1])
		if err != nil {
			t.Fatalf("diff: %v\n%s", err, output)
		}
		for _, prefix := range []string{"shared:      ", "only in A:   ", "only in B:   "} {
			if !strings.Contains(output, prefix+"0 chunks (0 B)\n") {
				t.Errorf("nonempty chunk set: %s", output)
			}
		}
		if !strings.Contains(output, "dedup ratio: 0.0%\n") {
			t.Errorf("empty ratio is not zero: %s", output)
		}
	}
}

func TestManifestDiffRejectsConflictingHashSizes(t *testing.T) {
	x := codec.ChunkEntry{Size: 4096, CiphertextHash: [32]byte{1}}
	conflict := x
	conflict.Size = 8192
	one := diffFixture(t, []codec.ChunkEntry{x}, 0)
	other := diffFixture(t, []codec.ChunkEntry{conflict}, 0)
	within := diffFixture(t, []codec.ChunkEntry{x, conflict}, 0)
	for _, pair := range [][2]string{{one, other}, {other, one}, {within, one}, {one, within}} {
		output, err := runDiff(t, pair[0], pair[1])
		if err == nil || !strings.Contains(output, "conflicting sizes") {
			t.Errorf("expected conflicting-size rejection, got err=%v output=%s", err, output)
		}
	}
}
