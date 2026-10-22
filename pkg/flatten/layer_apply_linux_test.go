//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

type layerEntry struct {
	name, body, link string
	kind             byte
	mode             int64
	xattrs           map[string]string
}

func testLayerTar(t *testing.T, entries ...layerEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for _, e := range entries {
		kind, mode := e.kind, e.mode
		if kind == 0 {
			kind = tar.TypeReg
		}
		if mode == 0 {
			mode = 0644
		}
		h := &tar.Header{Name: e.name, Typeflag: kind, Mode: mode, Uid: os.Getuid(), Gid: os.Getgid(), Linkname: e.link, Xattrs: e.xattrs}
		if kind == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Size > 0 {
			if _, err := io.WriteString(tw, e.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func applyTestLayer(t *testing.T, root string, entries ...layerEntry) {
	t.Helper()
	b := testLayerTar(t, entries...)
	body := &observedBody{Reader: bytes.NewReader(b)}
	if err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{}); err != nil {
		t.Fatal(err)
	}
	if body.closed.Load() != 1 {
		t.Fatal("body close count")
	}
}

func assertLayerFile(t *testing.T, root, name, want string) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(root, name))
	if err != nil || string(got) != want {
		t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
	}
}

func TestUmociLateWhiteouts(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root,
		layerEntry{name: "d", kind: tar.TypeDir, mode: 0755},
		layerEntry{name: "d/lower", body: "old"},
		layerEntry{name: "replace", body: "old"})
	applyTestLayer(t, root,
		layerEntry{name: "d/upper", body: "new"},
		layerEntry{name: "replace", body: "new"},
		layerEntry{name: "d/.wh..wh..opq"},
		layerEntry{name: ".wh.replace"})
	assertLayerFile(t, root, "d/upper", "new")
	assertLayerFile(t, root, "replace", "new")
	if _, err := os.Lstat(filepath.Join(root, "d/lower")); !os.IsNotExist(err) {
		t.Fatalf("opaque retained lower entry: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, ".wh.replace")); !os.IsNotExist(err) {
		t.Fatal("whiteout leaked")
	}
}

func TestUmociDirectoriesLinksAndReplacement(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root,
		layerEntry{name: "d/lower", body: "keep"}, // implicit parent
		layerEntry{name: "original", body: "old", mode: 0640},
		layerEntry{name: "alias", kind: tar.TypeLink, link: "original", mode: 0640},
		layerEntry{name: "to-dir", body: "old"},
		layerEntry{name: "to-file/child", body: "old"})
	a, _ := os.Stat(filepath.Join(root, "original"))
	b, _ := os.Stat(filepath.Join(root, "alias"))
	if !os.SameFile(a, b) {
		t.Fatal("hardlink identity was lost")
	}
	applyTestLayer(t, root,
		layerEntry{name: "d", kind: tar.TypeDir, mode: 01777},
		layerEntry{name: "original", body: "new", mode: 0600},
		layerEntry{name: "to-dir", kind: tar.TypeDir, mode: 0755},
		layerEntry{name: "to-dir/child", body: "new"},
		layerEntry{name: "to-file", body: "new"},
		layerEntry{name: "sym", kind: tar.TypeSymlink, link: "to-dir/child"})
	for name, want := range map[string]string{"d/lower": "keep", "original": "new", "alias": "old", "to-dir/child": "new", "to-file": "new", "sym": "new"} {
		assertLayerFile(t, root, name, want)
	}
	st, err := os.Stat(filepath.Join(root, "d"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode()&(os.ModeSticky|os.ModePerm) != os.ModeSticky|0777 {
		t.Fatalf("directory mode %v", st.Mode())
	}
}

func TestUmociXattrsAndFIFO(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root,
		layerEntry{name: "payload", body: "data", xattrs: map[string]string{"user.kuasar-test": "value"}},
		layerEntry{name: "fifo", kind: tar.TypeFifo, mode: 0600})
	value := make([]byte, 32)
	n, err := unix.Lgetxattr(filepath.Join(root, "payload"), "user.kuasar-test", value)
	if err != nil || string(value[:n]) != "value" {
		t.Fatalf("xattr: %q %v", value[:max(n, 0)], err)
	}
	st, err := os.Lstat(filepath.Join(root, "fifo"))
	if err != nil || st.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("FIFO: %v %v", st, err)
	}
	// umoci deliberately omits host-specific SELinux labels. The adapter
	// must return an explicit error rather than silently claiming preservation.
	b := testLayerTar(t, layerEntry{name: "label", body: "x", xattrs: map[string]string{"security.selinux": "not-a-host-policy"}})
	body := &observedBody{Reader: bytes.NewReader(b)}
	err = applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{})
	if err == nil || !strings.Contains(err.Error(), "xattr") {
		t.Fatalf("silent xattr omission: %v", err)
	}
}

func TestUmociEmptyLayerAndMetadataBudget(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root)
	b := testLayerTar(t, layerEntry{name: "parent/child", body: "payload"})
	body := &observedBody{Reader: bytes.NewReader(b)}
	err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: 1})
	if err == nil || !strings.Contains(err.Error(), "metadata budget") {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("budget checked after application")
	}
	if body.closed.Load() != 1 {
		t.Fatal("body leaked on budget error")
	}
}

// Repeated headers, PAX data and shared ancestors are not retained per entry.
// The previous cumulative accounting exhausted this budget after a few entries.
func TestUmociMetadataBudgetReusesTransientAndDuplicatePaths(t *testing.T) {
	root := t.TempDir()
	entries := make([]layerEntry, 200)
	for i := range entries {
		entries[i] = layerEntry{name: "shared/parent/file", body: fmt.Sprint(i), xattrs: map[string]string{"user.budget": strings.Repeat("x", 512)}}
	}
	b := testLayerTar(t, entries...)
	body := &observedBody{Reader: bytes.NewReader(b)}
	if err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: (8 << 20) + (16 << 10)}); err != nil {
		t.Fatal(err)
	}
	assertLayerFile(t, root, "shared/parent/file", "199")
	if body.closed.Load() != 1 {
		t.Fatal("body leaked")
	}
}

func TestUmociMetadataBudgetBoundsSharedAncestorBacking(t *testing.T) {
	for _, allowGrowth := range []bool{false, true} {
		t.Run(fmt.Sprintf("allow-growth=%v", allowGrowth), func(t *testing.T) {
			root := t.TempDir()
			short := "shared/x"
			long := "shared/" + strings.Repeat("n", 80)
			shortAbsolute := filepath.Join(root, short)
			longAbsolute := filepath.Join(root, long)
			// Leave room for the first entry and its ancestor, the new leaf,
			// and the current header. Reassigning "shared" from the longer
			// entry must also be admitted before that entry reaches umoci.
			limit := int64(8<<20) + int64(2*(256+2*len(shortAbsolute))) +
				int64(256+2*len(longAbsolute)) + int64(2048+len(long))
			if allowGrowth {
				limit += int64(2 * (len(longAbsolute) - len(shortAbsolute)))
			}
			b := testLayerTar(t,
				layerEntry{name: short, body: "first"},
				layerEntry{name: long, body: "second"})
			body := &observedBody{Reader: bytes.NewReader(b)}
			err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: limit})
			if allowGrowth {
				if err != nil {
					t.Fatal(err)
				}
				assertLayerFile(t, root, long, "second")
			} else {
				if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
					t.Fatalf("shared ancestor backing escaped budget: %v", err)
				}
				if _, err := os.Stat(longAbsolute); !os.IsNotExist(err) {
					t.Fatalf("entry applied before admitting ancestor backing: %v", err)
				}
			}
			assertLayerFile(t, root, short, "first")
			if body.closed.Load() != 1 {
				t.Fatal("body leaked")
			}
		})
	}
}

func TestUmociMetadataBudgetStillBoundsUniquePaths(t *testing.T) {
	root := t.TempDir()
	var entries []layerEntry
	for i := range 200 {
		entries = append(entries, layerEntry{name: fmt.Sprintf("shared/parent/file-%03d", i), body: "x"})
	}
	b := testLayerTar(t, entries...)
	body := &observedBody{Reader: bytes.NewReader(b)}
	err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: (8 << 20) + (16 << 10)})
	if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
		t.Fatalf("unique paths escaped budget: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "shared/parent/file-199")); !os.IsNotExist(err) {
		t.Fatal("extracted past exhausted metadata budget")
	}
	if body.closed.Load() != 1 {
		t.Fatal("body leaked")
	}
}

func TestUmociMetadataBudgetChecksBeforeApplyingEntry(t *testing.T) {
	root := t.TempDir()
	b := testLayerTar(t, layerEntry{name: "file", body: "x"})
	body := &observedBody{Reader: bytes.NewReader(b)}
	err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: (8 << 20) + 2048})
	if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("budget checked after extraction")
	}
}

// Reusing one archive name through different symlink parents still retains
// distinct resolved upperPaths in umoci and must consume distinct budget.
func TestUmociMetadataBudgetFollowsSymlinkParents(t *testing.T) {
	root := t.TempDir()
	budget, err := newLayerMetadataBudget(9 << 20)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"first", "second"} {
		if err := os.Mkdir(filepath.Join(root, target), 0755); err != nil {
			t.Fatal(err)
		}
		_ = os.Remove(filepath.Join(root, "alias"))
		if err := os.Symlink(target, filepath.Join(root, "alias")); err != nil {
			t.Fatal(err)
		}
		if err := budget.reserve(root, &tar.Header{Name: "alias/file"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"first/file", "second/file"} {
		if _, ok := budget.paths[want]; !ok {
			t.Fatalf("resolved path %q was not charged", want)
		}
	}
	if _, ok := budget.paths["alias/file"]; ok {
		t.Fatal("budget used unresolved archive path")
	}
}

func TestUmociMetadataBudgetBoundsRetargetedSymlinkParents(t *testing.T) {
	root := t.TempDir()
	var entries []layerEntry
	for i := range 100 {
		target := fmt.Sprintf("target-%03d", i)
		// Lower directories are not archive entries and do not themselves
		// consume this layer's path-table budget.
		if err := os.Mkdir(filepath.Join(root, target), 0755); err != nil {
			t.Fatal(err)
		}
		entries = append(entries,
			layerEntry{name: "alias", kind: tar.TypeSymlink, link: target},
			layerEntry{name: "alias/file", body: "x"})
	}
	b := testLayerTar(t, entries...)
	body := &observedBody{Reader: bytes.NewReader(b)}
	err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: (8 << 20) + (16 << 10)})
	if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
		t.Fatalf("retargeted parent escaped budget: %v", err)
	}
	assertLayerFile(t, root, "target-000/file", "x")
	if _, err := os.Stat(filepath.Join(root, "target-099/file")); !os.IsNotExist(err) {
		t.Fatal("all resolved targets escaped the path-table limit")
	}
	if body.closed.Load() != 1 {
		t.Fatal("body leaked on symlink budget error")
	}
}

func TestUmociMetadataBudgetBoundsSinglePAXHeader(t *testing.T) {
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	hdr := &tar.Header{Name: "file", Mode: 0644, PAXRecords: map[string]string{"example.com/description": strings.Repeat("x", 8192)}}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	b := archive.Bytes()
	body := &observedBody{Reader: bytes.NewReader(b)}
	err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: (8 << 20) + (8 << 10)})
	if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
		t.Fatalf("oversized current header escaped budget: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "file")); !os.IsNotExist(err) {
		t.Fatal("entry extracted before admitting header memory")
	}
	if body.closed.Load() != 1 {
		t.Fatal("body leaked on PAX budget error")
	}
}

func TestUmociMetadataBudgetWhiteoutsAreTransient(t *testing.T) {
	for _, opaque := range []bool{false, true} {
		t.Run(fmt.Sprintf("opaque=%v", opaque), func(t *testing.T) {
			root := t.TempDir()
			var entries []layerEntry
			for i := range 200 {
				if opaque {
					dir := fmt.Sprintf("dir-%03d", i)
					if err := os.Mkdir(filepath.Join(root, dir), 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(root, dir, "lower"), nil, 0600); err != nil {
						t.Fatal(err)
					}
					entries = append(entries, layerEntry{name: dir + "/.wh..wh..opq"})
				} else {
					name := fmt.Sprintf("file-%03d", i)
					if err := os.WriteFile(filepath.Join(root, name), nil, 0600); err != nil {
						t.Fatal(err)
					}
					entries = append(entries, layerEntry{name: ".wh." + name})
				}
			}
			b := testLayerTar(t, entries...)
			body := &observedBody{Reader: bytes.NewReader(b)}
			if err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: (8 << 20) + (4 << 10)}); err != nil {
				t.Fatal(err)
			}
			for i := range 200 {
				name := fmt.Sprintf("file-%03d", i)
				if opaque {
					name = fmt.Sprintf("dir-%03d/lower", i)
				}
				if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
					t.Fatalf("whiteout did not remove %s: %v", name, err)
				}
			}
			if body.closed.Load() != 1 {
				t.Fatal("body leaked")
			}
		})
	}
}

func TestUmociMetadataBudgetResetsPerLayer(t *testing.T) {
	root := t.TempDir()
	for i := range 3 {
		var entries []layerEntry
		for j := range 15 {
			entries = append(entries, layerEntry{name: fmt.Sprintf("layer-%d/file-%02d", i, j), body: "x"})
		}
		b := testLayerTar(t, entries...)
		body := &observedBody{Reader: bytes.NewReader(b)}
		if err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: (8 << 20) + (16 << 10)}); err != nil {
			t.Fatalf("layer %d: %v", i, err)
		}
	}
	for i := range 3 {
		assertLayerFile(t, root, fmt.Sprintf("layer-%d/file-14", i), "x")
	}
}
