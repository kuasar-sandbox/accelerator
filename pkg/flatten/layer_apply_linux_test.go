//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

func TestLayerRootEntriesIgnored(t *testing.T) {
	// archive/tar's writer rejects regular names ending in a slash.
	tests := []struct {
		name string
		path string
		kind byte
	}{
		{"slash-directory", "/", tar.TypeDir},
		{"slash-symlink", "/", tar.TypeSymlink},
		{"slash-hardlink", "/", tar.TypeLink},
		{"dot-directory", ".", tar.TypeDir},
		{"dot-symlink", ".", tar.TypeSymlink},
		{"dot-hardlink", ".", tar.TypeLink},
		{"dot-slash-directory", "./", tar.TypeDir},
		{"dot-slash-symlink", "./", tar.TypeSymlink},
		{"dot-slash-hardlink", "./", tar.TypeLink},
		{"slashes-directory", "///", tar.TypeDir},
		{"slashes-symlink", "///", tar.TypeSymlink},
		{"slashes-hardlink", "///", tar.TypeLink},
		{"slash-dot-directory", "/./", tar.TypeDir},
		{"slash-dot-symlink", "/./", tar.TypeSymlink},
		{"slash-dot-hardlink", "/./", tar.TypeLink},
		{"dots-directory", "././", tar.TypeDir},
		{"dots-symlink", "././", tar.TypeSymlink},
		{"dots-hardlink", "././", tar.TypeLink},
		{"dot-regular", ".", tar.TypeReg},
		{"empty-regular", "", tar.TypeReg},
		{"parent-regular", "..", tar.TypeReg},
		{"absolute-parent-directory", "/..", tar.TypeDir},
		{"cleanable-root-directory", "d/../", tar.TypeDir},
	}
	limits := []struct {
		name  string
		bytes int64
	}{
		{"default", 0},
		{"explicit", 128 << 20},
	}
	for _, tc := range tests {
		for _, limit := range limits {
			t.Run(tc.name+"/"+limit.name, func(t *testing.T) {
				root := t.TempDir()
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
				const key = "user.kuasar-root"
				if err := unix.Lsetxattr(root, key, []byte("keep"), 0); err != nil {
					t.Fatal(err)
				}
				before, err := os.Stat(root)
				if err != nil {
					t.Fatal(err)
				}
				var archive bytes.Buffer
				tw := tar.NewWriter(&archive)
				hdr := &tar.Header{Name: tc.path, Typeflag: tc.kind, Mode: 0700, Uid: 12345, Gid: 23456,
					Xattrs: map[string]string{key: "replace"}}
				if tc.kind == tar.TypeLink || tc.kind == tar.TypeSymlink {
					// Root entries intentionally skip the whole entry before
					// umoci handles Linkname or attempts to create the link.
					hdr.Linkname = "/not-a-root-replacement"
				}
				if tc.kind == tar.TypeReg {
					hdr.Size = int64(len("ignored payload"))
				}
				if err := tw.WriteHeader(hdr); err != nil {
					t.Fatal(err)
				}
				if hdr.Size > 0 {
					if _, err := io.WriteString(tw, "ignored payload"); err != nil {
						t.Fatal(err)
					}
				}
				if err := tw.WriteHeader(&tar.Header{Name: "payload", Mode: 0644, Size: 4, Uid: os.Getuid(), Gid: os.Getgid()}); err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(tw, "data"); err != nil {
					t.Fatal(err)
				}
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
				b := archive.Bytes()
				body := &observedBody{Reader: bytes.NewReader(b)}
				if err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: limit.bytes}); err != nil {
					t.Fatal(err)
				}
				assertLayerFile(t, root, "payload", "data")
				after, err := os.Stat(root)
				if err != nil {
					t.Fatal(err)
				}
				oldStat, newStat := before.Sys().(*syscall.Stat_t), after.Sys().(*syscall.Stat_t)
				if !os.SameFile(before, after) || before.Mode() != after.Mode() || oldStat.Uid != newStat.Uid || oldStat.Gid != newStat.Gid {
					t.Fatal("root entry changed staging directory identity, permissions or ownership")
				}
				value := make([]byte, 32)
				n, err := unix.Lgetxattr(root, key, value)
				if err != nil || string(value[:max(n, 0)]) != "keep" {
					t.Fatalf("root xattr = %q, %v", value[:max(n, 0)], err)
				}
				if body.closed.Load() != 1 {
					t.Fatal("body close count")
				}
			})
		}
	}
}

func TestLayerAbsolutePathsRemainRootRelative(t *testing.T) {
	root := t.TempDir()
	applyTestLayer(t, root,
		layerEntry{name: "///d/./original", body: "data"},
		layerEntry{name: "/hardlink", kind: tar.TypeLink, link: "/d/original"},
		layerEntry{name: "/alias", kind: tar.TypeSymlink, link: "/d"},
		layerEntry{name: "/alias/child", body: "scoped"},
		layerEntry{name: "/d/lower", body: "old"})
	assertLayerFile(t, root, "d/original", "data")
	assertLayerFile(t, root, "d/child", "scoped")
	a, err := os.Stat(filepath.Join(root, "d/original"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(root, "hardlink"))
	if err != nil || !os.SameFile(a, b) {
		t.Fatalf("absolute hardlink lost identity: %v", err)
	}
	if link, err := os.Readlink(filepath.Join(root, "alias")); err != nil || link != "/d" {
		t.Fatalf("symlink target changed: %q, %v", link, err)
	}
	applyTestLayer(t, root,
		layerEntry{name: "/d/upper", body: "new"},
		layerEntry{name: "/d/.wh..wh..opq"},
		layerEntry{name: "/.wh.hardlink"})
	assertLayerFile(t, root, "d/upper", "new")
	for _, name := range []string{"d/lower", "hardlink"} {
		if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("absolute whiteout retained %s: %v", name, err)
		}
	}
}

func TestLayerUmociPathNormalizationStaysWithinRoot(t *testing.T) {
	tests := []struct {
		name      string
		entries   []layerEntry
		want      map[string]string
		hardlink  string
		missing   string
		wantError bool
	}{
		{name: "relative-parent-name", entries: []layerEntry{{name: "../outside", body: "updated"}}, want: map[string]string{"outside": "updated"}},
		{name: "absolute-parent-name", entries: []layerEntry{{name: "/../outside", body: "updated"}}, want: map[string]string{"outside": "updated"}},
		{name: "relative-cleanable-name", entries: []layerEntry{{name: "d/../outside", body: "updated"}}, want: map[string]string{"outside": "updated"}},
		{name: "absolute-escape-name", entries: []layerEntry{{name: "/d/../../outside", body: "updated"}}, want: map[string]string{"outside": "updated"}},
		{name: "relative-parent-hardlink", entries: []layerEntry{{name: "alias", kind: tar.TypeLink, link: "../outside"}}, want: map[string]string{"alias": "inside"}, hardlink: "outside"},
		{name: "absolute-parent-hardlink", entries: []layerEntry{{name: "alias", kind: tar.TypeLink, link: "/../outside"}}, want: map[string]string{"alias": "inside"}, hardlink: "outside"},
		{name: "relative-cleanable-hardlink", entries: []layerEntry{{name: "alias", kind: tar.TypeLink, link: "d/../outside"}}, want: map[string]string{"alias": "inside"}, hardlink: "outside"},
		{name: "absolute-escape-hardlink", entries: []layerEntry{{name: "alias", kind: tar.TypeLink, link: "/d/../../outside"}}, want: map[string]string{"alias": "inside"}, hardlink: "outside"},
		{name: "relative-symlink-parent", entries: []layerEntry{{name: "scope", kind: tar.TypeSymlink, link: "../outside-dir"}, {name: "scope/sentinel", body: "updated"}}, want: map[string]string{"outside-dir/sentinel": "updated"}},
		{name: "absolute-symlink-parent", entries: []layerEntry{{name: "scope", kind: tar.TypeSymlink, link: "/../../outside-dir"}, {name: "scope/sentinel", body: "updated"}}, want: map[string]string{"outside-dir/sentinel": "updated"}},
		{name: "symlink-hardlink-parent", entries: []layerEntry{{name: "scope", kind: tar.TypeSymlink, link: "../outside-dir"}, {name: "alias", kind: tar.TypeLink, link: "scope/sentinel"}}, want: map[string]string{"alias": "inside"}, hardlink: "outside-dir/sentinel"},
		{name: "parent-whiteout", entries: []layerEntry{{name: "../.wh.outside"}}, missing: "outside"},
		{name: "symlink-whiteout", entries: []layerEntry{{name: "scope", kind: tar.TypeSymlink, link: "../outside-dir"}, {name: "scope/.wh.sentinel"}}, missing: "outside-dir/sentinel"},
		{name: "root-parent-hardlink", entries: []layerEntry{{name: "alias", kind: tar.TypeLink, link: "/.."}}, missing: "alias", wantError: true},
	}
	for _, tc := range tests {
		for _, limit := range []struct {
			name  string
			bytes int64
		}{{"default", 0}, {"explicit", 128 << 20}} {
			t.Run(tc.name+"/"+limit.name, func(t *testing.T) {
				parent := t.TempDir()
				root := filepath.Join(parent, "rootfs")
				if err := os.Mkdir(root, 0755); err != nil {
					t.Fatal(err)
				}
				// The same paths exist outside and inside the image, so both
				// accidental writes and accidental hardlink reads are observable.
				sentinels := []string{"outside", "outside-dir/sentinel"}
				before := make([]os.FileInfo, len(sentinels))
				for i, name := range sentinels {
					for _, base := range []string{parent, root} {
						target := filepath.Join(base, name)
						if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
							t.Fatal(err)
						}
						body := "inside"
						if base == parent {
							body = "outside sentinel"
						}
						if err := os.WriteFile(target, []byte(body), 0640); err != nil {
							t.Fatal(err)
						}
					}
					var err error
					before[i], err = os.Lstat(filepath.Join(parent, name))
					if err != nil {
						t.Fatal(err)
					}
				}
				entries := append([]layerEntry(nil), tc.entries...)
				entries = append(entries, layerEntry{name: "complete", body: "done"})
				b := testLayerTar(t, entries...)
				body := &observedBody{Reader: bytes.NewReader(b)}
				err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: limit.bytes})
				if (err != nil) != tc.wantError {
					t.Fatalf("apply error = %v; want error %v", err, tc.wantError)
				}
				if !tc.wantError {
					assertLayerFile(t, root, "complete", "done")
				}
				for name, want := range tc.want {
					assertLayerFile(t, root, name, want)
				}
				if tc.hardlink != "" {
					a, err := os.Stat(filepath.Join(root, tc.hardlink))
					if err != nil {
						t.Fatal(err)
					}
					alias, err := os.Stat(filepath.Join(root, "alias"))
					if err != nil || !os.SameFile(a, alias) {
						t.Fatalf("hardlink target is not the image's file: %v", err)
					}
				}
				if tc.missing != "" {
					if _, err := os.Lstat(filepath.Join(root, tc.missing)); !os.IsNotExist(err) {
						t.Fatalf("%s should be absent: %v", tc.missing, err)
					}
				}
				for i, name := range sentinels {
					assertLayerFile(t, parent, name, "outside sentinel")
					after, err := os.Lstat(filepath.Join(parent, name))
					if err != nil || !os.SameFile(before[i], after) || before[i].Mode() != after.Mode() {
						t.Fatalf("outside sentinel changed: %s: %v", name, err)
					}
				}
				if body.closed.Load() != 1 {
					t.Fatal("body close count")
				}
			})
		}
	}
}

func TestIgnoredRootStillVerifiesCompleteLayer(t *testing.T) {
	b := testLayerTar(t, layerEntry{name: ".", body: "ignored payload"})
	tests := []struct {
		name    string
		corrupt func(*LayerInput, *observedBody)
	}{
		{"digest", func(in *LayerInput, _ *observedBody) { in.Digest = layerHash(nil) }},
		{"diffid", func(in *LayerInput, _ *observedBody) { in.DiffID = layerHash(nil) }},
		{"truncated", func(_ *LayerInput, body *observedBody) { body.Reader = bytes.NewReader(b[:len(b)-1]) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := &observedBody{Reader: bytes.NewReader(b)}
			in := streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body)
			tc.corrupt(&in, body)
			if err := applyVerifiedLayer(context.Background(), in, t.TempDir(), LayerLimits{}); err == nil {
				t.Fatal("ignored root bypassed complete layer validation")
			}
			if body.closed.Load() != 1 {
				t.Fatal("body close count")
			}
		})
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

// Repeated directory headers have bounded retained state, but their old
// cumulative charge exceeded the implicit 128 MiB limit. Builders must not
// require a private library knob to admit this otherwise valid input.
func TestUmociDefaultMetadataBudgetDoesNotRejectRepeatedEntries(t *testing.T) {
	name := strings.Repeat("d/", 64) + "leaf"
	entries := make([]layerEntry, 6000, 6001)
	for i := range entries {
		entries[i] = layerEntry{name: name, kind: tar.TypeDir, mode: 0755}
	}
	entries = append(entries, layerEntry{name: "complete", body: "payload"})
	b := testLayerTar(t, entries...)
	for _, tc := range []struct {
		name      string
		budget    int64
		wantError bool
	}{
		{"default", 0, false},
		{"explicit", 128 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			body := &observedBody{Reader: bytes.NewReader(b)}
			err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), root, LayerLimits{MetadataBytes: tc.budget})
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
					t.Fatalf("explicit limit: %v", err)
				}
				if _, err := os.Stat(filepath.Join(root, "complete")); !os.IsNotExist(err) {
					t.Fatalf("entry beyond explicit budget was applied: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertLayerFile(t, root, "complete", "payload")
			}
			if body.closed.Load() != 1 {
				t.Fatal("body close count")
			}
		})
	}
}

func TestLayerMetadataBudgetNormalization(t *testing.T) {
	for _, n := range []int64{0, 128 << 20, 512 << 20} {
		got, err := (LayerLimits{MetadataBytes: n}).normalized()
		if err != nil || got.MetadataBytes != n {
			t.Fatalf("metadata %d: got %d, %v", n, got.MetadataBytes, err)
		}
		if got.DecodedBytes != 128<<30 || got.DecoderBytes != 64<<20 {
			t.Fatalf("unrelated layer limits changed: %+v", got)
		}
	}
	if _, err := (LayerLimits{MetadataBytes: -1}).normalized(); err == nil {
		t.Fatal("negative explicit metadata limit accepted")
	}
}
