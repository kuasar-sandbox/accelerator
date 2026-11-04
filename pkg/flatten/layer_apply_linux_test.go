//go:build linux

package flatten

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

type layerEntry struct {
	name, body, link   string
	kind               byte
	mode               int64
	hasMode            bool
	uid, gid           int
	hasOwner           bool
	devmajor, devminor int64
	xattrs             map[string]string
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
		if mode == 0 && !e.hasMode {
			mode = 0644
		}
		uid, gid := os.Getuid(), os.Getgid()
		if e.hasOwner {
			uid, gid = e.uid, e.gid
		}
		h := &tar.Header{
			Name:     e.name,
			Typeflag: kind,
			Mode:     mode,
			Uid:      uid,
			Gid:      gid,
			Linkname: e.link,
			Devmajor: e.devmajor,
			Devminor: e.devminor,
			Xattrs:   e.xattrs,
		}
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

func newTestTree(t *testing.T) *virtualTree {
	t.Helper()
	tree := newVirtualTree(t.TempDir())
	t.Cleanup(func() {
		if err := tree.Close(); err != nil {
			t.Errorf("close virtual tree: %v", err)
		}
	})
	return tree
}

func (t *virtualTree) lstat(rel string) (*virtualInode, error) {
	clean, err := layerPath(rel)
	if err != nil {
		return nil, err
	}
	if clean == "." {
		return t.root, nil
	}
	parentName, baseName := path.Dir(clean), path.Base(clean)
	dir, _, err := t.resolveDirPath(parentName, false)
	if err != nil {
		return nil, err
	}
	if dir == nil {
		return nil, os.ErrNotExist
	}
	child := dir.children[baseName]
	if child == nil {
		return nil, os.ErrNotExist
	}
	return child, nil
}

func (t *virtualTree) stat(rel string) (*virtualInode, error) {
	clean, err := layerPath(rel)
	if err != nil {
		return nil, err
	}
	cur := clean
	for loops := 0; ; loops++ {
		if loops > maxSymlinkLoops {
			return nil, unix.ELOOP
		}
		v, err := t.lstat(cur)
		if err != nil {
			return nil, err
		}
		if v.mode&unix.S_IFMT != unix.S_IFLNK {
			return v, nil
		}
		target := v.linkTarget
		if strings.HasPrefix(target, "/") {
			cur = path.Clean(strings.TrimLeft(target, "/"))
		} else {
			_, canonicalDir, err := t.resolveDirPath(path.Dir(cur), false)
			if err != nil {
				return nil, err
			}
			var parts []string
			if canonicalDir != "." {
				parts = strings.Split(canonicalDir, "/")
			}
			for _, p := range strings.Split(target, "/") {
				switch p {
				case "", ".":
				case "..":
					if len(parts) > 0 {
						parts = parts[:len(parts)-1]
					}
				default:
					parts = append(parts, p)
				}
			}
			if len(parts) == 0 {
				cur = "."
			} else {
				cur = strings.Join(parts, "/")
			}
		}
	}
}

func (t *virtualTree) readFile(rel string) ([]byte, error) {
	v, err := t.stat(rel)
	if err != nil {
		return nil, err
	}
	if v.mode&unix.S_IFMT != unix.S_IFREG {
		return nil, unix.EISDIR
	}
	buf := make([]byte, v.size)
	n, err := t.readAt(context.Background(), v, buf, 0)
	if err != nil || uint64(n) != v.size {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func (t *virtualTree) getxattr(rel, key string) (string, error) {
	v, err := t.lstat(rel)
	if err != nil {
		return "", err
	}
	val, ok := v.xattrs[key]
	if !ok {
		return "", unix.ENODATA
	}
	return val, nil
}

func applyTestLayer(t *testing.T, tree *virtualTree, entries ...layerEntry) {
	t.Helper()
	b := testLayerTar(t, entries...)
	body := &observedBody{Reader: bytes.NewReader(b)}
	if err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), tree, LayerLimits{}); err != nil {
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

func assertVirtualFile(t *testing.T, tree *virtualTree, name, want string) {
	t.Helper()
	got, err := tree.readFile(name)
	if err != nil || string(got) != want {
		t.Fatalf("%s = %q, %v; want %q", name, got, err, want)
	}
}

func TestUmociLateWhiteouts(t *testing.T) {
	tree := newTestTree(t)
	applyTestLayer(t, tree,
		layerEntry{name: "d", kind: tar.TypeDir, mode: 0755},
		layerEntry{name: "d/lower", body: "old"},
		layerEntry{name: "replace", body: "old"})
	applyTestLayer(t, tree,
		layerEntry{name: "d/upper", body: "new"},
		layerEntry{name: "replace", body: "new"},
		layerEntry{name: "d/.wh..wh..opq"},
		layerEntry{name: ".wh.replace"})
	assertVirtualFile(t, tree, "d/upper", "new")
	assertVirtualFile(t, tree, "replace", "new")
	if _, err := tree.lstat("d/lower"); !os.IsNotExist(err) {
		t.Fatalf("opaque retained lower entry: %v", err)
	}
	if _, err := tree.lstat(".wh.replace"); !os.IsNotExist(err) {
		t.Fatal("whiteout leaked")
	}
}

func TestUmociDirectoriesLinksAndReplacement(t *testing.T) {
	tree := newTestTree(t)
	applyTestLayer(t, tree,
		layerEntry{name: "d/lower", body: "keep"}, // implicit parent
		layerEntry{name: "original", body: "old", mode: 0640},
		layerEntry{name: "alias", kind: tar.TypeLink, link: "original", mode: 0640},
		layerEntry{name: "to-dir", body: "old"},
		layerEntry{name: "to-file/child", body: "old"})
	a, err := tree.stat("original")
	if err != nil {
		t.Fatal(err)
	}
	b, err := tree.stat("alias")
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a.ino != b.ino || a.nlink != 2 {
		t.Fatal("hardlink identity was lost")
	}
	applyTestLayer(t, tree,
		layerEntry{name: "d", kind: tar.TypeDir, mode: 01777},
		layerEntry{name: "original", body: "new", mode: 0600},
		layerEntry{name: "to-dir", kind: tar.TypeDir, mode: 0755},
		layerEntry{name: "to-dir/child", body: "new"},
		layerEntry{name: "to-file", body: "new"},
		layerEntry{name: "sym", kind: tar.TypeSymlink, link: "to-dir/child"})
	for name, want := range map[string]string{"d/lower": "keep", "original": "new", "alias": "old", "to-dir/child": "new", "to-file": "new", "sym": "new"} {
		assertVirtualFile(t, tree, name, want)
	}
	v, err := tree.stat("d")
	if err != nil {
		t.Fatal(err)
	}
	st := v.stat()
	n := nodeMetadata(&st)
	if n.Mode&(fs.ModeSticky|fs.ModePerm) != fs.ModeSticky|0777 {
		t.Fatalf("directory mode %v", n.Mode)
	}
}

func TestUmociXattrsAndFIFO(t *testing.T) {
	tree := newTestTree(t)
	applyTestLayer(t, tree,
		layerEntry{name: "payload", body: "data", xattrs: map[string]string{"user.kuasar-test": "value"}},
		layerEntry{name: "fifo", kind: tar.TypeFifo, mode: 0600})
	value, err := tree.getxattr("payload", "user.kuasar-test")
	if err != nil || value != "value" {
		t.Fatalf("xattr: %q %v", value, err)
	}
	v, err := tree.lstat("fifo")
	if err != nil || v.mode&unix.S_IFMT != unix.S_IFIFO {
		t.Fatalf("FIFO: %v %v", v, err)
	}
	// umoci deliberately omits host-specific SELinux labels. The adapter
	// must return an explicit error rather than silently claiming preservation.
	b := testLayerTar(t, layerEntry{name: "label", body: "x", xattrs: map[string]string{"security.selinux": "not-a-host-policy"}})
	body := &observedBody{Reader: bytes.NewReader(b)}
	err = applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), tree, LayerLimits{})
	if err == nil || !strings.Contains(err.Error(), "xattr") {
		t.Fatalf("silent xattr omission: %v", err)
	}
}

func TestUmociEmptyLayerAndMetadataBudget(t *testing.T) {
	tree := newTestTree(t)
	applyTestLayer(t, tree)
	b := testLayerTar(t, layerEntry{name: "parent/child", body: "payload"})
	body := &observedBody{Reader: bytes.NewReader(b)}
	err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), tree, LayerLimits{MetadataBytes: 1})
	if err == nil || !strings.Contains(err.Error(), "metadata budget") {
		t.Fatal(err)
	}
	if len(tree.root.children) != 0 {
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
			tree := newTestTree(t)
			body := &observedBody{Reader: bytes.NewReader(b)}
			err := applyVerifiedLayer(context.Background(), streamInput(b, b, "application/vnd.oci.image.layer.v1.tar", body), tree, LayerLimits{MetadataBytes: tc.budget})
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "metadata budget exceeded") {
					t.Fatalf("explicit limit: %v", err)
				}
				if _, err := tree.stat("complete"); !os.IsNotExist(err) {
					t.Fatalf("entry beyond explicit budget was applied: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertVirtualFile(t, tree, "complete", "payload")
			}
			if body.closed.Load() != 1 {
				t.Fatal("body close count")
			}
		})
	}
}

func TestVirtualTreeRootlessOwnershipDevicesAndSpoolReclamation(t *testing.T) {
	tree := newTestTree(t)
	bigPayload := strings.Repeat("A", 64<<10)
	applyTestLayer(t, tree,
		layerEntry{name: "etc/shadow", body: "root:*:19000:0:99999:7:::", mode: 0000, hasMode: true, uid: 10001, gid: 10002, hasOwner: true},
		layerEntry{name: "bin/suid", body: "elf", mode: 06755, uid: 0, gid: 0, hasOwner: true},
		layerEntry{name: "dev/null", kind: tar.TypeChar, mode: 0666, uid: 0, gid: 0, hasOwner: true, devmajor: 1, devminor: 3},
		layerEntry{name: "dev/sda", kind: tar.TypeBlock, mode: 0660, uid: 0, gid: 6, hasOwner: true, devmajor: 8, devminor: 0},
		layerEntry{name: "tmp/ephemeral", body: bigPayload, mode: 0644})
	if tree.spoolSize != int64(len("root:*:19000:0:99999:7:::")+len("elf")+len(bigPayload)) {
		t.Fatalf("unexpected spool size before whiteout: %d", tree.spoolSize)
	}
	// Whiteout the tail entry and verify spool tail truncation reclaims it.
	applyTestLayer(t, tree, layerEntry{name: "tmp/.wh.ephemeral"})
	if tree.spoolSize != int64(len("root:*:19000:0:99999:7:::")+len("elf")) {
		t.Fatalf("expected spool tail truncation after whiteout, got %d", tree.spoolSize)
	}
	shadow, err := tree.stat("etc/shadow")
	if err != nil {
		t.Fatal(err)
	}
	if shadow.uid != 10001 || shadow.gid != 10002 || shadow.mode&0777 != 0 {
		t.Fatalf("shadow metadata mismatch: uid=%d gid=%d mode=%o", shadow.uid, shadow.gid, shadow.mode)
	}
	suid, err := tree.stat("bin/suid")
	if err != nil {
		t.Fatal(err)
	}
	st := suid.stat()
	n := nodeMetadata(&st)
	if n.Mode&(fs.ModeSetuid|fs.ModeSetgid|fs.ModePerm) != fs.ModeSetuid|fs.ModeSetgid|0755 {
		t.Fatalf("suid mode mismatch: %v", n.Mode)
	}
	chr, err := tree.stat("dev/null")
	if err != nil {
		t.Fatal(err)
	}
	chrSt := chr.stat()
	chrNode := nodeMetadata(&chrSt)
	if chrNode.Mode&(fs.ModeDevice|fs.ModeCharDevice) != fs.ModeDevice|fs.ModeCharDevice || unix.Major(chr.rdev) != 1 || unix.Minor(chr.rdev) != 3 {
		t.Fatalf("char device mismatch: mode=%v rdev=%x", chrNode.Mode, chr.rdev)
	}
	blk, err := tree.stat("dev/sda")
	if err != nil {
		t.Fatal(err)
	}
	blkSt := blk.stat()
	blkNode := nodeMetadata(&blkSt)
	if blkNode.Mode&(fs.ModeDevice|fs.ModeCharDevice) != fs.ModeDevice || blk.gid != 6 || unix.Major(blk.rdev) != 8 || unix.Minor(blk.rdev) != 0 {
		t.Fatalf("block device mismatch: mode=%v gid=%d rdev=%x", blkNode.Mode, blk.gid, blk.rdev)
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
