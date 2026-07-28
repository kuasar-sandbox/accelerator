package tar

import (
	stdtar "archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func TestParseRule(t *testing.T) {
	cases := []struct {
		in   string
		want Rule
		err  bool
	}{
		{in: "a/b.txt", want: Rule{Tar: "a/b.txt", FS: "a/b.txt"}},
		{in: "a:b", want: Rule{Tar: "a", FS: "b"}},
		{in: "a:-", want: Rule{Tar: "a", FS: "-"}},
		{in: "d/", want: Rule{Tar: "d", FS: "d", Prefix: true}},
		{in: "/d/:o/", want: Rule{Tar: "d", FS: "o", Prefix: true}},
		{in: "d/:o", want: Rule{Tar: "d", FS: "o", Prefix: true}},
		{in: "d/:", want: Rule{Tar: "d", FS: ".", Prefix: true}},
		{in: ":o/", want: Rule{Tar: "", FS: "o", Prefix: true}},
		{in: "./d/:o/", want: Rule{Tar: "d", FS: "o", Prefix: true}},
		{in: "a/b:", want: Rule{Tar: "a/b", FS: "b"}},
		{in: "d/:o/x/", want: Rule{Tar: "d", FS: "o/x", Prefix: true}},
		{in: "", err: true},
		{in: "../x", err: true},
		{in: "d/:-", err: true}, // stdio on a directory rule
		{in: ":x", err: true},   // archive root with a file target
		{in: "a:b/", err: true}, // file rule with directory target
		{in: "/", err: true},    // bare root without colon
	}
	for _, c := range cases {
		got, err := ParseRule(c.in)
		if c.err {
			if err == nil {
				t.Errorf("ParseRule(%q): want error, got %+v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRule(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseRule(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

func TestParseRulesSingleStdio(t *testing.T) {
	if _, err := ParseRules([]string{"a:-", "b:-"}); err == nil {
		t.Fatal("two stdio rules must be rejected")
	}
}

var fixtureMtime = time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC)

// fixtureArchive builds a tree archive with the stdlib writer:
// dirs, files with modes/mtimes, a symlink and a hardlink pair.
func fixtureArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := stdtar.NewWriter(&buf)
	add := func(hdr *stdtar.Header, body string) {
		t.Helper()
		hdr.ModTime = fixtureMtime
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if body != "" {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	add(&stdtar.Header{Name: "f1.txt", Mode: 0o640, Size: 5}, "hello")
	add(&stdtar.Header{Name: "f1.hard", Typeflag: stdtar.TypeLink, Linkname: "f1.txt"}, "")
	add(&stdtar.Header{Name: "sub/", Typeflag: stdtar.TypeDir, Mode: 0o755}, "")
	add(&stdtar.Header{Name: "sub/f2.txt", Mode: 0o755, Size: 5}, "world")
	add(&stdtar.Header{Name: "sub/deep/", Typeflag: stdtar.TypeDir, Mode: 0o700}, "")
	add(&stdtar.Header{Name: "sub/deep/f3.txt", Mode: 0o600, Size: 4}, "deep")
	add(&stdtar.Header{Name: "sub/link", Typeflag: stdtar.TypeSymlink, Linkname: "f2.txt"}, "")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestExtractRoundTrip(t *testing.T) {
	archive := fixtureArchive(t)
	dst := t.TempDir()
	if err := Extract(bytes.NewReader(archive), []Rule{{Tar: "", FS: dst, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	for rel, want := range map[string]string{
		"f1.txt": "hello", "f1.hard": "hello", "sub/f2.txt": "world", "sub/deep/f3.txt": "deep",
	} {
		got, err := os.ReadFile(filepath.Join(dst, rel))
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", rel, got, want)
		}
	}
	st, err := os.Stat(filepath.Join(dst, "f1.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o640 {
		t.Errorf("f1.txt mode = %v, want 0640", st.Mode().Perm())
	}
	if !st.ModTime().Equal(fixtureMtime) {
		t.Errorf("f1.txt mtime = %v", st.ModTime())
	}
	if target, err := os.Readlink(filepath.Join(dst, "sub/link")); err != nil || target != "f2.txt" {
		t.Errorf("symlink = %q, %v", target, err)
	}
	var s1, s2 syscall.Stat_t
	if err := syscall.Stat(filepath.Join(dst, "f1.txt"), &s1); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Stat(filepath.Join(dst, "f1.hard"), &s2); err != nil {
		t.Fatal(err)
	}
	if s1.Ino != s2.Ino {
		t.Errorf("hardlink not preserved: ino %d vs %d", s1.Ino, s2.Ino)
	}
	if dirSt, err := os.Stat(filepath.Join(dst, "sub/deep")); err != nil || dirSt.Mode().Perm() != 0o700 {
		t.Errorf("dir mode = %v, %v", dirSt.Mode().Perm(), err)
	}
}

func TestExtractRenameRules(t *testing.T) {
	archive := fixtureArchive(t)
	dst := t.TempDir()
	rules, err := ParseRules([]string{
		"sub/:" + filepath.Join(dst, "renamed") + "/",
		"f1.txt:" + filepath.Join(dst, "one.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Extract(bytes.NewReader(archive), rules, Options{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "renamed/f2.txt")); string(got) != "world" {
		t.Errorf("renamed/f2.txt = %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "one.txt")); string(got) != "hello" {
		t.Errorf("one.txt = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dst, "f1.txt")); !os.IsNotExist(err) {
		t.Error("unselected name extracted")
	}
}

func TestExtractStdio(t *testing.T) {
	archive := fixtureArchive(t)
	var out bytes.Buffer
	if err := Extract(bytes.NewReader(archive), []Rule{{Tar: "sub/f2.txt", FS: "-"}}, Options{Stdout: &out}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "world" {
		t.Errorf("stdout = %q", out.String())
	}
}

func TestExtractChmodOverride(t *testing.T) {
	archive := fixtureArchive(t)
	dst := t.TempDir()
	mode, err := ParseMode("750")
	if err != nil {
		t.Fatal(err)
	}
	if err := Extract(bytes.NewReader(archive), []Rule{{Tar: "", FS: dst, Prefix: true}}, Options{Chmod: &mode}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(dst, "f1.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o750 {
		t.Errorf("mode = %v, want 0750", st.Mode().Perm())
	}
}

func TestExtractChownToSelf(t *testing.T) {
	archive := fixtureArchive(t)
	dst := t.TempDir()
	owner := Owner{UID: os.Getuid(), GID: os.Getgid()}
	if err := Extract(bytes.NewReader(archive), []Rule{{Tar: "f1.txt", FS: filepath.Join(dst, "f")}}, Options{Chown: &owner}); err != nil {
		t.Fatal(err)
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(dst, "f"), &st); err != nil {
		t.Fatal(err)
	}
	if int(st.Uid) != owner.UID || int(st.Gid) != owner.GID {
		t.Errorf("owner = %d:%d", st.Uid, st.Gid)
	}
}

func mkSparseLogical() ([]byte, []sparse.Extent) {
	const size = 3 << 20
	buf := make([]byte, size)
	copy(buf, "head")
	copy(buf[1<<20:], "middle")
	return buf, []sparse.Extent{
		{Offset: 4096, Size: (1 << 20) - 4096},
		{Offset: (1 << 20) + 4096, Size: (3 << 20) - ((1 << 20) + 4096)},
	}
}

// mkTarstream packages the logical bytes + hole map as a tarstream
// archive (the writer reads only the data extents).
func mkTarstream(t *testing.T, name string, logical []byte, holes []sparse.Extent) []byte {
	t.Helper()
	src, err := sparse.NewSource(bytes.NewReader(logical), uint64(len(logical)), holes)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := tarstream.WriteTo(context.Background(), &buf, name, src); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func sparseSupported(t *testing.T, dir string) bool {
	t.Helper()
	p := filepath.Join(dir, "probe")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	f.Truncate(1 << 20)
	f.Close()
	var st syscall.Stat_t
	if err := syscall.Stat(p, &st); err != nil {
		t.Fatal(err)
	}
	os.Remove(p)
	return st.Blocks*512 < 1<<20
}

// TestExtractSparseHoleExact: the engine extracts sparse members
// hole-exact when the archive is re-openable (Plan C: the member is
// re-located by ordinal on a second handle, recovering the map the
// stdlib reader hides). Without Reopen it must FAIL — never silently
// densify — and --dense explicitly opts into logical-bytes
// materialization.
func TestExtractSparseHoleExact(t *testing.T) {
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	logical, holes := mkSparseLogical()

	// Multi-entry archive: [0] dense file, [1] dir, [2] SPARSE member.
	var buf bytes.Buffer
	tw := stdtar.NewWriter(&buf)
	tw.WriteHeader(&stdtar.Header{Name: "plain.txt", Mode: 0o644, Size: 5})
	tw.Write([]byte("hello"))
	tw.WriteHeader(&stdtar.Header{Name: "d/", Typeflag: stdtar.TypeDir, Mode: 0o755})
	tw.Flush() // no Close: the sparse member's WriteTo carries the trailer
	buf.Write(mkTarstream(t, "disk/img.raw", logical, holes))
	archivePath := filepath.Join(dir, "a.tar")
	if err := os.WriteFile(archivePath, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	// (1) Re-openable: hole-exact extraction of the whole archive.
	outDir := filepath.Join(dir, "x")
	os.Mkdir(outDir, 0o755)
	af, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer af.Close()
	opts := Options{Reopen: func() (io.ReadSeekCloser, error) { return os.Open(archivePath) }}
	if err := Extract(af, []Rule{{Tar: "", FS: outDir, Prefix: true}}, opts); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(outDir, "disk/img.raw"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, logical) {
		t.Error("sparse member content mismatch")
	}
	f, err := os.Open(filepath.Join(outDir, "disk/img.raw"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	probed, err := sparse.ProbeHoles(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(probed) != len(holes) {
		t.Fatalf("restored holes = %v, want %v", probed, holes)
	}
	for i := range holes {
		if probed[i] != holes[i] {
			t.Fatalf("restored holes = %v, want %v", probed, holes)
		}
	}
	if plain, _ := os.ReadFile(filepath.Join(outDir, "plain.txt")); string(plain) != "hello" {
		t.Errorf("dense sibling = %q", plain)
	}

	// (2) Single-pass input without Reopen: hard error, never densify.
	err = Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "", FS: filepath.Join(dir, "y"), Prefix: true}}, Options{})
	if err == nil || !strings.Contains(err.Error(), "sparse member") {
		t.Fatalf("want sparse-member error, got %v", err)
	}

	// (3) --dense: explicit logical-bytes materialization succeeds.
	zDir := filepath.Join(dir, "z")
	os.Mkdir(zDir, 0o755)
	if err := Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "", FS: zDir, Prefix: true}}, Options{Dense: true}); err != nil {
		t.Fatal(err)
	}
	dgot, err := os.ReadFile(filepath.Join(zDir, "disk/img.raw"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dgot, logical) {
		t.Error("dense content mismatch")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(filepath.Join(zDir, "disk/img.raw"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 < int64(len(logical)) {
		t.Errorf("--dense extraction must be dense: %d blocks", st.Blocks)
	}
}

// TestExtractFileExactHoles: ExtractFile restores exactly the DECLARED
// holes — sparse on disk, hole map round-tripped — for both the
// seekable and the one-pass view.
func TestExtractFileExactHoles(t *testing.T) {
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	logical, holes := mkSparseLogical()
	archive := mkTarstream(t, "disk/img.raw", logical, holes)

	for _, tc := range []struct {
		name string
		in   io.Reader
	}{
		{"seek", bytes.NewReader(archive)},
		{"sequential", struct{ io.Reader }{bytes.NewReader(archive)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := tarstream.ReadFrom(tc.in, "disk/img.raw")
			if err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, tc.name+".raw")
			if err := ExtractFile(v, out, Options{}); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, logical) {
				t.Error("content mismatch")
			}
			f, err := os.Open(out)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			probed, err := sparse.ProbeHoles(f)
			if err != nil {
				t.Fatal(err)
			}
			if len(probed) != len(holes) {
				t.Fatalf("restored holes = %v, want %v", probed, holes)
			}
			for i := range holes {
				if probed[i] != holes[i] {
					t.Fatalf("restored holes = %v, want %v", probed, holes)
				}
			}
		})
	}
}

// TestDenseZerosStayAllocated: zero-valued bytes in a tar are DATA —
// extraction must keep them allocated, never punching them into holes
// (written zeros and holes carry different meanings).
func TestDenseZerosStayAllocated(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	tw := stdtar.NewWriter(&buf)
	payload := make([]byte, 256<<10)
	copy(payload, "data")
	if err := tw.WriteHeader(&stdtar.Header{Name: "z.bin", Mode: 0o644, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatal(err)
	}
	tw.Close()

	out := filepath.Join(dir, "z.bin")
	if err := Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "z.bin", FS: out}}, Options{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("content mismatch")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(out, &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 < int64(len(payload)) {
		t.Errorf("dense zeros were punched into holes: %d blocks", st.Blocks)
	}
}

func TestExtractFifo(t *testing.T) {
	var buf bytes.Buffer
	tw := stdtar.NewWriter(&buf)
	if err := tw.WriteHeader(&stdtar.Header{Name: "p/pipe", Typeflag: stdtar.TypeFifo, Mode: 0o600}); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	dst := t.TempDir()
	if err := Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "", FS: dst, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Lstat(filepath.Join(dst, "p/pipe"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode()&os.ModeNamedPipe == 0 {
		t.Errorf("not a fifo: %v", st.Mode())
	}
}

func TestEscapeRefused(t *testing.T) {
	dir := t.TempDir()
	var buf bytes.Buffer
	tw := stdtar.NewWriter(&buf)
	if err := tw.WriteHeader(&stdtar.Header{Name: "../evil.txt", Mode: 0o644, Size: 0}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&stdtar.Header{Name: "d", Typeflag: stdtar.TypeSymlink, Linkname: "/tmp"}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&stdtar.Header{Name: "d/escaped-tar-test", Mode: 0o644, Size: 0}); err != nil {
		t.Fatal(err)
	}
	tw.Close()

	var warned []string
	o := Options{Warnf: func(f string, a ...any) { warned = append(warned, f) }}
	err := Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "", FS: dir, Prefix: true}}, o)
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("symlink write-out must fail with escape error, got %v", err)
	}
	if _, statErr := os.Lstat("/tmp/escaped-tar-test"); !os.IsNotExist(statErr) {
		os.Remove("/tmp/escaped-tar-test")
		t.Error("symlink write-out escaped")
	}
	if _, statErr := os.Lstat(filepath.Join(dir, "..", "evil.txt")); !os.IsNotExist(statErr) {
		t.Error("../ member escaped")
	}
	if len(warned) == 0 {
		t.Error("missing ../ warning")
	}
}

func TestMemberNotFound(t *testing.T) {
	archive := fixtureArchive(t)
	err := Extract(bytes.NewReader(archive),
		[]Rule{{Tar: "no/such/file", FS: filepath.Join(t.TempDir(), "x")}}, Options{})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("want not-found error, got %v", err)
	}
}

func TestExtractAllNoRules(t *testing.T) {
	archive := fixtureArchive(t)
	dst := t.TempDir()
	cwd, _ := os.Getwd()
	if err := os.Chdir(dst); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := Extract(bytes.NewReader(archive), nil, Options{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "sub/f2.txt")); string(got) != "world" {
		t.Errorf("sub/f2.txt = %q", got)
	}
}

// TestExtractTreeFromPipe: every extract shape is single-pass over a
// genuinely non-seekable stream.
func TestExtractTreeFromPipe(t *testing.T) {
	archive := fixtureArchive(t)
	pr, pw := io.Pipe()
	go func() {
		pw.Write(archive)
		pw.Close()
	}()
	dst := t.TempDir()
	if err := Extract(pr, []Rule{{Tar: "", FS: dst, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "sub/deep/f3.txt")); string(got) != "deep" {
		t.Errorf("sub/deep/f3.txt = %q", got)
	}
}
