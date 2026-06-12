package tar

import (
	stdtar "archive/tar"
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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

// requireTar skips when no usable GNU tar is available (the engine
// delegates everything to it).
func requireTar(t *testing.T) {
	t.Helper()
	if _, err := LocateTar(); err != nil {
		t.Skipf("no usable GNU tar: %v", err)
	}
}

// buildTree writes a fixture tree and returns its root.
func buildTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	mk := func(rel string, mode os.FileMode, content string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil { // bypass umask
			t.Fatal(err)
		}
	}
	mk("f1.txt", 0o640, "hello")
	mk("sub/f2.txt", 0o755, "world")
	mk("sub/deep/f3.txt", 0o600, "deep")
	if err := os.Symlink("f2.txt", filepath.Join(root, "sub/link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "f1.txt"), filepath.Join(root, "f1.hard")); err != nil {
		t.Fatal(err)
	}
	mt := time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, p := range []string{"f1.txt", "sub/f2.txt", "sub/deep/f3.txt"} {
		if err := os.Chtimes(filepath.Join(root, p), mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestRoundTrip(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	var buf bytes.Buffer
	rules, err := ParseRules([]string{":" + src + "/"})
	if err != nil {
		t.Fatal(err)
	}
	if err := Create(&buf, rules, Options{}); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	if err := Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "", FS: dst, Prefix: true}}, Options{}); err != nil {
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
	if want := time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC); !st.ModTime().Equal(want) {
		t.Errorf("f1.txt mtime = %v, want %v", st.ModTime(), want)
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
}

func TestCreateDeterministic(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	rules := []Rule{{Tar: "", FS: src, Prefix: true}}
	var a, b bytes.Buffer
	if err := Create(&a, rules, Options{}); err != nil {
		t.Fatal(err)
	}
	if err := Create(&b, rules, Options{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Error("two Create runs over the same tree differ")
	}
}

func archiveNames(t *testing.T, b []byte) []string {
	t.Helper()
	var names []string
	tr := stdtar.NewReader(bytes.NewReader(b))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, hdr.Name)
	}
	return names
}

func TestRenameRules(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	var buf bytes.Buffer
	rules, err := ParseRules([]string{
		"etc/:" + filepath.Join(src, "sub") + "/",
		"top.txt:" + filepath.Join(src, "f1.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Create(&buf, rules, Options{}); err != nil {
		t.Fatal(err)
	}

	names := archiveNames(t, buf.Bytes())
	want := []string{"etc/", "etc/deep/", "etc/deep/f3.txt", "etc/f2.txt", "etc/link", "top.txt"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("names = %v, want %v", names, want)
	}

	dst := t.TempDir()
	xr, err := ParseRules([]string{
		"etc/:" + filepath.Join(dst, "renamed") + "/",
		"top.txt:" + filepath.Join(dst, "one.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Extract(bytes.NewReader(buf.Bytes()), xr, Options{}); err != nil {
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

// TestHostileNames runs a prefix rename whose names contain regex and
// sed metacharacters end to end.
func TestHostileNames(t *testing.T) {
	requireTar(t)
	src := t.TempDir()
	dir := "we,ird&dir$x [a]+b"
	if err := os.MkdirAll(filepath.Join(src, dir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, dir, "f.txt"), []byte("deep"), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := Create(&buf, []Rule{{Tar: "renamed", FS: src, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, n := range archiveNames(t, buf.Bytes()) {
		if !strings.HasPrefix(n, "renamed/") && n != "renamed/" {
			t.Errorf("entry %q missing renamed/ prefix", n)
		}
	}

	dst := t.TempDir()
	if err := Extract(bytes.NewReader(buf.Bytes()),
		[]Rule{{Tar: "renamed/" + dir, FS: dst, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "f.txt")); string(got) != "deep" {
		t.Errorf("f.txt = %q", got)
	}
}

func TestStdio(t *testing.T) {
	requireTar(t)
	var buf bytes.Buffer
	in := strings.NewReader("from stdin")
	rules, err := ParseRules([]string{"data/blob.bin:-"})
	if err != nil {
		t.Fatal(err)
	}
	if err := Create(&buf, rules, Options{Stdin: in}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := Extract(bytes.NewReader(buf.Bytes()), rules, Options{Stdout: &out}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "from stdin" {
		t.Errorf("stdout = %q", out.String())
	}
}

func TestChownChmodOverride(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	var buf bytes.Buffer
	mode, err := ParseMode("750")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := ParseOwner("1234:5678")
	if err != nil {
		t.Fatal(err)
	}
	o := Options{Chown: &owner, Chmod: &mode}
	if err := Create(&buf, []Rule{{Tar: "", FS: src, Prefix: true}}, o); err != nil {
		t.Fatal(err)
	}
	tr := stdtar.NewReader(bytes.NewReader(buf.Bytes()))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if hdr.Uid != 1234 || hdr.Gid != 5678 {
			t.Errorf("%s owner = %d:%d", hdr.Name, hdr.Uid, hdr.Gid)
		}
		if hdr.Typeflag != stdtar.TypeSymlink && hdr.Mode&0o7777 != 0o750 {
			t.Errorf("%s mode = %o", hdr.Name, hdr.Mode)
		}
	}
}

// mkSparse creates a file with data at the start and at 1 MiB, a hole
// between them and a trailing hole to 3 MiB.
func mkSparse(t *testing.T, p string) {
	t.Helper()
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte("head"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("middle"), 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 << 20); err != nil {
		t.Fatal(err)
	}
}

func sparseSupported(t *testing.T, dir string) bool {
	t.Helper()
	p := filepath.Join(dir, "probe")
	mkSparse(t, p)
	var st syscall.Stat_t
	if err := syscall.Stat(p, &st); err != nil {
		t.Fatal(err)
	}
	os.Remove(p)
	return st.Blocks*512 < 3<<20
}

func TestSparseRoundTrip(t *testing.T) {
	requireTar(t)
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	src := filepath.Join(dir, "img.raw")
	mkSparse(t, src)

	var buf bytes.Buffer
	if err := Create(&buf, []Rule{{Tar: "disk/img.raw", FS: src}}, Options{}); err != nil {
		t.Fatal(err)
	}
	if buf.Len() > 64<<10 {
		t.Errorf("sparse encoding too large: %d bytes", buf.Len())
	}

	// Go's reader presents the logical content under the real name.
	tr := stdtar.NewReader(bytes.NewReader(buf.Bytes()))
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != "disk/img.raw" {
		t.Errorf("name = %q", hdr.Name)
	}
	if hdr.Size != 3<<20 {
		t.Errorf("logical size = %d", hdr.Size)
	}
	logical, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	wantLogical, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(logical, wantLogical) {
		t.Error("logical content mismatch")
	}

	// Extract restores both content and holes.
	out := filepath.Join(dir, "out.raw")
	if err := Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "disk/img.raw", FS: out}}, Options{}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, wantLogical) {
		t.Error("extracted content mismatch")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(out, &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 >= 3<<20 {
		t.Errorf("extracted file is dense: %d blocks", st.Blocks)
	}
}

// TestConcatExternallyReadable verifies a multi-rule (multi-invocation)
// archive is one valid stream for an external tar -t as well.
func TestConcatExternallyReadable(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	archive := filepath.Join(t.TempDir(), "a.tar")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := ParseRules([]string{
		"etc/:" + filepath.Join(src, "sub") + "/",
		"top.txt:" + filepath.Join(src, "f1.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Create(f, rules, Options{}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	bin, _ := LocateTar()
	out, err := exec.Command(bin, "-tf", archive).Output()
	if err != nil {
		t.Fatalf("external tar -t: %v", err)
	}
	listing := string(out)
	for _, want := range []string{"etc/f2.txt", "top.txt"} {
		if !strings.Contains(listing, want) {
			t.Errorf("external listing missing %q:\n%s", want, listing)
		}
	}
}

// TestEscapeRefused: ../ members are never selected; a symlink
// write-out attempt is refused by GNU tar's hardening and surfaces as
// an error.
func TestEscapeRefused(t *testing.T) {
	requireTar(t)
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

	dst := t.TempDir()
	err := Extract(bytes.NewReader(buf.Bytes()), []Rule{{Tar: "", FS: dst, Prefix: true}}, Options{})
	if err == nil {
		t.Fatal("symlink write-out must surface an error")
	}
	if _, statErr := os.Lstat("/tmp/escaped-tar-test"); !os.IsNotExist(statErr) {
		os.Remove("/tmp/escaped-tar-test")
		t.Error("symlink write-out escaped")
	}
	if _, statErr := os.Lstat(filepath.Join(dst, "..", "evil.txt")); !os.IsNotExist(statErr) {
		t.Error("../ member escaped")
	}
}

func TestMemberNotFound(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	var buf bytes.Buffer
	if err := Create(&buf, []Rule{{Tar: "", FS: src, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	err := Extract(bytes.NewReader(buf.Bytes()),
		[]Rule{{Tar: "no/such/file", FS: filepath.Join(t.TempDir(), "x")}}, Options{})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("want not-found error, got %v", err)
	}
}

// TestExtractFromRedirectedStdin: a shell `< archive.tar` hands the
// process a regular-file stdin whose Name() is "/dev/stdin"; the
// engine must resolve the real path (or spool) so the tar child does
// not read its own fd 0.
func TestExtractFromRedirectedStdin(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	archive := filepath.Join(t.TempDir(), "a.tar")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	if err := Create(f, []Rule{{Tar: "", FS: src, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	in, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	// Dup the fd: os.NewFile takes ownership, and two *os.File over one
	// fd would double-close it (corrupting whatever reuses the number).
	dupFD, err := syscall.Dup(int(in.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	stdinLike := os.NewFile(uintptr(dupFD), "/dev/stdin") // what a redirected stdin looks like
	defer stdinLike.Close()

	var out bytes.Buffer
	// sub/f2.txt, not f1.txt: the latter is stored as a hardlink member
	// (its twin sorts first) and -xO of a link member is empty by GNU
	// semantics.
	if err := Extract(stdinLike, []Rule{{Tar: "sub/f2.txt", FS: "-"}}, Options{Stdout: &out}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "world" {
		t.Errorf("stdout = %q", out.String())
	}
}

// TestExtractStdioStreaming: a single x:- rule must work on a
// non-seekable stream (no spool, no listing pass), including sparse
// members which arrive as logical bytes.
func TestExtractStdioStreaming(t *testing.T) {
	requireTar(t) // create still drives GNU tar
	dir := t.TempDir()
	if !sparseSupported(t, dir) {
		t.Skip("filesystem does not keep holes")
	}
	src := filepath.Join(dir, "img.raw")
	mkSparse(t, src)
	var buf bytes.Buffer
	if err := Create(&buf, []Rule{{Tar: "disk/img.raw", FS: src}}, Options{}); err != nil {
		t.Fatal(err)
	}

	pr, pw := io.Pipe() // genuinely non-seekable
	go func() {
		pw.Write(buf.Bytes())
		pw.Close()
	}()
	var out bytes.Buffer
	if err := Extract(pr, []Rule{{Tar: "disk/img.raw", FS: "-"}}, Options{Stdout: &out}); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Errorf("streamed content mismatch: %d vs %d bytes", out.Len(), len(want))
	}

	// Not-found still surfaces on the streaming path.
	if err := Extract(bytes.NewReader(buf.Bytes()),
		[]Rule{{Tar: "no/such", FS: "-"}}, Options{Stdout: io.Discard}); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Errorf("want not-found error, got %v", err)
	}
}

func TestExtractAllNoRules(t *testing.T) {
	requireTar(t)
	src := buildTree(t)
	var buf bytes.Buffer
	if err := Create(&buf, []Rule{{Tar: "", FS: src, Prefix: true}}, Options{}); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	cwd, _ := os.Getwd()
	if err := os.Chdir(dst); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := Extract(bytes.NewReader(buf.Bytes()), nil, Options{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dst, "sub/f2.txt")); string(got) != "world" {
		t.Errorf("sub/f2.txt = %q", got)
	}
}
