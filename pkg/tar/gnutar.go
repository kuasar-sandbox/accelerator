package tar

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// unixEpoch normalizes runner-derived timestamps on spooled stdin
// entries.
var unixEpoch = time.Unix(0, 0)

// The engine drives a GNU tar binary instead of encoding tar streams
// itself: sparse handling, the wire format and its edge cases stay GNU
// tar's problem. The package contributes rule resolution, deterministic
// invocation flags and stream plumbing.
//
// Multi-rule archives are produced by one tar invocation per rule with
// `-b 1` (blocking factor 1), under which a GNU tar archive ends in
// exactly two 512-byte zero blocks; the engine strips that trailer from
// every invocation and appends a single fresh one, yielding one valid
// stream (verified against both GNU tar and Go's archive/tar reader).

// trailerSize is the end-of-archive marker emitted with -b 1: two
// 512-byte zero blocks.
const trailerSize = 1024

// minVersion is the oldest GNU tar the engine accepts: --sort=name
// appeared in 1.28.
var minVersion = [2]int{1, 28}

// TarPathEnv overrides binary discovery (same pattern as
// MKFS_EROFS_PATH for mkfs.erofs).
const TarPathEnv = "TAR_PATH"

var (
	locateOnce sync.Once
	tarPath    string
	tarErr     error
)

// LocateTar finds the GNU tar binary: $TAR_PATH, then a `tar` next to
// the running executable, then $PATH — and verifies it is GNU tar
// >= 1.28 (busybox/bsdtar lack the sparse and determinism flags this
// package relies on). The result is cached for the process lifetime.
func LocateTar() (string, error) {
	locateOnce.Do(func() {
		tarPath, tarErr = locate()
		if tarErr == nil {
			tarErr = probe(tarPath)
		}
	})
	return tarPath, tarErr
}

func locate() (string, error) {
	if p := os.Getenv(TarPathEnv); p != "" {
		return p, nil
	}
	if exe, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(exe), "tar")
		if st, err := os.Stat(cand); err == nil && !st.IsDir() {
			return cand, nil
		}
	}
	if p, err := exec.LookPath("tar"); err == nil {
		return p, nil
	}
	return "", fmt.Errorf("tar: GNU tar not found (set %s, place `tar` alongside the binary, or add it to PATH; `make tar` in sandbox-deps builds a static one)", TarPathEnv)
}

var versionRe = regexp.MustCompile(`GNU tar.*?(\d+)\.(\d+)`)

func probe(bin string) error {
	out, err := exec.Command(bin, "--version").Output()
	if err != nil {
		return fmt.Errorf("tar: probe %s: %w", bin, err)
	}
	m := versionRe.FindSubmatch(out)
	if m == nil {
		first, _, _ := strings.Cut(string(out), "\n")
		return fmt.Errorf("tar: %s is not GNU tar (%q); busybox/bsdtar lack the required sparse/determinism flags", bin, first)
	}
	maj, _ := strconv.Atoi(string(m[1]))
	min, _ := strconv.Atoi(string(m[2]))
	if maj < minVersion[0] || (maj == minVersion[0] && min < minVersion[1]) {
		return fmt.Errorf("tar: %s is GNU tar %d.%d; need >= %d.%d (--sort=name)", bin, maj, min, minVersion[0], minVersion[1])
	}
	return nil
}

// commonCreateArgs are the deterministic-output flags shared by every
// create invocation: PAX format with stable extended-header names and
// no atime/ctime records, sorted members, numeric ownership, GNU PAX
// sparse 1.0 hole encoding, exact 1024-byte trailer via -b 1.
func commonCreateArgs() []string {
	return []string{
		"-b", "1",
		"--format=posix",
		"--pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime",
		"--sort=name",
		"--numeric-owner",
		"--sparse",
	}
}

// Create writes a tar stream containing the entries selected by rules:
// the rule's left side names the entry inside the archive, the right
// side the filesystem source ("-" reads the content from stdin via a
// spool file). Directory rules recurse. Holes in regular files are
// preserved (GNU --sparse, PAX 1.0). Rules run in order; each is one
// tar invocation and the streams are joined trailer-free.
func Create(w io.Writer, rules []Rule, o Options) error {
	if len(rules) == 0 {
		return fmt.Errorf("tar create: at least one rule is required")
	}
	bin, err := LocateTar()
	if err != nil {
		return err
	}
	for _, r := range rules {
		if err := createRule(bin, w, r, &o); err != nil {
			return err
		}
	}
	_, err = w.Write(make([]byte, trailerSize))
	return err
}

func createRule(bin string, w io.Writer, r Rule, o *Options) error {
	fsPath := r.FS
	stdinEntry := r.FS == "-"
	if stdinEntry { // stdin content: spool so tar can stat it
		tmp, err := os.CreateTemp(o.TmpDir, "tar-stdin-*")
		if err != nil {
			return err
		}
		defer os.Remove(tmp.Name())
		_, cpErr := io.Copy(tmp, o.stdin())
		if err := tmp.Close(); cpErr != nil || err != nil {
			return fmt.Errorf("spool stdin: %w", firstErr(cpErr, err))
		}
		// Deterministic entry metadata: the spool file's own owner and
		// timestamp are runner artifacts.
		if err := os.Chmod(tmp.Name(), 0o644); err != nil {
			return err
		}
		if err := os.Chtimes(tmp.Name(), unixEpoch, unixEpoch); err != nil {
			return err
		}
		fsPath = tmp.Name()
	}

	st, err := os.Lstat(fsPath)
	if err != nil {
		return err
	}
	args := commonCreateArgs()
	switch {
	case o.Chown != nil:
		args = append(args, "--owner=:"+strconv.Itoa(o.Chown.UID), "--group=:"+strconv.Itoa(o.Chown.GID))
	case stdinEntry:
		args = append(args, "--owner=:0", "--group=:0") // not the runner's uid
	}
	if o.Chmod != nil {
		args = append(args, "--mode="+strconv.FormatInt(tarMode(*o.Chmod), 8))
	}

	switch {
	case r.Prefix:
		if st.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%s: directory rule on a symlink; point at its target", r.FS)
		}
		if !st.IsDir() {
			return fmt.Errorf("%s: directory rule (trailing /) on a non-directory", r.FS)
		}
		if r.Tar != "" {
			// "./x" → "<tar>/x"; the root entry matches as "." (GNU
			// applies transforms to names without their trailing /).
			e1, err := transformExpr(`^\./`, r.Tar+"/")
			if err != nil {
				return err
			}
			e2, err := transformExpr(`^\.$`, r.Tar)
			if err != nil {
				return err
			}
			args = append(args, e1, e2)
		}
		args = append(args, "-cf", "-", "-C", fsPath, ".")
	default:
		if st.IsDir() {
			return fmt.Errorf("%s is a directory; use a trailing / (%s/)", r.FS, r.FS)
		}
		dir, base := filepath.Split(fsPath)
		if dir == "" {
			dir = "."
		}
		if base != r.Tar {
			expr, err := transformExpr("^"+escapeBRE(base)+"$", r.Tar)
			if err != nil {
				return err
			}
			args = append(args, expr)
		}
		args = append(args, "-cf", "-", "-C", dir, base)
	}

	cmd := exec.Command(bin, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	tt := &tailTrim{w: w, keep: trailerSize}
	cmd.Stdout = tt
	if err := cmd.Run(); err != nil {
		return tarFail("create", r, err, &stderr)
	}
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		o.warnf("%s", msg)
	}
	if tt.buffered() != trailerSize {
		return fmt.Errorf("tar create %s: short archive (%d trailing bytes)", r.Tar, tt.buffered())
	}
	return nil
}

// tarFail decorates a tar child-process failure with its stderr.
func tarFail(op string, rl Rule, err error, stderr *bytes.Buffer) error {
	msg := strings.TrimSpace(stderr.String())
	what := rl.Tar
	if what == "" {
		what = rl.FS
	}
	if what != "" {
		what = " " + what
	}
	if msg != "" {
		return fmt.Errorf("tar %s%s: %w: %s", op, what, err, msg)
	}
	return fmt.Errorf("tar %s%s: %w", op, what, err)
}

// tailTrim forwards everything written through it except the final
// `keep` bytes, which it holds back (the per-invocation end-of-archive
// trailer that must not reach the joined stream).
type tailTrim struct {
	w    io.Writer
	keep int
	tail []byte
}

func (t *tailTrim) Write(p []byte) (int, error) {
	n := len(p)
	t.tail = append(t.tail, p...)
	if over := len(t.tail) - t.keep; over > 0 {
		if _, err := t.w.Write(t.tail[:over]); err != nil {
			return 0, err
		}
		t.tail = append(t.tail[:0], t.tail[over:]...)
	}
	return n, nil
}

func (t *tailTrim) buffered() int { return len(t.tail) }

// transformDelim separates the s-expression parts of a --transform
// argument. \x01 cannot be escaped inside the expression, so paths
// containing it (or a newline, which breaks tar -t listing) are
// rejected outright. Arguments reach tar via execve, never a shell.
const transformDelim = "\x01"

// transformExpr renders a GNU --transform argument from a left-side
// BRE (already escaped where literal) and a literal replacement.
// flags=rhS rewrites member names and hardlink targets but leaves
// symlink targets alone — they are relative to the link's location,
// not the archive root.
func transformExpr(lhsBRE, rhs string) (string, error) {
	for _, part := range []string{lhsBRE, rhs} {
		if strings.ContainsAny(part, transformDelim+"\n") {
			return "", fmt.Errorf("tar: path %q contains bytes a --transform expression cannot carry", part)
		}
	}
	var b strings.Builder
	for _, c := range []byte(rhs) {
		if c == '\\' || c == '&' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return "--transform=flags=rhS;s" + transformDelim + lhsBRE + transformDelim + b.String() + transformDelim, nil
}

// escapeBRE makes a literal string safe inside the basic (sed-style)
// regular expressions GNU --transform uses. Only BRE metacharacters
// are escaped; backslashing + ? ( ) { } | would turn those literals
// into GNU BRE operators.
func escapeBRE(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch c {
		case '\\', '.', '[', ']', '*', '^', '$':
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return b.String()
}

// tarMode converts permission + setuid/setgid/sticky bits of an
// fs.FileMode into tar mode bits (octal for --mode).
func tarMode(m os.FileMode) int64 {
	v := int64(m & os.ModePerm)
	if m&os.ModeSetuid != 0 {
		v |= 0o4000
	}
	if m&os.ModeSetgid != 0 {
		v |= 0o2000
	}
	if m&os.ModeSticky != 0 {
		v |= 0o1000
	}
	return v
}

func firstErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}
