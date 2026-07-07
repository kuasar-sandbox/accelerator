package tar

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"strconv"
	"strings"
)

// Owner is a numeric uid:gid override.
type Owner struct {
	UID, GID int
}

// ParseOwner parses a --chown value into a numeric Owner. Each side is a
// number used verbatim, or a NAME resolved against /etc/passwd / /etc/group
// (Docker COPY --chown semantics). When flatten-ctl extracts into a guest
// rootfs (the COPY path), that passwd/group IS the image's — CGO is off, so
// os/user reads the files directly, never libc/NSS. Forms:
//
//	uid:gid · user:group · user (→ the user's primary group) · 1000 (→ 1000:1000)
func ParseOwner(s string) (Owner, error) {
	if s == "" {
		return Owner{}, fmt.Errorf("chown: empty")
	}
	uPart, gPart, hasGroup := strings.Cut(s, ":")
	if uPart == "" {
		return Owner{}, fmt.Errorf("chown %q: empty user", s)
	}
	uid, uName, err := resolveID(uPart, false)
	if err != nil {
		return Owner{}, err
	}
	// No group given: a numeric user mirrors to the same gid (1000 → 1000:1000);
	// a named user takes its primary group from /etc/passwd.
	if !hasGroup {
		if uName == "" {
			return Owner{UID: uid, GID: uid}, nil
		}
		u, err := user.Lookup(uName)
		if err != nil {
			return Owner{}, fmt.Errorf("chown %q: lookup user: %w", s, err)
		}
		gid, err := strconv.Atoi(u.Gid)
		if err != nil {
			return Owner{}, fmt.Errorf("chown %q: user %q primary gid %q not numeric", s, uName, u.Gid)
		}
		return Owner{UID: uid, GID: gid}, nil
	}
	if gPart == "" {
		return Owner{}, fmt.Errorf("chown %q: empty group", s)
	}
	gid, _, err := resolveID(gPart, true)
	if err != nil {
		// A common shape is "name:name" where only the USER exists (no
		// like-named group); fall back to that user's primary group.
		if uName != "" && gPart == uName {
			if u, uerr := user.Lookup(uName); uerr == nil {
				if pg, perr := strconv.Atoi(u.Gid); perr == nil {
					return Owner{UID: uid, GID: pg}, nil
				}
			}
		}
		return Owner{}, err
	}
	return Owner{UID: uid, GID: gid}, nil
}

// resolveID parses a uid/gid component: a non-negative number is used as-is
// (name=""), otherwise it is looked up by name. group selects the namespace.
func resolveID(s string, group bool) (id int, name string, err error) {
	if n, aerr := strconv.Atoi(s); aerr == nil {
		if n < 0 {
			return 0, "", fmt.Errorf("chown %q: negative id", s)
		}
		return n, "", nil
	}
	if group {
		g, gerr := user.LookupGroup(s)
		if gerr != nil {
			return 0, "", fmt.Errorf("chown: lookup group %q: %w", s, gerr)
		}
		n, _ := strconv.Atoi(g.Gid)
		return n, s, nil
	}
	u, uerr := user.Lookup(s)
	if uerr != nil {
		return 0, "", fmt.Errorf("chown: lookup user %q: %w", s, uerr)
	}
	n, _ := strconv.Atoi(u.Uid)
	return n, s, nil
}

// ParseMode parses the --chmod value as octal permission bits
// (including setuid/setgid/sticky).
func ParseMode(s string) (fs.FileMode, error) {
	n, err := strconv.ParseUint(s, 8, 32)
	if err != nil || n > 0o7777 {
		return 0, fmt.Errorf("chmod %q: want octal permission bits", s)
	}
	m := fs.FileMode(n & 0o777)
	if n&0o4000 != 0 {
		m |= fs.ModeSetuid
	}
	if n&0o2000 != 0 {
		m |= fs.ModeSetgid
	}
	if n&0o1000 != 0 {
		m |= fs.ModeSticky
	}
	return m, nil
}

// Options tweaks Create and Extract. The zero value preserves source
// ownership and permissions and uses the process stdio for "-" rules.
type Options struct {
	// Chown, when non-nil, overrides the owner of every selected
	// entry. Extract with Chown set fails hard when chown fails;
	// without it, ownership is preserved best-effort (EPERM as an
	// unprivileged user downgrades to a one-time warning).
	Chown *Owner
	// NoChown skips ownership restoration entirely. It is useful for
	// payload-only extraction checks where uid/gid metadata is not part
	// of the assertion and the caller wants clean unprivileged output.
	NoChown bool
	// Chmod, when non-nil, overrides the permission bits (including
	// setuid/setgid/sticky) of every selected entry, directories
	// included.
	Chmod *fs.FileMode

	// Stdout backs the "-" side of stdio rules; nil means os.Stdout.
	Stdout io.Writer

	// Warnf, when non-nil, receives non-fatal diagnostics (skipped
	// sockets, unprivileged chown downgrades). nil silences them.
	Warnf func(format string, args ...any)

	// Reopen, when non-nil, returns an independent ReadSeeker over the
	// SAME archive. It enables hole-exact extraction of sparse members
	// in multi-entry archives: the engine re-locates the member by
	// ordinal on the second handle and recovers the hole map the
	// stdlib reader hides (golang.org/issue/22735). nil + a sparse
	// member is a hard error (single-pass input cannot recover the
	// map) unless Dense is set.
	Reopen func() (io.ReadSeekCloser, error)

	// Dense disables sparse handling wholesale: members extract as
	// their logical bytes (stdlib semantics), declared holes
	// materializing as allocated zeros. The default is hole-exact
	// extraction — never a silent multi-GiB densification.
	Dense bool
}

func (o *Options) warnf(format string, args ...any) {
	if o.Warnf != nil {
		o.Warnf(format, args...)
	}
}

func (o *Options) stdout() io.Writer {
	if o.Stdout != nil {
		return o.Stdout
	}
	return os.Stdout
}
