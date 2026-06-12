package tar

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// Owner is a numeric uid:gid override.
type Owner struct {
	UID, GID int
}

// ParseOwner parses the --chown value "uid:gid" (numeric only — name
// resolution against the host's /etc/passwd would be wrong for guest
// content, which is this package's main audience).
func ParseOwner(s string) (Owner, error) {
	u, g, ok := strings.Cut(s, ":")
	if !ok {
		return Owner{}, fmt.Errorf("chown %q: want uid:gid (numeric)", s)
	}
	uid, err := strconv.Atoi(u)
	if err != nil || uid < 0 {
		return Owner{}, fmt.Errorf("chown %q: bad uid", s)
	}
	gid, err := strconv.Atoi(g)
	if err != nil || gid < 0 {
		return Owner{}, fmt.Errorf("chown %q: bad gid", s)
	}
	return Owner{UID: uid, GID: gid}, nil
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
	// Chmod, when non-nil, overrides the permission bits (including
	// setuid/setgid/sticky) of every selected entry, directories
	// included.
	Chmod *fs.FileMode

	// Stdout backs the "-" side of stdio rules; nil means os.Stdout.
	Stdout io.Writer

	// Warnf, when non-nil, receives non-fatal diagnostics (skipped
	// sockets, unprivileged chown downgrades). nil silences them.
	Warnf func(format string, args ...any)
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
