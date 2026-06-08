package flatten

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// capCHOWN is the Linux capability number for CAP_CHOWN; it occupies bit 0 of
// the capability bitmasks reported in /proc/self/status.
const capCHOWN = 0

// RequireOwnershipCap verifies the current process can chown files to arbitrary
// uid/gid — the privilege Build needs to preserve each image entry's real
// ownership (see applyOwnerMode). The check looks at the effective capability
// set (CapEff) in /proc/self/status and, only if that can't be read, falls back
// to a root-euid check. Callers run it at startup so an unprivileged export
// fails fast with a clear message instead of partway through the first layer's
// chown (after a potentially expensive pull + extract).
func RequireOwnershipCap() error {
	if hasEffectiveChownCap() {
		return nil
	}
	return fmt.Errorf("preserving image file ownership requires root/CAP_CHOWN; rerun as root or grant CAP_CHOWN")
}

// hasEffectiveChownCap reports whether the process holds CAP_CHOWN in its
// effective set. It parses CapEff from /proc/self/status; if that file is
// unreadable or malformed it falls back to euid==0. Parsing CapEff (rather than
// only checking euid) correctly accepts a non-root process that was granted
// CAP_CHOWN and rejects a root process that dropped it.
func hasEffectiveChownCap() bool {
	capEff, ok := readCapEff()
	if !ok {
		return os.Geteuid() == 0
	}
	return capEff&(1<<uint(capCHOWN)) != 0
}

// readCapEff returns the CapEff bitmask from /proc/self/status.
func readCapEff() (uint64, bool) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "CapEff:") {
			continue
		}
		hex := strings.TrimSpace(strings.TrimPrefix(line, "CapEff:"))
		v, err := strconv.ParseUint(hex, 16, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}
