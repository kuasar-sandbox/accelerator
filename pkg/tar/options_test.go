package tar

import "testing"

func TestParseOwnerNumeric(t *testing.T) {
	cases := []struct {
		in       string
		uid, gid int
		bad      bool
	}{
		{in: "0:0", uid: 0, gid: 0},
		{in: "1000:1000", uid: 1000, gid: 1000},
		{in: "1000:2000", uid: 1000, gid: 2000},
		{in: "1000", uid: 1000, gid: 1000}, // numeric, no group → mirror
		{in: "", bad: true},
		{in: ":5", bad: true},
		{in: "5:", bad: true},
		{in: "-1:0", bad: true},
	}
	for _, c := range cases {
		o, err := ParseOwner(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("ParseOwner(%q): want error, got %+v", c.in, o)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseOwner(%q): %v", c.in, err)
			continue
		}
		if o.UID != c.uid || o.GID != c.gid {
			t.Errorf("ParseOwner(%q) = %d:%d, want %d:%d", c.in, o.UID, o.GID, c.uid, c.gid)
		}
	}
}

// TestParseOwnerName resolves against the real /etc/passwd; "root" (uid 0)
// is the one name present on every Linux host. Name resolution is the COPY
// --chown=<name> path (Docker semantics, resolved against the target rootfs).
func TestParseOwnerName(t *testing.T) {
	o, err := ParseOwner("root:root")
	if err != nil {
		t.Skipf("root:root lookup unavailable in this env: %v", err)
	}
	if o.UID != 0 || o.GID != 0 {
		t.Errorf(`ParseOwner("root:root") = %d:%d, want 0:0`, o.UID, o.GID)
	}
	// Single name → user's primary group (root's is 0).
	if o, err := ParseOwner("root"); err == nil && (o.UID != 0 || o.GID != 0) {
		t.Errorf(`ParseOwner("root") = %d:%d, want 0:0`, o.UID, o.GID)
	}
}
