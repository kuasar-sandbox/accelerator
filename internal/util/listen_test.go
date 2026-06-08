package util

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestUnixAddr(t *testing.T) {
	cases := []struct {
		in       string
		wantPath string
		wantOK   bool
	}{
		{"127.0.0.1:7070", "", false},
		{"0.0.0.0:7100", "", false},
		{"/run/sandbox/store.sock", "/run/sandbox/store.sock", true},
		{"unix:///run/x.sock", "/run/x.sock", true},
		{"unix:/run/x.sock", "/run/x.sock", true},
	}
	for _, c := range cases {
		gotPath, gotOK := UnixAddr(c.in)
		if gotPath != c.wantPath || gotOK != c.wantOK {
			t.Errorf("UnixAddr(%q) = (%q,%v), want (%q,%v)", c.in, gotPath, gotOK, c.wantPath, c.wantOK)
		}
	}
}

func TestListenUnixBindAndPerms(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "nested", "x.sock") // nested dir must be created
	lis, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen(%q): %v", sock, err)
	}
	defer lis.Close()

	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		t.Errorf("path is not a socket: mode %v", fi.Mode())
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("socket perm = %o, want 600", perm)
	}

	// A connection round-trips, proving the listener accepts.
	go func() {
		c, _ := lis.Accept()
		if c != nil {
			c.Close()
		}
	}()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.Close()
}

func TestListenUnixClearsStaleSocket(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "x.sock")
	// First bind, then close — Go unlinks on Close, so simulate a leftover by
	// re-creating a dead socket file via a listener we abandon without unlink.
	l1, err := Listen(sock)
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	ul := l1.(*net.UnixListener)
	ul.SetUnlinkOnClose(false) // leave a stale socket file behind
	l1.Close()
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("expected a stale socket file, got: %v", err)
	}

	// Second bind must detect the stale (no live listener) socket and replace it.
	l2, err := Listen(sock)
	if err != nil {
		t.Fatalf("second Listen over stale socket: %v", err)
	}
	l2.Close()
}

func TestListenUnixRefusesLiveSocket(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "x.sock")
	l1, err := Listen(sock)
	if err != nil {
		t.Fatalf("first Listen: %v", err)
	}
	defer l1.Close()

	// A second bind while the first listener is live must NOT steal the path.
	if _, err := Listen(sock); err == nil {
		t.Fatal("expected 'address already in use', got nil")
	}
}

func TestListenUnixRefusesNonSocketPath(t *testing.T) {
	reg := filepath.Join(t.TempDir(), "regular.file")
	if err := os.WriteFile(reg, []byte("not a socket"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(reg); err == nil {
		t.Fatal("expected refusal to clobber a non-socket path, got nil")
	}
	// The file must survive the failed bind.
	if _, err := os.Stat(reg); err != nil {
		t.Errorf("Listen removed a non-socket path: %v", err)
	}
}
