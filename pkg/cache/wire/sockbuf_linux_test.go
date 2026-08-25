//go:build linux

package wire

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// TestSendBufTargetEffectiveBoundary pins the kernel-doubling arithmetic
// through the production predicate: getsockopt(SO_SNDBUF) reports twice
// the accepted request, so the success/failure threshold must be the
// doubled target, not unixSendBufferSize. Effective values land on each
// side of both thresholds.
func TestSendBufTargetEffectiveBoundary(t *testing.T) {
	cases := []struct {
		name           string
		effective      int
		wantSufficient bool
	}{
		{"clamped to 1MiB (wmem_max=512KiB), reads back 1MiB", 1 << 20, false},
		{"clamped to 768KiB, reads back 1.5MiB", 3 * (1 << 19), false},
		{"one byte below target", sendBufTargetEffective - 1, false},
		{"exactly at target", sendBufTargetEffective, true},
		{"above target", sendBufTargetEffective + 4096, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sufficientSendBuf(tc.effective); got != tc.wantSufficient {
				t.Fatalf("sufficientSendBuf(%d) = %v, want %v", tc.effective, got, tc.wantSufficient)
			}
		})
	}
}

// TestSetAndGetSendBufReadback exercises the real syscall path with the
// correct option (unix.SO_SNDBUF, not a magic number): the read-back must
// be positive, equal to an independent GetsockoptInt call, and consistent
// with a plain set of unixSendBufferSize — either the kernel-doubled
// target (host with a raised wmem_max) or the wmem_max clamp (a positive
// value below the target on constrained hosts).
func TestSetAndGetSendBufReadback(t *testing.T) {
	ln, err := net.Listen("unix", t.TempDir()+"/sb.sock")
	if err != nil {
		t.Skipf("unix listen: %v", err)
	}
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			c.Close()
		}
	}()
	c, err := net.Dial("unix", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	uc := c.(*net.UnixConn)
	raw, err := uc.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}

	got := setAndGetSendBuf(raw, unix.SO_SNDBUF)
	if got <= 0 {
		t.Fatalf("read-back %d not positive", got)
	}

	// Cross-check against an independent getsockopt on the same fd.
	var direct int
	err = raw.Control(func(fd uintptr) {
		v, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
		if err != nil {
			t.Errorf("direct GetsockoptInt: %v", err)
			return
		}
		direct = v
	})
	if err != nil {
		t.Fatalf("Control: %v", err)
	}
	if got != direct {
		t.Fatalf("setAndGetSendBuf read-back %d != direct getsockopt %d", got, direct)
	}

	// A plain request of unixSendBufferSize either fully applies (read-back
	// == doubled target) or is clamped by wmem_max (read-back < target but
	// still a positive, kernel-doubled bookkeeping value).
	if got != sendBufTargetEffective && got >= sendBufTargetEffective {
		t.Fatalf("read-back %d unexpectedly above the doubled target %d", got, sendBufTargetEffective)
	}
	t.Logf("plain-set read-back: %d (target %d, sufficient=%v)", got, sendBufTargetEffective, sufficientSendBuf(got))
}
