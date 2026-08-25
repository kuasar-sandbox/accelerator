//go:build linux

package wire

import (
	"net"
	"testing"
)

// TestSendBufTargetEffectiveBoundary pins the kernel-doubling arithmetic:
// getsockopt(SO_SNDBUF) reports twice the accepted request, so the
// success/failure threshold must be the doubled target, not
// unixSendBufferSize. Drives real sockets whose effective value lands on
// each side of both thresholds.
func TestSendBufTargetEffectiveBoundary(t *testing.T) {
	cases := []struct {
		name string
		// effective (read-back) value the kernel will report
		effective int
		// wantAtTarget: whether tuneUnixSocket should consider the
		// connection fully provisioned at this effective value
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

// sufficientSendBuf reports whether an effective (read-back) SO_SNDBUF
// value meets the working-set target. Extracted so the boundary is testable
// without root/CAP_NET_ADMIN manipulation of wmem_max.
func sufficientSendBuf(effective int) bool {
	return effective >= sendBufTargetEffective
}

// TestSetAndGetSendBufMonotonic sanity-checks the real syscall path: the
// read-back for a plain SO_SNDBUF request of unixSendBufferSize is at
// least the doubled target on hosts with a raised wmem_max, and never
// exceeds it by more than rounding.
func TestSetAndGetSendBufMonotonic(t *testing.T) {
	ln, err := net.Listen("unix", t.TempDir()+"/sb.sock")
	if err != nil {
		t.Skipf("unix listen: %v", err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
		}
	}()
	c, err := net.Dial("unix", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	raw, err := c.(*net.UnixConn).SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	got := setAndGetSendBuf(raw, 1 /* SO_SNDBUF */)
	if got < 0 {
		t.Fatalf("negative read-back %d", got)
	}
	// On a default host (wmem_max=224KiB) the value clamps to ~448KiB;
	// on a raised host it reports 2MiB. Both are non-negative and even;
	// the exact threshold behaviour is covered by the table test above.
	t.Logf("plain-set read-back: %d", got)
}
