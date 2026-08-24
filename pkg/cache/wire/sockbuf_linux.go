//go:build linux

package wire

import (
	"log"
	"net"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// unixSendBufferSize is the SO_SNDBUF working-set target for unix-socket
// wire connections: one maximum-size chunk frame (chunker max 1MiB plus
// wire header). The kernel doubles the setsockopt value, so the effective
// send buffer is ~2MiB — enough to stream a whole frame without the
// writer/reader ping-pong the 224KiB kernel default inflicts on every
// chunk response (measured ~30% end-to-end on snapshot restore). It is a
// deliberate private constant, not wire.MaxFrameSize: the frame ceiling
// is a protocol safety limit, while this is a socket working-set size
// that happens to be derived from the chunker maximum.
const unixSendBufferSize = 1 << 20

// tuneUnixSocket sizes a unix-socket connection's send buffer, best
// effort. Linux AF_UNIX stream backpressure is driven by the sender's
// sk_sndbuf (unix_stream_sendmsg chunks and blocks on it); the receive
// buffer is not part of this problem, so only SO_SNDBUF is touched.
//
// Strategy (avoids both the wmem_max clamp and misleading warnings):
//  1. plain SO_SNDBUF, then read the effective value back — on a host
//     with a raised net.core.wmem_max this already fully applies;
//  2. if still below target, SO_SNDBUFFORCE (bypasses the clamp; needs
//     CAP_NET_ADMIN, which cache-ctl and sandbox-ctl run with);
//  3. if the final value is still below target, warn once per process.
//
// Errors are swallowed at every step: buffer sizing must never break a
// connection that would otherwise work. Runs concurrently (one goroutine
// per connection), hence the sync.Once warning guard.
var warnSmallSendBufOnce sync.Once

func tuneUnixSocket(c net.Conn) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return
	}
	if setAndGetSendBuf(raw, unix.SO_SNDBUF) >= unixSendBufferSize {
		return
	}
	got := setAndGetSendBuf(raw, unix.SO_SNDBUFFORCE)
	if got < unixSendBufferSize {
		warnSmallSendBufOnce.Do(func() {
			log.Printf("wire: unix socket send buffer stuck at %d bytes (< %d target); "+
				"raise net.core.wmem_max or grant CAP_NET_ADMIN to avoid chunk-transfer throttling",
				got, unixSendBufferSize)
		})
	}
}

// setAndGetSendBuf sets SO_SNDBUF via opt and returns the effective
// buffer the kernel reports afterwards (the current value if the set
// fails, 0 if even the read fails). The kernel doubles the requested
// value; the read-back is what actually applies.
func setAndGetSendBuf(raw syscall.RawConn, opt int) int {
	var got int
	_ = raw.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, opt, unixSendBufferSize)
		if v, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_SNDBUF); err == nil {
			got = v
		}
	})
	return got
}
