package wire

import (
	"log"
	"net"
	"syscall"
)

// SetSocketBuffers sizes a connection's SO_SNDBUF/SO_RCVBUF to MaxFrameSize.
//
// One wire frame (header + one max-size chunk value) is the largest single
// write on a cache-wire socket, so a frame-sized buffer streams it without
// writer/reader ping-pong; the kernel doubles the requested value, yielding
// two frames in flight. The buffer is a cap, not a preallocation — no idle
// memory cost.
//
// It first tries SO_SNDBUFFORCE/SO_RCVBUFFORCE, which bypasses the
// net.core.{w,r}mem_max clamp (with the common 224KiB default, a plain
// setsockopt would silently degrade to 448KiB effective and roughly half
// the benefit). FORCE requires CAP_NET_ADMIN; without it the call fails
// with EPERM and we fall back to the plain SO_SNDBUF/SO_RCVBUF (still
// clamped) after logging once per process, so deployments that cannot
// raise the sysctl at least get a diagnosable hint in their logs.
//
// bestEffort: errors other than EPERM on the force path, and all errors on
// the fallback path, are swallowed — buffer sizing must never break a
// connection that would otherwise work.
var warnedSockBufFallback bool

func SetSocketBuffers(c net.Conn) {
	raw, ok := c.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return
	}
	rc, err := raw.SyscallConn()
	if err != nil {
		return
	}
	var forceErr error
	if err := rc.Control(func(fd uintptr) {
		forceErr = setsockoptBuffers(fd, syscall.SO_SNDBUFFORCE, syscall.SO_RCVBUFFORCE)
	}); err != nil {
		forceErr = err
	}
	if forceErr == nil {
		return
	}
	if !warnedSockBufFallback {
		warnedSockBufFallback = true
		log.Printf("wire: SO_SNDBUFFORCE %d failed (%v); falling back to plain SO_SNDBUF "+
			"(clamped by net.core.wmem_max — raise it or grant CAP_NET_ADMIN for full effect)",
			int(MaxFrameSize), forceErr)
	}
	// Plain setsockopt as fallback (clamped by wmem_max/rmem_max).
	_ = rc.Control(func(fd uintptr) {
		_ = setsockoptBuffers(fd, syscall.SO_SNDBUF, syscall.SO_RCVBUF)
	})
}

func setsockoptBuffers(fd uintptr, sndOpt, rcvOpt int) error {
	if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, sndOpt, int(MaxFrameSize)); err != nil {
		return err
	}
	return syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, rcvOpt, int(MaxFrameSize))
}
