//go:build !linux

package wire

import "net"

// tuneUnixSocket is a no-op off Linux: SO_SNDBUFFORCE is Linux-only and
// the throttling being worked around is Linux AF_UNIX backpressure.
func tuneUnixSocket(c net.Conn) {}
