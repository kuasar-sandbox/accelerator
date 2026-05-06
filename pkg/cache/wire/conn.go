package wire

import (
	"bufio"
	"net"
	"time"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
)

// Conn wraps a net.Conn with buffered reads and direct (writev) writes.
//
// Read side uses a bufio.Reader to coalesce small TCP segments.
// Write side writes directly to the raw connection so that net.Buffers
// (writev) can send header + value in a single syscall without copying.
type Conn struct {
	raw net.Conn
	br  *bufio.Reader
}

// NewConn wraps c with a 64 KB read buffer.
func NewConn(c net.Conn) *Conn {
	return &Conn{
		raw: c,
		br:  bufio.NewReaderSize(c, 64*1024),
	}
}

// ReadRequest reads a request frame through the buffered reader.
func (c *Conn) ReadRequest() (*Request, error) { return ReadRequest(c.br) }

// ReadResponse reads a response frame through the buffered reader,
// allocating the value payload from pool.
//
// Large-payload fast path: for payloads bigger than the bufio buffer,
// reads header through bufio (small, amortised syscall savings), then
// drains whatever bufio has prefetched into the pool-allocated value
// buffer, and fills the remainder directly from the raw conn with
// io.ReadFull. This eliminates the kernel→bufio→pool memcpy that
// dominated ReadResponse CPU (~22% of target) for 131 KiB shard
// payloads.
func (c *Conn) ReadResponse(pool cache.BlobPool) (*Response, error) {
	return readResponseFast(c.br, c.raw, pool)
}

// WriteRequest writes a request frame directly to the raw connection.
func (c *Conn) WriteRequest(r *Request) error { return WriteRequest(c.raw, r) }

// WriteResponse writes a response frame directly to the raw connection.
func (c *Conn) WriteResponse(r *Response) error { return WriteResponse(c.raw, r) }

// SetReadDeadline sets the read deadline on the underlying connection.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.raw.SetReadDeadline(t) }

// SetWriteDeadline sets the write deadline on the underlying connection.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }

// SetDeadline sets both read and write deadlines in a single syscall.
// Preferred over separate SetReadDeadline+SetWriteDeadline for hot
// paths — each of those is a syscall, this is one.
func (c *Conn) SetDeadline(t time.Time) error { return c.raw.SetDeadline(t) }

// SendCancel writes a CancelRequest frame directly to the raw
// connection, bypassing the bufio.Reader (which is read-side only).
// Safe to call from another goroutine while ReadResponse is blocked
// on the same Conn — Go's net.Conn supports concurrent Read+Write.
// NOT safe to call while WriteRequest is in progress (concurrent
// Write+Write is undefined); callers must ensure writes are serialised.
func (c *Conn) SendCancel() error {
	return WriteRequest(c.raw, &Request{Opcode: OpcodeCancelRequest})
}

// Close closes the underlying connection.
func (c *Conn) Close() error { return c.raw.Close() }
