package wire

import (
	"bytes"
	"net"
	"testing"

	"github.com/fullof-work/mass-sandbox/pkg/cache"
)

// BenchmarkWireCodec_RoundTrip_512KB measures the wire protocol
// round-trip (WriteRequest ObjectGet → server WriteResponse StatusHit
// 512KB payload → client ReadResponse) over a loopback TCP pair. This
// isolates the codec + bufio + net.Buffers (writev) + TCP kernel cost
// without any cache logic, so we can compare per-op cost against the
// full-path integration bench.
func BenchmarkWireCodec_RoundTrip_512KB(b *testing.B) {
	srv, cli := tcpPipeForBench(b)
	defer srv.Close()
	defer cli.Close()

	srvConn := NewConn(srv)
	cliConn := NewConn(cli)

	value := bytes.Repeat([]byte("X"), 512*1024)
	serverErr := make(chan error, 1)

	// Server loop: read req, write 512KB response, for b.N iterations.
	go func() {
		for i := 0; i < b.N; i++ {
			req, err := srvConn.ReadRequest()
			if err != nil {
				serverErr <- err
				return
			}
			req.Release()
			// Fresh memBlob per iteration (ReadResponse allocates on
			// receive; production rocks.Get similarly returns a fresh
			// buffer, so bench parity is OK).
			resp := &Response{Status: StatusHit, Value: cache.NewMemBlob(value)}
			if err := srvConn.WriteResponse(resp); err != nil {
				serverErr <- err
				return
			}
			resp.Value.Release()
		}
		serverErr <- nil
	}()

	req := &Request{
		Opcode:    OpcodeObjectGet,
		Namespace: NSChunk,
		Hash:      sha256Hash("wire-bench"),
	}

	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cliConn.WriteRequest(req); err != nil {
			b.Fatalf("WriteRequest: %v", err)
		}
		resp, err := cliConn.ReadResponse(nil)
		if err != nil {
			b.Fatalf("ReadResponse: %v", err)
		}
		if resp.Status != StatusHit {
			b.Fatalf("status=%d", resp.Status)
		}
		resp.Value.Release()
	}
	b.StopTimer()

	if err := <-serverErr; err != nil {
		b.Fatalf("server: %v", err)
	}
}

func tcpPipeForBench(b *testing.B) (net.Conn, net.Conn) {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	var srvConn net.Conn
	accepted := make(chan struct{})
	go func() {
		srvConn, _ = ln.Accept()
		close(accepted)
	}()

	cli, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	<-accepted
	return srvConn, cli
}
