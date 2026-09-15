package client

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type interruptedGetServer struct {
	pb.UnimplementedStoreServer
	calls atomic.Int32
}

func (s *interruptedGetServer) Get(_ *pb.GetRequest, out grpc.ServerStreamingServer[pb.GetResponse]) error {
	n := s.calls.Add(1)
	if n <= 3 {
		if err := out.Send(&pb.GetResponse{Data: []byte("discard partial response")}); err != nil {
			return err
		}
		return status.Error([]codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled}[n-1], "single attempt interrupted")
	}
	return out.Send(&pb.GetResponse{Data: bytes.Repeat([]byte{0x42}, 4096)})
}
func TestGetDiscardsHalfStreamAndRecoversWithoutBusinessRetry(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &interruptedGetServer{}
	server := grpc.NewServer()
	pb.RegisterStoreServer(server, service)
	done := make(chan struct{})
	go func() { defer close(done); server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); <-done }()
	c, err := New(listener.Addr().String(), 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := int32(1); i <= 3; i++ {
		found, data, err := c.Get(context.Background(), store.PartitionChunk, store.ContentKey{})
		if err == nil || readerr.IsPermanent(err) || found || data != nil || service.calls.Load() != i {
			t.Fatalf("attempt %d: found=%v bytes=%d err=%v calls=%d", i, found, len(data), err, service.calls.Load())
		}
	}
	found, data, err := c.Get(context.Background(), store.PartitionChunk, store.ContentKey{})
	if err != nil || !found || !bytes.Equal(data, bytes.Repeat([]byte{0x42}, 4096)) || service.calls.Load() != 4 {
		t.Fatalf("recovery spliced/replayed a stream: bytes=%d err=%v", len(data), err)
	}
}
