package manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/codec"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
	storefs "github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

func TestGetManifestBlobVerifiesRequestedPhysicalContentKey(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "store.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := storefs.New(storefs.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	server, err := storeserver.New(storeserver.Options{
		Backend: backend,
		Generations: func() []store.Generation {
			return []store.Generation{"G1"}
		},
		VerifyKey: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterStoreServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	client, err := storeclient.New(socket, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	admission, err := client.AdmitWrite(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	physical, err := codec.Marshal(&codec.Manifest{Version: codec.Version1, ChunkMode: codec.ChunkModeFixed}, nil)
	if err != nil {
		t.Fatal(err)
	}
	actualKey := store.ContentKey(sha256.Sum256(physical))
	wrongKey := store.ContentKey(sha256.Sum256([]byte("different physical manifest")))
	if _, err := client.Put(context.Background(), admission, store.PartitionManifest, wrongKey, physical); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{Store: StoreConfig{Endpoint: socket, Pool: 1}}
	if _, err := cfg.GetManifestBlob(context.Background(), wrongKey); err == nil {
		t.Fatal("GetManifestBlob accepted bytes that did not match the requested key")
	}
	if _, err := client.Put(context.Background(), admission, store.PartitionManifest, actualKey, physical); err != nil {
		t.Fatal(err)
	}
	got, err := cfg.GetManifestBlob(context.Background(), actualKey)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, physical) {
		t.Fatal("GetManifestBlob changed physical manifest bytes")
	}
}

func TestVerifyManifestContentKeyCoversCacheBranchHelper(t *testing.T) {
	physical := []byte("physical manifest")
	key := store.ContentKey(sha256.Sum256(physical))
	if err := verifyManifestContentKey(physical, key, "cache"); err != nil {
		t.Fatal(err)
	}
	physical[0] ^= 1
	if err := verifyManifestContentKey(physical, key, "cache"); err == nil {
		t.Fatal("tampered cache bytes were accepted")
	}
}
