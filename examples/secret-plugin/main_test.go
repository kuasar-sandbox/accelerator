package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
)

func privateSocket(t *testing.T) string {
	t.Helper()
	// Keep the pathname short enough for Unix sockets, including on CI hosts.
	dir, err := os.MkdirTemp("", "sp-demo-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "plugin.sock")
}

func TestResolveThroughUnixSocket(t *testing.T) {
	socket := privateSocket(t)
	listener, cleanup, err := listenSocket(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cleanup(); err != nil {
			t.Error(err)
		}
	})
	info, err := os.Lstat(socket)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions: %v, %v", info, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- serve(ctx, listener) }()
	client, err := secret.New("demo::" + socket + ":cache_ttl=0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	request, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	value, err := client.Resolve(request, "@demo/example-key")
	want := make([]byte, 32)
	for i := range want {
		want[i] = byte(i)
	}
	if err != nil || !bytes.Equal(value, want) {
		t.Fatalf("demo resolve: length=%d, err=%v", len(value), err)
	}
	if _, err := client.Resolve(request, "@demo/missing-key"); err == nil {
		t.Fatal("unknown demo key succeeded")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("server did not stop after cancellation")
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned socket remains after cleanup: %v", err)
	}
}

func TestSocketRequiresPrivateDirectory(t *testing.T) {
	for _, path := range []string{"relative.sock", "/tmp/demo:socket", "/tmp/demo socket"} {
		if _, _, err := listenSocket(path); err == nil {
			t.Errorf("accepted invalid socket path %q", path)
		}
	}
	socket := privateSocket(t)
	if err := os.Chmod(filepath.Dir(socket), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := listenSocket(socket); err == nil {
		t.Fatal("accepted a shared socket directory")
	}
}

func TestSocketPreservesExistingAndReplacementFiles(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		socket := privateSocket(t)
		want := []byte("existing file")
		if err := os.WriteFile(socket, want, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := listenSocket(socket); err == nil {
			t.Fatal("accepted an occupied socket path")
		}
		got, err := os.ReadFile(socket)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("existing file changed")
		}
	})
	t.Run("replacement", func(t *testing.T) {
		socket := privateSocket(t)
		_, cleanup, err := listenSocket(socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cleanup() })
		if _, _, err := listenSocket(socket); err == nil {
			t.Fatal("accepted an existing socket")
		}
		if err := os.Remove(socket); err != nil {
			t.Fatal(err)
		}
		want := []byte("replacement file")
		if err := os.WriteFile(socket, want, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := cleanup(); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(socket)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("cleanup removed or changed a replacement file")
		}
	})
}
