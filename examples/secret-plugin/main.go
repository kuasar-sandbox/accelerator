// The Secret plugin example serves fixed demonstration bytes over a private
// Unix socket. Replace resolveDemo to connect your own secret backend.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/kuasar-sandbox/accelerator/pkg/secret/plugin"
)

func main() {
	socket := flag.String("socket", "", "absolute Unix socket path in an existing private directory")
	flag.Parse()
	if *socket == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: secret-plugin-example --socket /private/directory/plugin.sock")
		os.Exit(2)
	}
	log.SetFlags(0)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *socket); err != nil {
		log.Printf("secret-plugin-example: %v", err)
		os.Exit(1)
	}
}

// resolveDemo is the backend integration point. These publicly known bytes are
// only demonstration data, never a production key. A real implementation should
// pass ctx to its backend, bound external calls, and return the raw secret bytes.
func resolveDemo(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key != "example-key" {
		return nil, os.ErrNotExist
	}
	value := make([]byte, 32)
	for i := range value {
		value[i] = byte(i)
	}
	return value, nil
}

func run(ctx context.Context, socket string) (err error) {
	listener, cleanup, err := listenSocket(socket)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, cleanup()) }()
	log.Printf("demo Secret plugin listening on %s", socket)
	return serve(ctx, listener)
}

func listenSocket(socket string) (*net.UnixListener, func() error, error) {
	if !filepath.IsAbs(socket) || strings.ContainsRune(socket, ':') || strings.IndexFunc(socket, unicode.IsSpace) >= 0 {
		return nil, nil, errors.New("socket path must be absolute and contain no colon or whitespace")
	}
	dir, err := os.Lstat(filepath.Dir(socket))
	if err != nil {
		return nil, nil, fmt.Errorf("inspect socket directory: %w", err)
	}
	owner, ok := dir.Sys().(*syscall.Stat_t)
	if !dir.IsDir() || dir.Mode().Perm() != 0o700 || !ok || owner.Uid != uint32(os.Geteuid()) {
		return nil, nil, errors.New("socket directory must be owned by the current user and have mode 0700")
	}
	if _, err := os.Lstat(socket); err == nil {
		return nil, nil, errors.New("socket path already exists; use a fresh private directory")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("inspect socket path: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return nil, nil, fmt.Errorf("listen: %w", err)
	}
	// Do not let Close unlink a different file later placed at this path.
	listener.SetUnlinkOnClose(false)
	owned, err := os.Lstat(socket)
	if err != nil {
		_ = listener.Close()
		return nil, nil, fmt.Errorf("inspect created socket: %w", err)
	}
	cleanup := func() error {
		_ = listener.Close()
		current, err := os.Lstat(socket)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !os.SameFile(owned, current) {
			return nil
		}
		return os.Remove(socket)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		return nil, nil, errors.Join(fmt.Errorf("set socket permissions: %w", err), cleanup())
	}
	return listener, cleanup, nil
}

func serve(ctx context.Context, listener net.Listener) error {
	server := &http.Server{
		Handler:           plugin.NewHandler(resolveDemo),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    8 << 10,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			<-done
			return fmt.Errorf("shutdown: %w", err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
