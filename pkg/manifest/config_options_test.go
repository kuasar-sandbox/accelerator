package manifest

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"

	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storefs "github.com/kuasar-sandbox/accelerator/pkg/store/fs"
	"github.com/kuasar-sandbox/accelerator/pkg/store/pb"
	storeserver "github.com/kuasar-sandbox/accelerator/pkg/store/server"
)

func TestWriteAdmissionOfflineDefaultAndExplicitGeneration(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured string
		want       string
	}{
		{name: "default NONE", want: "NONE"},
		{name: "explicit", configured: "G3", want: "G3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Manifest: ManifestSubConfig{WriteGeneration: tc.configured}}
			admission, err := cfg.WriteAdmission(context.Background())
			if err != nil {
				t.Fatalf("WriteAdmission: %v", err)
			}
			if string(admission.Generation) != tc.want {
				t.Fatalf("generation = %q, want %q", admission.Generation, tc.want)
			}
			wantSalt, err := store.SaltForGeneration(admission.Generation)
			if err != nil {
				t.Fatal(err)
			}
			if admission.Salt != wantSalt {
				t.Fatalf("salt = %x, want %x", admission.Salt, wantSalt)
			}
		})
	}
}

func TestManifestReadOptionsDefaultTrueAndParseFalse(t *testing.T) {
	defaultConfig, err := ParseConfig([]byte("manifest: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !defaultConfig.fetchOptions().VerifyContent {
		t.Fatal("omitted manifest.verify_content did not default true")
	}
	disabledConfig, err := ParseConfig([]byte("manifest:\n  verify_content: false\n  write_generation: G3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if disabledConfig.fetchOptions().VerifyContent {
		t.Fatal("manifest.verify_content=false was ignored")
	}
	if disabledConfig.Manifest.WriteGeneration != "G3" {
		t.Fatalf("write_generation = %q", disabledConfig.Manifest.WriteGeneration)
	}
}

func TestWriteAdmissionRejectsInvalidOfflineGeneration(t *testing.T) {
	cfg := Config{Manifest: ManifestSubConfig{WriteGeneration: "bad/generation"}}
	if _, err := cfg.WriteAdmission(context.Background()); err == nil {
		t.Fatal("invalid write_generation accepted")
	}
}

func TestWriteAdmissionStoreFailureDoesNotFallBackOffline(t *testing.T) {
	cfg := Config{
		Manifest: ManifestSubConfig{WriteGeneration: "G3"},
		Store: StoreConfig{
			Endpoint: "unix:///definitely/missing/accelerator-store.sock",
			Pool:     1,
			Timeout:  "20ms",
		},
	}
	if _, err := cfg.WriteAdmission(context.Background()); err == nil {
		t.Fatal("Store admission failure fell back to offline derivation")
	}
}

func TestWriteAdmissionOnlineLatestExplicitRemovedAndNONE(t *testing.T) {
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
			return []store.Generation{"NONE", "G1", "G2"}
		},
		VerifyKey: true,
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

	for _, tc := range []struct {
		name       string
		configured string
		want       store.Generation
		wantError  bool
	}{
		{name: "latest", want: "G2"},
		{name: "active older", configured: "G1", want: "G1"},
		{name: "ordinary NONE", configured: "NONE", want: "NONE"},
		{name: "removed", configured: "G0", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Manifest: ManifestSubConfig{WriteGeneration: tc.configured},
				Store:    StoreConfig{Endpoint: socket, Pool: 1, Timeout: "1s"},
			}
			admission, err := cfg.WriteAdmission(context.Background())
			if tc.wantError {
				if err == nil {
					t.Fatal("removed generation accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if admission.Generation != tc.want {
				t.Fatalf("generation = %q, want %q", admission.Generation, tc.want)
			}
			wantSalt, err := store.SaltForGeneration(tc.want)
			if err != nil {
				t.Fatal(err)
			}
			if admission.Salt != wantSalt {
				t.Fatal("Store returned a non-canonical salt")
			}
		})
	}
}
