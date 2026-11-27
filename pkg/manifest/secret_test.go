package manifest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
)

func TestCustomerKeyPlugin(t *testing.T) {
	if os.Getenv("KUASAR_MANIFEST_SECRET_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCustomerKeyPlugin$")
		cmd.Env = append(os.Environ(), "KUASAR_MANIFEST_SECRET_TEST_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated process: %v\n%s", err, out)
		}
		return
	}
	dir, err := os.MkdirTemp("", "manifest-secret-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "broker.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte{0x71}, 32)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		value := want
		if req.Key == "short" {
			value = []byte{1, 2}
		}
		json.NewEncoder(w).Encode(map[string]string{"key": req.Key, "value": "base64:" + base64.StdEncoding.EncodeToString(value)})
	})}
	go server.Serve(listener)
	defer server.Close()
	t.Setenv(secret.Env, "kms::"+path+":cache_ttl=0")
	t.Setenv(CustomerKeyEnv, "@kms/env")
	cfg := &Config{Manifest: ManifestSubConfig{Key: "@kms/config"}}
	got, err := cfg.CustomerKeyContext(context.Background())
	if err != nil || !bytes.Equal(got[:], want) {
		t.Fatalf("resolved key: %x %v", got, err)
	}
	if cfg.Manifest.Key != "@kms/config" || os.Getenv(CustomerKeyEnv) != "@kms/env" {
		t.Fatal("resolved material replaced reference text")
	}
	t.Setenv(CustomerKeyEnv, "")
	if got, err := cfg.CustomerKey(); err != nil || !bytes.Equal(got[:], want) {
		t.Fatalf("configuration reference: %x %v", got, err)
	}
	t.Setenv(CustomerKeyEnv, "@kms/short")
	if _, err := cfg.CustomerKey(); err == nil || !strings.Contains(err.Error(), "must be 32 bytes") {
		t.Fatalf("length validation: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cfg.CustomerKeyContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
