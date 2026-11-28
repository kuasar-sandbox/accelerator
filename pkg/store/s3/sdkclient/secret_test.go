package sdkclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
)

func TestStaticCredentialsSecretPlugin(t *testing.T) {
	if os.Getenv("KUASAR_S3_SECRET_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestStaticCredentialsSecretPlugin$")
		cmd.Env = append(os.Environ(), "KUASAR_S3_SECRET_TEST_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated process: %v\n%s", err, out)
		}
		return
	}
	setDefaultCredentialEnvironment(t)
	dir, err := os.MkdirTemp("", "s3-secret-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "broker.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var rotated atomic.Bool
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if req.Key == "broken" {
			w.WriteHeader(503)
			return
		}
		value := req.Key + "-v1"
		if rotated.Load() {
			value = req.Key + "-v2"
		}
		if req.Key == "empty" {
			value = ""
		}
		json.NewEncoder(w).Encode(map[string]string{"key": req.Key, "value": "base64:" + base64.StdEncoding.EncodeToString([]byte(value))})
	})}
	go server.Serve(listener)
	defer server.Close()
	t.Setenv(secret.Env, "kms::"+path+":cache_ttl=0")
	ctx := context.Background()
	cfg := Config{Endpoint: "http://unused.invalid", Bucket: "test", AccessKey: "@kms/ak", SecretKey: "@kms/sk"}
	client, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	creds, err := client.api.Options().Credentials.Retrieve(ctx)
	if err != nil || creds.AccessKeyID != "ak-v1" || creds.SecretAccessKey != "sk-v1" {
		t.Fatalf("credentials: %+v %v", creds, err)
	}
	if cfg.AccessKey != "@kms/ak" || cfg.SecretKey != "@kms/sk" {
		t.Fatal("configuration replaced by resolved material")
	}
	rotated.Store(true)
	creds, err = client.api.Options().Credentials.Retrieve(ctx)
	if err != nil || creds.AccessKeyID != "ak-v1" || calls.Load() != 2 {
		t.Fatalf("existing client re-resolved static credentials: calls=%d error=%v", calls.Load(), err)
	}
	second, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	creds, err = second.api.Options().Credentials.Retrieve(ctx)
	if err != nil || creds.AccessKeyID != "ak-v2" || creds.SecretAccessKey != "sk-v2" || calls.Load() != 4 {
		t.Fatalf("new client: %+v %v calls=%d", creds, err, calls.Load())
	}
	for _, key := range []string{"broken", "empty"} {
		bad := cfg
		bad.AccessKey = "@kms/" + key
		if client, err := New(ctx, bad); err == nil {
			client.Close()
			t.Fatalf("accepted %s plugin value", key)
		}
	}
	providerCfg := cfg
	providerCfg.AccessKey = "@missing/no-fallback"
	providerCfg.Credentials = credentialsProviderFunc(func(context.Context) (Credentials, error) {
		return Credentials{AccessKeyID: "provider-ak", SecretAccessKey: "provider-sk"}, nil
	})
	before := calls.Load()
	provided, err := New(ctx, providerCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer provided.Close()
	creds, err = provided.api.Options().Credentials.Retrieve(ctx)
	if err != nil || creds.AccessKeyID != "provider-ak" || calls.Load() != before {
		t.Fatalf("provider precedence: %+v %v", creds, err)
	}
	if _, err := New(ctx, Config{Endpoint: cfg.Endpoint, Bucket: cfg.Bucket, AccessKey: "@missing/key", SecretKey: "literal"}); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("missing plugin was not authoritative: %v", err)
	}
}
