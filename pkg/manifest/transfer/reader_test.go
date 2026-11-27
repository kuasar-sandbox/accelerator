package transfer

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func TestRemoteReaderUsesAlreadyBoundCustomerKey(t *testing.T) {
	// A changed or unavailable reference must not be re-resolved when a transfer
	// first opens its remote Getter after constructing its local reader.
	t.Setenv(manifest.CustomerKeyEnv, "@unconfigured/changed")
	cfg := &manifest.Config{Crypto: crypto.Config{Chunk: "aes", Manifest: "aes"}}
	r, err := NewReader(cfg, [32]byte{7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, err = r.remoteFetcher()
	if err == nil || !strings.Contains(err.Error(), "cache.endpoint or store.endpoint required") {
		t.Fatalf("remote reader did not retain the supplied key: %v", err)
	}
}

func TestOpenTarstreamPinsCarrierAndVerifiesWithReadPolicy(t *testing.T) {
	ctx := context.Background()
	codec, err := crypto.NewTarStreamCodec([32]byte{7})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, encrypted := range []bool{false, true} {
		path := filepath.Join(dir, "input")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		var write []tarstream.WriteOption
		if encrypted {
			write = append(write, tarstream.WithCodec(codec, true))
		}
		body := bytes.Repeat([]byte{2}, 128<<10)
		scheme, digest, err := tarstream.WriteTo(ctx, f, "overlay", sparse.Dense(bytes.NewReader(body), uint64(len(body))), write...)
		if err = errors.Join(err, f.Close()); err != nil {
			t.Fatal(err)
		}
		if !encrypted {
			if _, err := OpenTarstream(path, tarstream.WithCodec(codec, true)); !errors.Is(err, tarstream.ErrPlaintextForbidden) {
				t.Fatalf("required accepted plaintext: %v", err)
			}
		} else if _, err := OpenTarstream(path); !errors.Is(err, tarstream.ErrCodecRequired) {
			t.Fatalf("unencrypted reader accepted ciphertext: %v", err)
		}
		opened, err := OpenTarstream(path, tarstream.WithCodec(codec, false), tarstream.WithExpectedDigest(scheme, digest))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path, path+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("different carrier"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(opened.Verify(ctx), opened.Close()); err != nil {
			t.Fatalf("lost pinned file: %v", err)
		}
	}
}
