package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/transfer"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tailzip"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"gopkg.in/yaml.v3"
)

func TestExplicitCarrierIdentitySurvivesDisabledOrdinaryVerification(t *testing.T) {
	cfg, config, _ := transferConfig(t, true)
	disabled := false
	cfg.Manifest.VerifyContent = &disabled
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(config, raw, 0600); err != nil {
		t.Fatal(err)
	}
	body := []byte("unique-source-payload-for-identity-check")
	path, encoded, digest := artifactFixture(t, body, nil, nil)
	index := bytes.Index(encoded, body)
	if index < 0 {
		t.Fatal("payload not found")
	}
	encoded[index] ^= 1
	if err = os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	for _, storeMode := range []bool{false, true} {
		out, _, err := invokeTransfer(t, config, storeMode, nil, "file://"+path+"@digest:"+digest)
		if !errors.Is(err, tarstream.ErrDigestMismatch) {
			t.Fatalf("store=%v accepted stale identity: output=%d err=%v", storeMode, len(out), err)
		}
	}
}

func TestTarstreamUsesContentNotBundleExtension(t *testing.T) {
	_, config, _ := transferConfig(t, false)
	path, _, _ := artifactFixture(t, []byte("payload"), nil, nil)
	renamed := filepath.Join(filepath.Dir(path), "data.bundle")
	if err := os.Rename(path, renamed); err != nil {
		t.Fatal(err)
	}
	out, _, err := invokeTransfer(t, config, false, nil, renamed)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := readArtifact(t, out)
	if string(body) != "payload" {
		t.Fatalf("unexpected body %q", body)
	}
}

type declaredBoundarySource struct {
	sparse.Source
	boundary uint64
}

func (s declaredBoundarySource) PayloadCommitment() (uint64, [32]byte, bool) {
	return s.boundary, [32]byte{}, false
}
func (s declaredBoundarySource) TarStreamDigest(string) ([32]byte, bool) { return [32]byte{}, false }

func TestDeclaredTailMustStartAtZIPBoundary(t *testing.T) {
	_, config, _ := transferConfig(t, false)
	tail, err := tailzip.Encode([]tailzip.Entry{{Name: "config.json", Body: []byte("{}")}})
	if err != nil {
		t.Fatal(err)
	}
	raw := append([]byte("payloadJUNK"), tail...)
	src, _ := sparse.NewSource(bytes.NewReader(raw), uint64(len(raw)), nil)
	var encoded bytes.Buffer
	if _, _, err = tarstream.WriteTo(context.Background(), &encoded, "image", declaredBoundarySource{src, 7}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "mismatched.tar")
	if err = os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{path, "-"} {
		_, _, err := invokeTransfer(t, config, false, bytes.NewBuffer(encoded.Bytes()), "--strip-tail", input)
		if err == nil {
			t.Fatalf("input %s accepted declared-tail junk", input)
		}
	}
}

func TestTarstreamOnlyTransferReportsLogicalProgress(t *testing.T) {
	cfg, _, _ := transferConfig(t, false)
	key, err := cfg.CustomerKey()
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x51}, 256<<10)
	source, _ := sparse.NewSource(bytes.NewReader(data), uint64(len(data)), nil)
	var out bytes.Buffer
	calls := 0
	last := uint64(0)
	_, err = transfer.Write(context.Background(), cfg, key, source, transfer.WriteOptions{Mode: "tarstream", Name: "image", Output: &out, OnProgress: func(done, total uint64) {
		if done < last || done > total || total != uint64(len(data)) {
			t.Errorf("invalid progress %d/%d after %d", done, total, last)
		}
		calls++
		last = done
	}})
	if err != nil {
		t.Fatal(err)
	}
	if calls == 0 || last != source.Size() {
		t.Fatalf("missing progress calls=%d last=%d", calls, last)
	}
}

func TestStoredManifestDedupBytes(t *testing.T) {
	cfg, _, _ := transferConfig(t, true)
	key, err := cfg.CustomerKey()
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte{0x31}, 8192)
	for i := 0; i < 2; i++ {
		src, _ := sparse.NewSource(bytes.NewReader(body), uint64(len(body)), nil)
		result, err := transfer.Write(context.Background(), cfg, key, src, transfer.WriteOptions{Mode: "none", Store: true})
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && result.Stats.StoredBytes != 0 {
			t.Fatalf("deduplicated transfer reports %d new bytes", result.Stats.StoredBytes)
		}
	}
}

func TestManifestRoundtripPreservesTailCarrierIdentity(t *testing.T) {
	cfg, config, _ := transferConfig(t, true)
	tail, err := tailzip.Encode([]tailzip.Entry{{Name: "config.json", Body: []byte("{\"Cmd\":[\"sh\"]}")}})
	if err != nil {
		t.Fatal(err)
	}
	path, original, digest := artifactFixture(t, bytes.Repeat([]byte{0x73}, 8192), nil, tail)
	key, _, err := invokeTransfer(t, config, true, nil, path)
	if err != nil {
		t.Fatal(err)
	}
	returned, _, err := invokeTransfer(t, config, false, nil, strings.TrimSpace(string(key)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(returned, original) {
		t.Error("Store/load changed tail boundary or physical carrier identity")
	}
	cfg.Crypto.Local = manifestcrypto.LocalRequired
	yamlBytes, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(config, yamlBytes, 0600); err != nil {
		t.Fatal(err)
	}
	encrypted, _, err := invokeTransfer(t, config, false, nil, strings.TrimSpace(string(key)))
	if err != nil {
		t.Fatal(err)
	}
	customer, err := cfg.CustomerKey()
	if err != nil {
		t.Fatal(err)
	}
	codec, _, err := localTarStreamCodec(cfg, customer)
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := manifest.ParseHexKey(digest)
	expected := codec.KeyedDigest(canonical)
	_, _, err = tarstream.SourceAt(bytes.NewReader(encrypted), int64(len(encrypted)), "image", tarstream.WithCodec(codec, true), tarstream.WithExpectedDigest("hmac", manifest.HexKey(expected)))
	if err != nil {
		t.Fatalf("reencryption changed expected carrier identity: %v", err)
	}
}
