package main

import (
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
)

func TestLocalTarStreamCodecPolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		policy       manifestcrypto.LocalPolicy
		wantCodec    bool
		wantRequired bool
	}{
		{name: "default"},
		{name: "off", policy: manifestcrypto.LocalOff},
		{name: "auto", policy: manifestcrypto.LocalAuto, wantCodec: true},
		{name: "required", policy: manifestcrypto.LocalRequired, wantCodec: true, wantRequired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &manifest.Config{Crypto: manifestcrypto.Config{Local: tc.policy}}
			codec, required, err := localTarStreamCodec(cfg, [32]byte{0x42})
			if err != nil {
				t.Fatal(err)
			}
			if (codec != nil) != tc.wantCodec || required != tc.wantRequired {
				t.Fatalf("localTarStreamCodec() = (codec=%v, required=%v), want (%v, %v)", codec != nil, required, tc.wantCodec, tc.wantRequired)
			}
			if got := len(localReadOptions(codec, required)); got != btoi(tc.wantCodec) {
				t.Fatalf("read options = %d", got)
			}
			if got := len(localWriteOptions(codec, required)); got != btoi(tc.wantCodec) {
				t.Fatalf("write options = %d", got)
			}
		})
	}

	cfg := &manifest.Config{Crypto: manifestcrypto.Config{Local: "aes"}}
	if _, _, err := localTarStreamCodec(cfg, [32]byte{}); err == nil {
		t.Fatal("invalid local policy accepted")
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}
