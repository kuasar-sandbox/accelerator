package transfer

import (
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

// LocalCodec resolves the existing per-process file encryption policy.
func LocalCodec(cfg *manifest.Config, key [32]byte) (tarstream.Codec, bool, error) {
	policy, err := cfg.Crypto.LocalPolicy()
	if err != nil {
		return nil, false, err
	}
	if policy == manifestcrypto.LocalOff {
		return nil, false, nil
	}
	codec, err := manifestcrypto.NewTarStreamCodec(key)
	return codec, policy == manifestcrypto.LocalRequired, err
}
func readOptions(codec tarstream.Codec, required bool) []tarstream.ReadOption {
	if codec == nil {
		return nil
	}
	return []tarstream.ReadOption{tarstream.WithCodec(codec, required)}
}
func writeOptions(codec tarstream.Codec, required bool) []tarstream.WriteOption {
	if codec == nil {
		return nil
	}
	return []tarstream.WriteOption{tarstream.WithCodec(codec, required)}
}
