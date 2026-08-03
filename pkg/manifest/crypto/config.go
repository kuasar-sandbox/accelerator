package crypto

// Config is the YAML-tagged crypto schema. Two algorithms are configurable
// independently: chunk encryption and manifest-key-table encryption. The only
// supported value for each is "aes"; any other value is rejected at
// construction (crypto.New / NewChunkEncryptor), so encryption cannot be
// silently disabled by a misconfiguration.
type Config struct {
	Chunk    string `yaml:"chunk"`    // "aes"
	Manifest string `yaml:"manifest"` // "aes"
}
