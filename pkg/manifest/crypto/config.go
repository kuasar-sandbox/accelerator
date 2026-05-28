package crypto

// Config is the YAML-tagged crypto schema. Two algorithms are
// configurable independently: chunk encryption and manifest-key-table
// encryption. In production both should be "aes"; "fake" exists for
// performance baselines (HMAC + plaintext, NOT secure) and must be
// rejected by any code path that has a production safety guard.
type Config struct {
	Chunk    string `yaml:"chunk"`    // "aes" | "fake"
	Manifest string `yaml:"manifest"` // "aes" | "fake"
}
