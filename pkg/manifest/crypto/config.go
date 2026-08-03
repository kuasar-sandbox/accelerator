package crypto

import "fmt"

// LocalPolicy controls compatibility for customer-key-bound local storage.
// It is not an algorithm selector; each local wire format fixes its algorithm.
type LocalPolicy string

const (
	LocalOff      LocalPolicy = "off"
	LocalAuto     LocalPolicy = "auto"
	LocalRequired LocalPolicy = "required"
)

// Config is the YAML-tagged crypto schema. Chunk and manifest select their
// existing algorithms. Local is a compatibility policy for local storage and
// defaults to off when omitted.
type Config struct {
	Chunk    string      `yaml:"chunk"`           // "aes"
	Manifest string      `yaml:"manifest"`        // "aes"
	Local    LocalPolicy `yaml:"local,omitempty"` // off | auto | required
}

// LocalPolicy validates and resolves the local-storage policy. The zero value
// deliberately means off so existing configurations do not begin reading key
// material or encrypting local files after an upgrade.
func (c Config) LocalPolicy() (LocalPolicy, error) {
	switch c.Local {
	case "", LocalOff:
		return LocalOff, nil
	case LocalAuto, LocalRequired:
		return c.Local, nil
	default:
		return "", fmt.Errorf("crypto: local: unsupported policy %q (want off, auto, or required)", c.Local)
	}
}
