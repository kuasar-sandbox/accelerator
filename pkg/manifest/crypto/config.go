package crypto

import "fmt"

// LocalPolicy selects plaintext compatibility for a customer-key-bound storage
// domain. Each wire format fixes its algorithm.
type LocalPolicy string

const (
	LocalOff      LocalPolicy = "off"
	LocalAuto     LocalPolicy = "auto"
	LocalRequired LocalPolicy = "required"
)

// Config is the YAML-tagged crypto schema. Chunk and manifest select their
// existing algorithms. Local and Ref independently configure local files
// (default off) and named ref-location tarstreams (default auto).
type Config struct {
	Chunk    string      `yaml:"chunk"`           // "aes"
	Manifest string      `yaml:"manifest"`        // "aes"
	Local    LocalPolicy `yaml:"local,omitempty"` // off | auto | required
	Ref      LocalPolicy `yaml:"ref,omitempty"`   // off | auto | required
}

// LocalPolicy validates and resolves the local-storage policy. The zero value
// deliberately means off so existing configurations do not begin reading key
// material or encrypting local files after an upgrade.
func (c Config) LocalPolicy() (LocalPolicy, error) {
	return parsePolicy("local", c.Local, LocalOff)
}

// RefPolicy resolves the policy for tarstreams at named ref locations.
// Auto allows existing plaintext and encrypts new output with the customer key.
func (c Config) RefPolicy() (LocalPolicy, error) {
	return parsePolicy("ref", c.Ref, LocalAuto)
}

func parsePolicy(field string, value, fallback LocalPolicy) (LocalPolicy, error) {
	switch value {
	case "":
		return fallback, nil
	case LocalOff, LocalAuto, LocalRequired:
		return value, nil
	default:
		return "", fmt.Errorf("crypto: %s: unsupported policy %q (want off, auto, or required)", field, value)
	}
}
