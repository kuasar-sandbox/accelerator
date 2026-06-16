package remote

import (
	"os"

	"github.com/google/go-containerregistry/pkg/authn"
)

// Namespaced env vars for registry credentials. A bearer token wins over
// basic auth; absence of all three falls back to anonymous (public images).
// Credentials live in env only — never on argv (ps-visible) and never in the
// remote config file. This matches the flatten data-plane model where the
// management plane injects per-task tenant pull credentials.
const (
	EnvRegistryToken    = "FLATTEN_REGISTRY_TOKEN"
	EnvRegistryUsername = "FLATTEN_REGISTRY_USERNAME"
	EnvRegistryPassword = "FLATTEN_REGISTRY_PASSWORD"
)

// authenticator resolves registry credentials from the FLATTEN_REGISTRY_*
// env vars, falling back to anonymous when none are set.
func authenticator() authn.Authenticator {
	if tok := os.Getenv(EnvRegistryToken); tok != "" {
		return &authn.Bearer{Token: tok}
	}
	if user := os.Getenv(EnvRegistryUsername); user != "" {
		return &authn.Basic{Username: user, Password: os.Getenv(EnvRegistryPassword)}
	}
	return authn.Anonymous
}
