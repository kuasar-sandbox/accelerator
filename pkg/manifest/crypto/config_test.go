package crypto

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestConfigRefPolicyIndependentOfLocal(t *testing.T) {
	t.Setenv("MANIFEST_KEY", "")
	for _, local := range []LocalPolicy{"", LocalOff, LocalAuto, LocalRequired} {
		for _, ref := range []LocalPolicy{"", LocalOff, LocalAuto, LocalRequired} {
			cfg := Config{Chunk: "aes", Manifest: "aes", Local: local, Ref: ref}
			wantLocal, wantRef := local, ref
			if local == "" {
				wantLocal = LocalOff
			}
			if ref == "" {
				wantRef = LocalAuto
			}
			if got, err := cfg.LocalPolicy(); err != nil || got != wantLocal {
				t.Errorf("local=%q ref=%q: LocalPolicy() = %q, %v", local, ref, got, err)
			}
			if got, err := cfg.RefPolicy(); err != nil || got != wantRef {
				t.Errorf("local=%q ref=%q: RefPolicy() = %q, %v", local, ref, got, err)
			}
			if _, _, err := New(cfg); err != nil {
				t.Errorf("New(local=%q, ref=%q): %v", local, ref, err)
			}
		}
	}
}

func TestConfigRefPolicyYAML(t *testing.T) {
	for _, value := range []LocalPolicy{LocalOff, LocalAuto, LocalRequired, "plaintext"} {
		var cfg Config
		if err := yaml.Unmarshal([]byte("chunk: aes\nmanifest: aes\nref: "+string(value)+"\n"), &cfg); err != nil {
			t.Fatal(err)
		}
		got, err := cfg.RefPolicy()
		if value == "plaintext" {
			if err == nil || !strings.Contains(err.Error(), "crypto: ref:") {
				t.Fatalf("invalid ref policy error = %v", err)
			}
			if _, _, err := New(cfg); err == nil {
				t.Fatal("New accepted an invalid ref policy")
			}
		} else if err != nil || got != value {
			t.Fatalf("ref=%q: got %q, %v", value, got, err)
		}
	}
}

func TestConfigLocalPolicy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value LocalPolicy
		want  LocalPolicy
	}{
		{name: "default", want: LocalOff},
		{name: "off", value: LocalOff, want: LocalOff},
		{name: "auto", value: LocalAuto, want: LocalAuto},
		{name: "required", value: LocalRequired, want: LocalRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (Config{Local: tc.value}).LocalPolicy()
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("LocalPolicy() = %q, want %q", got, tc.want)
			}
		})
	}

	if _, err := (Config{Local: "aes"}).LocalPolicy(); err == nil {
		t.Fatal("algorithm name accepted as a local policy")
	}
	if _, _, err := New(Config{Chunk: "aes", Manifest: "aes", Local: "aes"}); err == nil {
		t.Fatal("New accepted an invalid local policy")
	}
}

func TestConfigLocalPolicyYAML(t *testing.T) {
	for _, tc := range []struct {
		document string
		want     LocalPolicy
	}{
		{document: "chunk: aes\nmanifest: aes\n", want: LocalOff},
		{document: "local: off\n", want: LocalOff},
		{document: "local: auto\n", want: LocalAuto},
		{document: "local: required\n", want: LocalRequired},
	} {
		var config Config
		if err := yaml.Unmarshal([]byte(tc.document), &config); err != nil {
			t.Fatal(err)
		}
		if got, err := config.LocalPolicy(); err != nil || got != tc.want {
			t.Fatalf("LocalPolicy(%q) = %q, %v; want %q", tc.document, got, err, tc.want)
		}
	}
}
