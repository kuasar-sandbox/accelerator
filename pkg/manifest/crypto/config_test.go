package crypto

import (
	"testing"

	"gopkg.in/yaml.v3"
)

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
