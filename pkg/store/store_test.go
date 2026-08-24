package store

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func TestValidateGenerations(t *testing.T) {
	valid := []Generation{"G1", "release.2026-08_19", "z"}
	if err := ValidateGenerations(valid); err != nil {
		t.Fatalf("valid list: %v", err)
	}
	for _, test := range []struct {
		name string
		list []Generation
	}{
		{name: "empty"},
		{name: "duplicate", list: []Generation{"G1", "G1"}},
		{name: "dot", list: []Generation{"."}},
		{name: "dot dot", list: []Generation{".."}},
		{name: "slash", list: []Generation{"a/b"}},
		{name: "backslash", list: []Generation{`a\b`}},
		{name: "control", list: []Generation{"a\nb"}},
		{name: "leading punctuation", list: []Generation{"-G1"}},
		{name: "too long", list: []Generation{Generation(strings.Repeat("a", 129))}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateGenerations(test.list); err == nil {
				t.Fatal("invalid list accepted")
			}
		})
	}
	tooMany := make([]Generation, maxGenerations+1)
	for i := range tooMany {
		tooMany[i] = Generation(fmt.Sprintf("G%d", i))
	}
	if err := ValidateGenerations(tooMany); err == nil {
		t.Fatal("oversized list accepted")
	}
}

func TestSaltForGeneration(t *testing.T) {
	want := sha256.Sum256([]byte("accelerator-salt-v1G1"))
	got, err := SaltForGeneration("G1")
	if err != nil {
		t.Fatalf("SaltForGeneration: %v", err)
	}
	if got != want {
		t.Fatalf("SaltForGeneration = %x, want %x", got, want)
	}
	if other, err := SaltForGeneration("G2"); err != nil || other == got {
		t.Fatalf("different generation salt = %x, %v", other, err)
	}
	if _, err := SaltForGeneration("bad/generation"); err == nil {
		t.Fatal("unsafe generation accepted")
	}
}
