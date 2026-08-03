package manifest

import (
	"strings"
	"testing"
)

func TestParseRef(t *testing.T) {
	digest := strings.Repeat("a", 64)
	tests := []struct {
		name     string
		raw      string
		want     Ref
		portable bool
	}{
		{
			name:     "manifest",
			raw:      "manifest://" + digest,
			want:     Ref{Scheme: RefSchemeManifest, Path: digest},
			portable: true,
		},
		{
			name: "local file",
			raw:  "file:///var/lib/sandbox/root.snapshot",
			want: Ref{Scheme: RefSchemeFile, Path: "/var/lib/sandbox/root.snapshot"},
		},
		{
			name:     "located file",
			raw:      "file://root.snapshot@location:0198f7a1-1234",
			want:     Ref{Scheme: RefSchemeFile, Path: "root.snapshot", Location: "0198f7a1-1234"},
			portable: true,
		},
		{
			name:     "digested located file",
			raw:      "file://base.image@sha256:" + digest + "@location:build.1",
			want:     Ref{Scheme: RefSchemeFile, Path: "base.image", DigestScheme: "sha256", Digest: digest, Location: "build.1"},
			portable: true,
		},
		{
			name:     "HMAC digested located file",
			raw:      "file://base.image@hmac:" + digest + "@location:build.1",
			want:     Ref{Scheme: RefSchemeFile, Path: "base.image", DigestScheme: "hmac", Digest: digest, Location: "build.1"},
			portable: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRef(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("ParseRef() = %#v, want %#v", got, tt.want)
			}
			if got.String() != tt.raw {
				t.Fatalf("String() = %q, want %q", got.String(), tt.raw)
			}
			if got.Portable() != tt.portable {
				t.Fatalf("Portable() = %v, want %v", got.Portable(), tt.portable)
			}
		})
	}
}

func TestParseRefRejectsInvalid(t *testing.T) {
	digest := strings.Repeat("a", 64)
	for _, raw := range []string{
		"",
		"http://example.com",
		"manifest://" + digest + ":" + digest,
		"manifest://" + digest + "@location:x",
		"file://",
		"file://../root.snapshot@location:x",
		"file:///root.snapshot@location:x",
		"file://dir/root.snapshot@location:x",
		"file://root.snapshot@location:.bad",
		"file://root.snapshot@location:x@location:y",
		"file://root.snapshot@location:",
		"file://base.image@sha256:",
		"file://base.image@hmac:",
		"file://base.image@location:x@sha256:" + digest,
		"file://base.image@location:x@hmac:" + digest,
		"file://base.image@sha256:" + digest + "@hmac:" + digest,
		"file://base.image@hmac:" + digest + "@hmac:" + digest,
		"file://base.image@sha256:" + strings.Repeat("A", 64),
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseRef(raw); err == nil {
				t.Fatalf("ParseRef(%q) succeeded", raw)
			}
		})
	}
}

func TestRefValidateDigestFieldsTogether(t *testing.T) {
	base := Ref{Scheme: RefSchemeFile, Path: "base.image"}
	for _, ref := range []Ref{
		{Scheme: base.Scheme, Path: base.Path, Digest: strings.Repeat("a", 64)},
		{Scheme: base.Scheme, Path: base.Path, DigestScheme: "sha256"},
		{Scheme: base.Scheme, Path: base.Path, DigestScheme: "unknown", Digest: strings.Repeat("a", 64)},
	} {
		if err := ref.Validate(); err == nil {
			t.Fatalf("Validate(%#v) succeeded", ref)
		}
	}
}
