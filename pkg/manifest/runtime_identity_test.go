package manifest

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotRuntimeReference(t *testing.T) {
	key := strings.Repeat("a", 64)
	runtime := RuntimeIdentity{Basename: "runtime:v1.bundle", Digest: strings.Repeat("b", 64)}
	for _, base := range []string{
		"manifest://" + key,
		"file:///tmp/snapshot.bundle",
		"file://snapshot.tarstream@digest:" + key,
		"file://snapshot.tarstream@hmac:" + key + "@location:node.1",
		"file://snapshot.bundle@manifest:" + key + "@location:node.1",
	} {
		t.Run(base, func(t *testing.T) {
			legacy, err := ParseRef(base)
			if err != nil {
				t.Fatal(err)
			}
			raw := base + "@runtime:" + runtime.Basename + ":digest:" + runtime.Digest
			got, err := ParseRef(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got.Runtime != runtime || got.String() != raw || got.Base() != legacy || got.Portable() != legacy.Portable() {
				t.Fatalf("runtime declaration changed base identity: %#v (legacy %#v)", got, legacy)
			}
			if got.Scheme == RefSchemeManifest {
				before, _ := ParseKeyRef(base)
				after, err := ParseKeyRef(raw)
				if err != nil || before != after {
					t.Fatalf("content lookup differs: %x, %v", after, err)
				}
			} else if got.Location != "" {
				path, err := (RefLocations{"node.1": "/artifacts"}).ResolveFile(got, "")
				if err != nil || path != filepath.Join("/artifacts", got.Path) {
					t.Fatalf("ResolveFile = %q, %v", path, err)
				}
			}
		})
	}
}

func TestRuntimeAnnotationRejectsMalformed(t *testing.T) {
	digest := strings.Repeat("a", 64)
	valid := "@runtime:runtime.bundle:digest:" + digest
	for _, suffix := range []string{
		"@runtime", "@runtime:", "@runtime=runtime.bundle", "@runtime::digest:",
		"@runtime:runtime.bundle", "@runtime:runtime.bundle:digest:", "@runtime::digest:" + digest,
		"@runtime:runtime.tar:digest:" + digest, "@runtime:.bundle:digest:" + digest,
		"@runtime:bad\n.bundle:digest:" + digest, "@runtime:bad\x7f.bundle:digest:" + digest,
		"@runtime:bad\xff.bundle:digest:" + digest,
		"@runtime:/runtime.bundle:digest:" + digest, "@runtime:dir/runtime.bundle:digest:" + digest,
		"@runtime:dir\\runtime.bundle:digest:" + digest, "@runtime:..:digest:" + digest,
		"@runtime:.:digest:" + digest, "@runtime:bad\x00.bundle:digest:" + digest,
		"@runtime:" + strings.Repeat("x", MaxRuntimeBasenameBytes+1) + ":digest:" + digest,
		"@runtime:runtime.bundle:digest:" + strings.Repeat("A", 64),
		"@runtime:runtime.bundle:digest:" + strings.Repeat("z", 64),
		"@runtime:runtime.bundle:sha256:" + digest,
		"@runtime:runtime.bundle@location:x:digest:" + digest,
		"@runtime:runtime.bundle@digest:" + digest + ":digest:" + digest,
		valid + valid, valid + "@location:x", valid + "@manifest:" + digest,
	} {
		for _, base := range []string{"manifest://" + digest, "file://snapshot.bundle"} {
			t.Run(base+suffix, func(t *testing.T) {
				if ref, err := ParseRef(base + suffix); err == nil {
					t.Fatalf("accepted malformed runtime declaration: %#v", ref)
				}
			})
		}
	}
}

func TestRuntimeAnnotationLengthBoundary(t *testing.T) {
	base := "manifest://" + strings.Repeat("a", 64)
	for _, name := range []string{strings.Repeat("x", MaxRuntimeBasenameBytes-len(".bundle")) + ".bundle", strings.Repeat("界", 82) + "xx.bundle"} {
		raw := base + "@runtime:" + name + ":digest:" + strings.Repeat("b", 64)
		ref, err := ParseRef(raw)
		if err != nil || ref.String() != raw || len(raw)-len(base) != MaxRuntimeAnnotationBytes {
			t.Fatalf("maximum annotation failed: %#v, %v", ref, err)
		}
	}
}

func TestRuntimeIdentityOptionalButNotPartial(t *testing.T) {
	for _, runtime := range []RuntimeIdentity{
		{Basename: "runtime.bundle"}, {Digest: strings.Repeat("a", 64)},
	} {
		if err := runtime.Validate(); err == nil {
			t.Fatalf("accepted partial declaration: %#v", runtime)
		}
	}
	if err := (RuntimeIdentity{}).Validate(); err != nil {
		t.Fatalf("legacy declaration: %v", err)
	}
}

func FuzzSnapshotRuntimeReference(f *testing.F) {
	f.Add("manifest://" + strings.Repeat("a", 64))
	f.Add("file://snapshot.bundle@manifest:" + strings.Repeat("a", 64) + "@location:x@runtime:runtime.bundle:digest:" + strings.Repeat("b", 64))
	f.Add("file://snapshot.bundle@runtime::digest:")
	f.Fuzz(func(t *testing.T, raw string) {
		ref, err := ParseRef(raw)
		if err != nil {
			return
		}
		roundtrip, err := ParseRef(ref.String())
		if err != nil || roundtrip != ref {
			t.Fatalf("round trip %#v: %#v, %v", ref, roundtrip, err)
		}
		base, err := ParseRef(ref.Base().String())
		if err != nil || base != ref.Base() || !base.Runtime.IsZero() {
			t.Fatalf("base identity %#v: %#v, %v", ref, base, err)
		}
	})
}
