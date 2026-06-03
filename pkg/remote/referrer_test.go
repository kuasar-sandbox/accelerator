package remote

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/v1/random"
)

func TestOwnerValueDeterministicAndKeyed(t *testing.T) {
	o := Owner{Desc: "acme-prod", Key: "acme-prod"}
	k1 := bytes.Repeat([]byte{1}, 32)
	k2 := bytes.Repeat([]byte{2}, 32)

	a := o.ownerValue(k1)
	if a != o.ownerValue(k1) {
		t.Fatal("ownerValue not deterministic for the same key")
	}
	if a == o.ownerValue(k2) {
		t.Fatal("ownerValue must differ for different customer keys")
	}
	if !strings.HasSuffix(a, " acme-prod") {
		t.Fatalf("owner %q must end with the public desc", a)
	}
	// The HMAC token is the first space-separated field, hex-encoded SHA-256.
	if first := strings.Fields(a)[0]; len(first) != 64 {
		t.Fatalf("hmac token %q is not 64 hex chars", first)
	}
}

func TestIsExpired(t *testing.T) {
	now := time.Now()
	imp := now.Add(-2 * time.Hour).Format(time.RFC3339)
	if isExpired("", now) {
		t.Error("empty valid_at must not be expired")
	}
	if isExpired(imp, now) {
		t.Error("import-only valid_at must not be expired")
	}
	if !isExpired(imp+" "+now.Add(-time.Hour).Format(time.RFC3339), now) {
		t.Error("past expiry must be expired")
	}
	if isExpired(imp+" "+now.Add(time.Hour).Format(time.RFC3339), now) {
		t.Error("future expiry must not be expired")
	}
}

// TestReferrerRoundTrip — write a flatten-manifest referrer to the subject's
// repo, then find it back by owner; a different customer key must not match.
func TestReferrerRoundTrip(t *testing.T) {
	host := startRegistry(t)
	img, err := random.Image(512, 2)
	if err != nil {
		t.Fatal(err)
	}
	ref := host + "/test/base:v1"
	pushImage(t, ref, img)

	cfg := &Config{
		Insecure: true,
		Cache:    CacheConfig{Dir: t.TempDir()},
		Referer: RefererConfig{
			ArtifactType: "application/vnd.acme.flatten-manifest.v1+json",
			Desc:         "acme-prod",
			Key:          "acme-prod",
		},
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := cfg.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	ck := bytes.Repeat([]byte{7}, 32)
	const manifestID = "1111111111111111111111111111111111111111111111111111111111111111"

	if _, ok, err := cfg.FindReferrer(ctx, res, ck); err != nil || ok {
		t.Fatalf("expected initial miss, got ok=%v err=%v", ok, err)
	}
	if err := cfg.PutReferrer(ctx, res, manifestID, ck); err != nil {
		t.Fatalf("PutReferrer: %v", err)
	}
	id, ok, err := cfg.FindReferrer(ctx, res, ck)
	if err != nil {
		t.Fatalf("FindReferrer: %v", err)
	}
	if !ok || id != manifestID {
		t.Fatalf("expected hit id=%s, got ok=%v id=%q", manifestID, ok, id)
	}
	// A different customer key yields a different owner → no match.
	if _, ok, _ := cfg.FindReferrer(ctx, res, bytes.Repeat([]byte{9}, 32)); ok {
		t.Fatal("a different customer key must not match the owner")
	}
}
