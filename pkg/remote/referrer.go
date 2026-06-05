package remote

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	ggcrremote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// Annotation keys on the flatten-manifest referrer (the vnd.kuasar.* namespace,
// matching RefererArtifactType). owner ties the stored id to its owner via an
// HMAC; id is the accelerator manifest content key; valid_at records the import
// time and an optional expiry.
const (
	AnnOwner   = "vnd.kuasar.flatten-manifest.owner"
	AnnID      = "vnd.kuasar.flatten-manifest.id"
	AnnValidAt = "vnd.kuasar.flatten-manifest.valid_at"
)

// Owner identifies a flatten-manifest referrer's owner.
type Owner struct {
	ArtifactType string
	Desc         string
	Key          string // HMAC message; defaults to Desc
}

func (c *Config) refererOwner() Owner {
	return Owner{
		ArtifactType: RefererArtifactType,
		Desc:         c.Referer.Desc,
		Key:          c.Referer.Key,
	}
}

// ownerValue computes the owner annotation: "<hmac-hex> <desc>", where
// hmac = HMAC-SHA256(key=customerKey, msg=Owner.Key). The token is constant
// per (customer key, referer key) pair — computable before export, scoped to
// the tenant, and unforgeable without the secret customer key.
func (o Owner) ownerValue(customerKey []byte) string {
	mac := hmac.New(sha256.New, customerKey)
	mac.Write([]byte(o.Key))
	return hex.EncodeToString(mac.Sum(nil)) + " " + o.Desc
}

// FindReferrer looks for a flatten-manifest referrer on the subject image
// whose owner matches this owner (HMAC over customerKey) and whose valid_at
// has not expired. On a match it returns the stored manifest id and ok=true,
// letting the caller skip pull + flatten + upload entirely. No matching
// referrer yields ok=false, nil. Lookup/transport failures are returned so the
// caller can decide (typically warn and fall back to a full export).
func (c *Config) FindReferrer(ctx context.Context, subj *Resolved, customerKey []byte) (id string, ok bool, err error) {
	o := c.refererOwner()
	if o.ArtifactType == "" {
		return "", false, errors.New("remote: referrer artifact type is empty")
	}
	want := o.ownerValue(customerKey)
	idx, err := ggcrremote.Referrers(subj.Digest, c.remoteOpts(ctx)...)
	if err != nil {
		return "", false, fmt.Errorf("remote: list referrers for %s: %w", subj.Digest, err)
	}
	im, err := idx.IndexManifest()
	if err != nil {
		return "", false, fmt.Errorf("remote: read referrers index: %w", err)
	}
	now := time.Now()
	for _, d := range im.Manifests {
		anns := d.Annotations
		if anns[AnnOwner] == "" {
			// The descriptor carries no annotations — e.g. ggcr's tag-schema
			// fallback (for registries without the Referrers API) records only
			// artifactType. Fetch the manifest to read its annotations, but
			// only for our artifact type so we don't pull unrelated referrers
			// (SBOMs, signatures, ...). A compliant Referrers API includes the
			// annotations on the descriptor, taking the fast path here.
			if d.ArtifactType != "" && d.ArtifactType != o.ArtifactType {
				continue
			}
			anns, err = c.manifestAnnotations(ctx, subj.Repo, d.Digest)
			if err != nil {
				continue // unreadable / not an image manifest — skip
			}
		}
		if anns[AnnOwner] != want || isExpired(anns[AnnValidAt], now) {
			continue
		}
		if mid := anns[AnnID]; mid != "" {
			return mid, true, nil
		}
	}
	return "", false, nil
}

// manifestAnnotations fetches a referrer manifest and returns its annotations.
// Used when the referrers index descriptor omits them (the tag-schema
// fallback path).
func (c *Config) manifestAnnotations(ctx context.Context, repo name.Repository, d v1.Hash) (map[string]string, error) {
	img, err := ggcrremote.Image(repo.Digest(d.String()), c.remoteOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	m, err := img.Manifest()
	if err != nil {
		return nil, err
	}
	return m.Annotations, nil
}

// PutReferrer writes a flatten-manifest referrer on the subject image: an OCI
// artifact whose config media type carries the artifactType (so the registry
// reports that as the referrer's artifactType, per the OCI fallback rule), a
// subject pointing at the subject image, and the owner/id/valid_at
// annotations. Requires push access to the subject repository — OCI referrers
// live in the subject's repo.
func (c *Config) PutReferrer(ctx context.Context, subj *Resolved, id string, customerKey []byte) error {
	o := c.refererOwner()
	if o.ArtifactType == "" {
		return errors.New("remote: referrer artifact type is empty")
	}

	validAt, err := c.validAtValue()
	if err != nil {
		return err
	}
	anns := map[string]string{
		AnnOwner:   o.ownerValue(customerKey),
		AnnID:      id,
		AnnValidAt: validAt,
	}

	// Subject descriptor = the platform-selected image manifest.
	sd, err := ggcrremote.Get(subj.Digest, c.remoteOpts(ctx)...)
	if err != nil {
		return fmt.Errorf("remote: subject descriptor %s: %w", subj.Digest, err)
	}

	// No mutate.ArtifactType in this ggcr version: carry the artifactType as
	// the config media type. Registries surface a referrer's config media
	// type as its artifactType when the artifactType field is absent, so the
	// WithFilter("artifactType", ...) lookup in FindReferrer still matches.
	art := mutate.ConfigMediaType(empty.Image, types.MediaType(o.ArtifactType))
	// Force an OCI image manifest. empty.Image serialises as a Docker schema2
	// manifest by default, which has no `subject` field semantics — a compliant
	// OCI 1.1 registry (e.g. zot) then ignores the subject and never indexes the
	// referrer (the in-memory test registry's tag-schema fallback hides this).
	// Only an OCI image manifest carries a subject the Referrers API honours.
	art = mutate.MediaType(art, types.OCIManifestSchema1)
	annotated := mutate.Annotations(art, anns)
	withSubject := mutate.Subject(annotated, sd.Descriptor)
	img, ok := withSubject.(v1.Image)
	if !ok {
		return errors.New("remote: referrer artifact is not an image")
	}

	d, err := img.Digest()
	if err != nil {
		return fmt.Errorf("remote: referrer digest: %w", err)
	}
	ref := subj.Repo.Digest(d.String())
	if err := ggcrremote.Write(ref, img, c.remoteOpts(ctx)...); err != nil {
		return fmt.Errorf("remote: write referrer (needs push access to %s): %w", subj.Repo, err)
	}
	return nil
}

// validAtValue builds the valid_at annotation: "<import RFC3339>" plus, when
// referer.validity is set, " <expiry RFC3339>".
func (c *Config) validAtValue() (string, error) {
	now := time.Now().UTC()
	v := now.Format(time.RFC3339)
	if c.Referer.Validity == "" {
		return v, nil
	}
	dur, err := time.ParseDuration(c.Referer.Validity)
	if err != nil {
		return "", fmt.Errorf("remote: referer.validity: %w", err)
	}
	return v + " " + now.Add(dur).Format(time.RFC3339), nil
}

// isExpired reports whether a valid_at annotation ("<import>[ <expiry>]") has
// an expiry in the past. No expiry (or an unparseable one) → not expired.
func isExpired(validAt string, now time.Time) bool {
	fields := strings.Fields(validAt)
	if len(fields) < 2 {
		return false // import-only, no expiry
	}
	exp, err := time.Parse(time.RFC3339, fields[1])
	if err != nil {
		return false
	}
	return now.After(exp)
}
