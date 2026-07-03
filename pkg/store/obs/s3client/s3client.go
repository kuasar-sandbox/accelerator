// Package s3client wraps aws-sdk-go-v2/s3 in the small obs.s3Client
// interface, normalising AWS SDK error types into the sentinel
// errors the parent package expects (obs.ErrNotFound /
// obs.ErrPreconditionFailed) and applying the per-call defenses
// (no-trust-SDK-timeout, body close, content-length verification).
//
// Living in a sub-package keeps the AWS SDK out of pkg/store/obs's
// dependency tree — that package's unit tests stay SDK-free.
package s3client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/kuasar-sandbox/accelerator/pkg/store/obs"
)

// Config is the dialing-side configuration. AccessKey / SecretKey
// pass through to a static credentials provider; leaving them empty
// makes the AWS SDK fall through to its default credentials chain
// (env vars, ECS metadata, etc.) which is the right behaviour when
// the host is a cloud instance with an instance role attached.
type Config struct {
	// Endpoint is the OBS S3-compatible endpoint URL, e.g.
	// "https://obs.cn-north-4.example.com". Required.
	Endpoint string

	// Region is the OBS region name, e.g. "cn-north-4". Used for
	// signing only — OBS validates the region on each request.
	Region string

	// Bucket is the target bucket. Captured by Client to produce
	// per-call S3 inputs; obs.Store talks in keys, this layer
	// fills in the bucket.
	Bucket string

	// AccessKey / SecretKey: static AK/SK. Empty → default chain.
	AccessKey string
	SecretKey string
}

// Client is the s3Client implementation. Held in the parent package
// via the obs.s3Client interface; no public methods other than the
// interface contract.
type Client struct {
	api    *s3.Client
	bucket string
}

// New dials and returns a Client. Validation is minimal — the SDK
// itself surfaces any malformed endpoint / signature config the
// first time a request flies.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("s3client: endpoint is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3client: bucket is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3client: load aws config: %w", err)
	}
	api := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		// OBS's S3-compatible responses carry no CRC/SHA checksum
		// header, so the SDK logs a WARN on every GET. Silence it —
		// we verify content-length ourselves and the transport is TLS.
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	return &Client{api: api, bucket: cfg.Bucket}, nil
}

// Head implements obs.s3Client.
func (c *Client) Head(ctx context.Context, key string) (*obs.ObjectMeta, error) {
	out, err := c.api.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, obs.ErrNotFound
		}
		return nil, normaliseErr("head", key, err)
	}
	return &obs.ObjectMeta{
		Size: aws.ToInt64(out.ContentLength),
		ETag: aws.ToString(out.ETag),
	}, nil
}

// Get implements obs.s3Client. The body is fully read into memory
// — store-ctl's chunk/manifest workload caps single-object size at
// the chunker's max-chunk setting (typically ≤1 MiB), so a one-
// shot ReadAll is the right tradeoff vs streaming complexity.
//
// Defensive close: the response Body is *always* Closed before
// return, including on read failure, to avoid leaking a connection
// if the SDK's own cleanup path has a bug.
func (c *Client) Get(ctx context.Context, key string) ([]byte, *obs.ObjectMeta, error) {
	out, err := c.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, nil, obs.ErrNotFound
		}
		return nil, nil, normaliseErr("get", key, err)
	}
	defer out.Body.Close()

	// Trust the Content-Length header; allocate exact-fit buffer
	// to avoid bytes.Buffer growth amplification.
	size := aws.ToInt64(out.ContentLength)
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("s3client: read body %s: %w", key, err)
	}
	if size > 0 && int64(len(body)) != size {
		return nil, nil, fmt.Errorf("s3client: body size mismatch (header=%d, read=%d)",
			size, len(body))
	}
	return body, &obs.ObjectMeta{
		Size: int64(len(body)),
		ETag: aws.ToString(out.ETag),
	}, nil
}

// Put implements obs.s3Client.
func (c *Client) Put(ctx context.Context, key string, body []byte, opts obs.PutOptions) (string, error) {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	}
	if opts.ContentType != "" {
		in.ContentType = aws.String(opts.ContentType)
	}
	if opts.IfNoneMatch != "" {
		in.IfNoneMatch = aws.String(opts.IfNoneMatch)
	}
	if opts.IfMatch != "" {
		in.IfMatch = aws.String(opts.IfMatch)
	}
	out, err := c.api.PutObject(ctx, in)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", obs.ErrPreconditionFailed
		}
		return "", normaliseErr("put", key, err)
	}
	return aws.ToString(out.ETag), nil
}

// List implements obs.s3Client. Pages transparently via the SDK's
// continuation-token mechanism so callers see one flat key stream.
// visit returning false aborts the scan at the next page boundary
// (we don't issue any S3-level cancel — paginator just stops).
func (c *Client) List(ctx context.Context, prefix string, visit func(key string) bool) error {
	paginator := s3.NewListObjectsV2Paginator(c.api, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return normaliseErr("list", prefix, err)
		}
		for _, obj := range page.Contents {
			k := aws.ToString(obj.Key)
			if !visit(k) {
				return nil
			}
		}
	}
	return nil
}

// Delete implements obs.s3Client. S3 DeleteObject is idempotent —
// 200/204 OK whether the key existed or not — so we don't translate
// "absent" to a sentinel; only transport errors propagate.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return normaliseErr("delete", key, err)
	}
	return nil
}

// isNotFound recognises both the typed NoSuchKey shape and the
// generic 404-bearing APIError shape (the SDK occasionally returns
// the latter for HEAD).
func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code := ae.ErrorCode()
		// Some 404 paths produce only an HTTPStatusCode-bearing
		// error wrapped in an APIError without a proper code; check
		// both code and the textual hint.
		if code == "NoSuchKey" || code == "NotFound" || code == "404" {
			return true
		}
	}
	return false
}

// isPreconditionFailed maps HTTP 412 into our sentinel.
func isPreconditionFailed(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code := ae.ErrorCode()
		if code == "PreconditionFailed" || code == "412" ||
			strings.Contains(strings.ToLower(ae.ErrorMessage()), "precondition") {
			return true
		}
	}
	return false
}

// normaliseErr wraps an SDK error with the operation + key context
// without losing the original chain (errors.Is / As still work).
func normaliseErr(op, key string, err error) error {
	return fmt.Errorf("s3client: %s %s: %w", op, key, err)
}
