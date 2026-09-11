// Package sdkclient wraps aws-sdk-go-v2/s3 in the small s3.s3Client
// interface, normalising AWS SDK error types into the sentinel
// errors the parent package expects (s3.ErrNotFound /
// s3.ErrPreconditionFailed) and applying the per-call defenses
// (no-trust-SDK-timeout, body close, content-length verification).
//
// Living in a sub-package keeps the AWS SDK out of pkg/store/s3's
// dependency tree — that package's unit tests stay SDK-free.
package sdkclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/kuasar-sandbox/accelerator/pkg/store/s3"
)

// Config is the dialing-side configuration. AccessKey / SecretKey
// pass through to a static credentials provider; leaving them empty
// makes the AWS SDK fall through to its default credentials chain
// (env vars, ECS metadata, etc.) which is the right behaviour when
// the host is a cloud instance with an instance role attached.
type Config struct {
	// Endpoint is the S3-compatible endpoint URL. Required.
	Endpoint string

	// Region is the signing region. Empty defaults to us-east-1.
	Region string

	// Bucket is the target bucket. Captured by Client to produce
	// per-call S3 inputs; s3.Store talks in keys, this layer
	// fills in the bucket.
	Bucket string

	// PathStyle selects endpoint/bucket/key addressing. Callers must
	// pass the already resolved user-facing configuration value.
	PathStyle bool

	// AccessKey / SecretKey: static AK/SK. Empty → default chain.
	AccessKey string
	SecretKey string

	// TLS tunes certificate verification for the endpoint: an extra
	// CA bundle to trust, or opting out of verification (testing
	// only). Zero value → SDK defaults (system trust store, strict).
	TLS TLSConfig
}

// TLSConfig mirrors the tls: config block used by pkg/remote and the
// orchestrator build registry: an extra CA bundle appended to the
// system trust store, or a full opt-out of verification.
type TLSConfig struct {
	// CACert is a path to a PEM CA bundle (may hold several
	// certificates) appended to the system trust store — the way to
	// trust an endpoint (or intercepting proxy) whose CA is not in
	// the system store.
	CACert string

	// InsecureSkipVerify disables certificate verification entirely.
	// Insecure — traffic including credentials can be intercepted;
	// testing only. Mutually exclusive with CACert.
	InsecureSkipVerify bool
}

// Client is the s3Client implementation. Held in the parent package
// via the s3.s3Client interface; no public methods other than the
// interface contract.
type Client struct {
	api    *awss3.Client
	bucket string
}

// New validates the required location and static-credential pairing,
// then returns a Client. The SDK surfaces malformed endpoint or signing
// configuration when the first request is sent.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("s3 sdkclient: endpoint is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("s3 sdkclient: bucket is required")
	}
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, errors.New("s3 sdkclient: access key and secret key must be set together")
	}
	if cfg.TLS.CACert != "" && cfg.TLS.InsecureSkipVerify {
		return nil, errors.New("s3 sdkclient: tls ca_cert and insecure_skip_verify are mutually exclusive")
	}
	tlsClient, err := tlsHTTPClient(cfg.TLS)
	if err != nil {
		return nil, err
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if cfg.AccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3 sdkclient: load aws config: %w", err)
	}
	if tlsClient != nil {
		// Attached only after LoadDefaultConfig resolves the credential
		// chain: the default chain's identity clients (STS assume-role /
		// web identity, SSO OIDC) capture cfg.HTTPClient at construction,
		// so passing a tuned client as a load option would apply the
		// tuning to their token fetches too. The tuning is scoped to
		// this S3 client's object traffic only.
		awsCfg.HTTPClient = tlsClient
	}
	api := newAPI(awsCfg, cfg.Endpoint, cfg.PathStyle)
	return &Client{api: api, bucket: cfg.Bucket}, nil
}

// tlsHTTPClient builds the HTTP client for an endpoint whose TLS
// verification is tuned. It starts from the SDK's own buildable client
// so pooling, dialer and timeout defaults are preserved, and keeps the
// SDK transport's TLS 1.2 floor. A fresh client per call — tuned and
// default endpoints never share a connection pool. Returns nil when no
// tuning is configured (the SDK default, strict, applies).
func tlsHTTPClient(cfg TLSConfig) (*awshttp.BuildableClient, error) {
	if cfg.CACert == "" && !cfg.InsecureSkipVerify {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CACert != "" {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("s3 sdkclient: read tls ca_cert %q: %w", cfg.CACert, err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("s3 sdkclient: tls ca_cert %q: no PEM certificates found", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	} else {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // opt-in via config
	}
	return awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.TLSClientConfig = tlsCfg
	}), nil
}

func newAPI(awsCfg aws.Config, endpoint string, pathStyle bool) *awss3.Client {
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)

		o.UsePathStyle = pathStyle

		// Prefer the broadly supported S3 request format. Some
		// S3-compatible services do not implement AWS flexible checksum
		// trailers or aws-chunked request encoding.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired

		o.DisableLogOutputChecksumValidationSkipped = true
	})
}

// Head implements s3.s3Client.
func (c *Client) Head(ctx context.Context, key string) (*s3.ObjectMeta, error) {
	out, err := c.api.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, s3.ErrNotFound
		}
		return nil, normaliseErr("head", key, err)
	}
	return &s3.ObjectMeta{
		Size: aws.ToInt64(out.ContentLength),
		ETag: aws.ToString(out.ETag),
	}, nil
}

// Get implements s3.s3Client. The body is fully read into memory
// — store-ctl's chunk/manifest workload caps single-object size at
// the chunker's max-chunk setting (typically ≤1 MiB), so a one-
// shot ReadAll is the right tradeoff vs streaming complexity.
//
// Defensive close: the response Body is *always* Closed before
// return, including on read failure, to avoid leaking a connection
// if the SDK's own cleanup path has a bug.
func (c *Client) Get(ctx context.Context, key string) ([]byte, *s3.ObjectMeta, error) {
	return c.get(ctx, key, nil)
}

// GetLimited is Get with a hard response-body limit. It is used for small
// metadata objects whose size must be bounded before they are materialised in
// memory. A missing or dishonest Content-Length is still bounded while read.
func (c *Client) GetLimited(ctx context.Context, key string, maxBytes int64) ([]byte, *s3.ObjectMeta, error) {
	if maxBytes < 0 {
		return nil, nil, fmt.Errorf("s3 sdkclient: negative body limit %d", maxBytes)
	}
	return c.get(ctx, key, &maxBytes)
}

func (c *Client) get(ctx context.Context, key string, maxBytes *int64) ([]byte, *s3.ObjectMeta, error) {
	out, err := c.api.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, nil, s3.ErrNotFound
		}
		return nil, nil, normaliseErr("get", key, err)
	}
	defer out.Body.Close()

	size := aws.ToInt64(out.ContentLength)
	if maxBytes != nil && out.ContentLength != nil && size > *maxBytes {
		return nil, nil, fmt.Errorf("s3 sdkclient: body %s exceeds limit %d (header=%d)",
			key, *maxBytes, size)
	}

	reader := io.Reader(out.Body)
	if maxBytes != nil && *maxBytes < math.MaxInt64 {
		reader = io.LimitReader(out.Body, *maxBytes+1)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("s3 sdkclient: read body %s: %w", key, err)
	}
	if maxBytes != nil && int64(len(body)) > *maxBytes {
		return nil, nil, fmt.Errorf("s3 sdkclient: body %s exceeds limit %d", key, *maxBytes)
	}
	if out.ContentLength != nil && int64(len(body)) != size {
		return nil, nil, fmt.Errorf("s3 sdkclient: body size mismatch (header=%d, read=%d)",
			size, len(body))
	}
	return body, &s3.ObjectMeta{
		Size: int64(len(body)),
		ETag: aws.ToString(out.ETag),
	}, nil
}

// Put implements s3.s3Client.
func (c *Client) Put(ctx context.Context, key string, body []byte, opts s3.PutOptions) (string, error) {
	in := &awss3.PutObjectInput{
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
			return "", s3.ErrPreconditionFailed
		}
		return "", normaliseErr("put", key, err)
	}
	return aws.ToString(out.ETag), nil
}

// List implements s3.s3Client. Pages transparently via the SDK's
// continuation-token mechanism so callers see one flat key stream.
// visit returning false aborts the scan at the next page boundary
// (we don't issue any S3-level cancel — paginator just stops).
func (c *Client) List(ctx context.Context, prefix string, visit func(key string) bool) error {
	paginator := awss3.NewListObjectsV2Paginator(c.api, &awss3.ListObjectsV2Input{
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

// Delete implements s3.s3Client. S3 DeleteObject is idempotent —
// 200/204 OK whether the key existed or not — so we don't translate
// "absent" to a sentinel; only transport errors propagate.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.api.DeleteObject(ctx, &awss3.DeleteObjectInput{
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
	return fmt.Errorf("s3 sdkclient: %s %s: %w", op, key, err)
}
