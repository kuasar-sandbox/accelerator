package sdkclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
)

func setDefaultCredentialEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "default-chain-ak")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "default-chain-sk")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "environment-region")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	// The client options must override broader SDK configuration.
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_supported")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_supported")
}

func newServerClient(t *testing.T, handler http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	setDefaultCredentialEnvironment(t)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := New(context.Background(), Config{
		Endpoint:  server.URL,
		Bucket:    "test-bucket",
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client, server
}

func TestPutDoesNotUseFlexibleChecksumTrailer(t *testing.T) {
	body := []byte("ordinary non-empty S3-compatible payload")
	type observedRequest struct {
		method           string
		contentEncoding  string
		trailerHeader    string
		payloadHash      string
		authorization    string
		amzDate          string
		contentLength    int64
		transferEncoding []string
		body             []byte
		err              error
	}
	requests := make(chan observedRequest, 1)
	client, _ := newServerClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, err := io.ReadAll(r.Body)
		requests <- observedRequest{
			method:           r.Method,
			contentEncoding:  strings.Join(r.Header.Values("Content-Encoding"), ","),
			trailerHeader:    r.Header.Get("X-Amz-Trailer"),
			payloadHash:      r.Header.Get("X-Amz-Content-Sha256"),
			authorization:    r.Header.Get("Authorization"),
			amzDate:          r.Header.Get("X-Amz-Date"),
			contentLength:    r.ContentLength,
			transferEncoding: append([]string(nil), r.TransferEncoding...),
			body:             gotBody,
			err:              err,
		}
		w.Header().Set("ETag", `"wire-etag"`)
		w.WriteHeader(http.StatusOK)
	}))

	etag, err := client.Put(context.Background(), "objects/key", body, stores3.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if etag != `"wire-etag"` {
		t.Errorf("ETag=%q want %q", etag, `"wire-etag"`)
	}

	got := <-requests
	if got.err != nil {
		t.Fatalf("server read body: %v", got.err)
	}
	if got.method != http.MethodPut {
		t.Errorf("method=%q want PUT", got.method)
	}
	if strings.Contains(strings.ToLower(got.contentEncoding), "aws-chunked") {
		t.Errorf("Content-Encoding=%q contains aws-chunked", got.contentEncoding)
	}
	if got.trailerHeader != "" {
		t.Errorf("X-Amz-Trailer=%q want absent", got.trailerHeader)
	}
	if got.payloadHash == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" {
		t.Error("x-amz-content-sha256 uses flexible-checksum streaming trailer token")
	}
	if !bytes.Equal(got.body, body) {
		t.Errorf("request body=%q want %q", got.body, body)
	}
	if got.contentLength != int64(len(body)) {
		t.Errorf("Content-Length=%d want %d", got.contentLength, len(body))
	}
	if len(got.transferEncoding) != 0 {
		t.Errorf("Transfer-Encoding=%v want none", got.transferEncoding)
	}
	if !strings.HasPrefix(got.authorization, "AWS4-HMAC-SHA256 ") {
		t.Errorf("Authorization=%q does not contain SigV4", got.authorization)
	}
	if !strings.Contains(got.authorization, "Credential=default-chain-ak/") {
		t.Errorf("Authorization=%q did not use the default credential chain", got.authorization)
	}
	if !strings.Contains(got.authorization, "/us-east-1/s3/aws4_request") {
		t.Errorf("Authorization=%q did not use the default region", got.authorization)
	}
	if got.amzDate == "" {
		t.Error("x-amz-date is absent")
	}
}

func TestPutMapsPreconditionFailed(t *testing.T) {
	client, _ := newServerClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusPreconditionFailed)
		_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>`)
	}))

	_, err := client.Put(context.Background(), "key", []byte("payload"), stores3.PutOptions{IfNoneMatch: "*"})
	if !errors.Is(err, stores3.ErrPreconditionFailed) {
		t.Fatalf("error=%v want s3.ErrPreconditionFailed", err)
	}
}

func TestMissingObjectsMapToNotFound(t *testing.T) {
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			client, _ := newServerClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			}))
			var err error
			if method == http.MethodHead {
				_, err = client.Head(context.Background(), "missing")
			} else {
				_, _, err = client.Get(context.Background(), "missing")
			}
			if !errors.Is(err, stores3.ErrNotFound) {
				t.Fatalf("error=%v want s3.ErrNotFound", err)
			}
		})
	}
}

func TestListPaginates(t *testing.T) {
	var calls atomic.Int32
	client, _ := newServerClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		switch r.URL.Query().Get("continuation-token") {
		case "":
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name><Prefix>prefix/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>
  <IsTruncated>true</IsTruncated><Contents><Key>prefix/a</Key><Size>1</Size></Contents>
  <NextContinuationToken>page-2</NextContinuationToken>
</ListBucketResult>`)
		case "page-2":
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name><Prefix>prefix/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated><Contents><Key>prefix/b</Key><Size>1</Size></Contents>
</ListBucketResult>`)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))

	var keys []string
	if err := client.List(context.Background(), "prefix/", func(key string) bool {
		keys = append(keys, key)
		return true
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if got, want := strings.Join(keys, ","), "prefix/a,prefix/b"; got != want {
		t.Errorf("keys=%q want %q", got, want)
	}
	if calls.Load() != 2 {
		t.Errorf("requests=%d want 2", calls.Load())
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	var calls atomic.Int32
	client, _ := newServerClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	for i := 0; i < 2; i++ {
		if err := client.Delete(context.Background(), "already-absent"); err != nil {
			t.Fatalf("Delete #%d: %v", i+1, err)
		}
	}
	if calls.Load() != 2 {
		t.Errorf("delete requests=%d want 2", calls.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newRoundTripClient(rt http.RoundTripper) *Client {
	return newRoundTripClientWithPathStyle(rt, true)
}

func newRoundTripClientWithPathStyle(rt http.RoundTripper, pathStyle bool) *Client {
	awsCfg := aws.Config{
		Region: "us-east-1",
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider("test-ak", "test-sk", "")),
		HTTPClient: &http.Client{Transport: rt},
	}
	api := newAPI(awsCfg, "https://objects.example.com", pathStyle)
	return &Client{api: api, bucket: "test-bucket"}
}

func TestClientUsesConfiguredAddressingStyle(t *testing.T) {
	type observedRequest struct {
		method string
		host   string
		path   string
	}
	for _, tc := range []struct {
		name        string
		pathStyle   bool
		wantHost    string
		wantPutPath string
		wantGetPath string
	}{
		{
			name:        "path style",
			pathStyle:   true,
			wantHost:    "objects.example.com",
			wantPutPath: "/test-bucket/objects/key",
			wantGetPath: "/test-bucket/__meta/generations",
		},
		{
			name:        "virtual hosted style",
			pathStyle:   false,
			wantHost:    "test-bucket.objects.example.com",
			wantPutPath: "/objects/key",
			wantGetPath: "/__meta/generations",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan observedRequest, 2)
			client := newRoundTripClientWithPathStyle(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				requests <- observedRequest{
					method: req.Method,
					host:   req.URL.Host,
					path:   req.URL.EscapedPath(),
				}
				body := ""
				header := http.Header{"ETag": []string{`"addressing-etag"`}}
				if req.Method == http.MethodGet {
					body = "generation-1\n"
					header.Set("Content-Length", strconv.Itoa(len(body)))
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     header,
					Body:       io.NopCloser(strings.NewReader(body)),
					Request:    req,
				}, nil
			}), tc.pathStyle)

			if _, err := client.Put(context.Background(), "objects/key", []byte("payload"), stores3.PutOptions{}); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if _, _, err := client.Get(context.Background(), "__meta/generations"); err != nil {
				t.Fatalf("Get: %v", err)
			}

			wantPaths := map[string]string{
				http.MethodPut: tc.wantPutPath,
				http.MethodGet: tc.wantGetPath,
			}
			for range 2 {
				got := <-requests
				if got.host != tc.wantHost {
					t.Errorf("%s host=%q want %q", got.method, got.host, tc.wantHost)
				}
				if got.path != wantPaths[got.method] {
					t.Errorf("%s path=%q want %q", got.method, got.path, wantPaths[got.method])
				}
			}
		})
	}
}

type trackingBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *trackingBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestGetValidatesBodyLengthAndClosesBody(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		length  int
		wantErr string
	}{
		{name: "matching length", body: "abc", length: 3},
		{name: "mismatched length", body: "abc", length: 5, wantErr: "body size mismatch (header=5, read=3)"},
		{name: "zero length mismatch", body: "abc", length: 0, wantErr: "body size mismatch (header=0, read=3)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			responseBody := &trackingBody{Reader: strings.NewReader(tc.body)}
			client := newRoundTripClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("Content-Length", strconv.Itoa(tc.length))
				header.Set("ETag", `"get-etag"`)
				return &http.Response{
					StatusCode:    http.StatusOK,
					Status:        "200 OK",
					Proto:         "HTTP/1.1",
					ProtoMajor:    1,
					ProtoMinor:    1,
					Header:        header,
					Body:          responseBody,
					ContentLength: int64(tc.length),
					Request:       req,
				}, nil
			}))

			body, meta, err := client.Get(context.Background(), "key")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if string(body) != tc.body || meta.Size != int64(len(tc.body)) || meta.ETag != `"get-etag"` {
					t.Errorf("body=%q meta=%+v", body, meta)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error=%v want substring %q", err, tc.wantErr)
			}
			if !responseBody.closed.Load() {
				t.Error("response body was not closed")
			}
		})
	}
}

func TestGetLimitedBoundsResponseBeforeMaterialisingIt(t *testing.T) {
	for _, tc := range []struct {
		name          string
		contentLength int64
		wantRemaining int
	}{
		{name: "advertised oversized body", contentLength: 6, wantRemaining: 6},
		{name: "unknown length body", contentLength: -1, wantRemaining: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader("abcdef")
			responseBody := &trackingBody{Reader: reader}
			client := newRoundTripClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				header := make(http.Header)
				header.Set("ETag", `"limited-etag"`)
				if tc.contentLength >= 0 {
					header.Set("Content-Length", strconv.FormatInt(tc.contentLength, 10))
				}
				return &http.Response{
					StatusCode:    http.StatusOK,
					Status:        "200 OK",
					Proto:         "HTTP/1.1",
					ProtoMajor:    1,
					ProtoMinor:    1,
					Header:        header,
					Body:          responseBody,
					ContentLength: tc.contentLength,
					Request:       req,
				}, nil
			}))

			if _, _, err := client.GetLimited(context.Background(), "generation-list", 4); err == nil ||
				!strings.Contains(err.Error(), "exceeds limit 4") {
				t.Fatalf("GetLimited error = %v, want body limit error", err)
			}
			if got := reader.Len(); got != tc.wantRemaining {
				t.Fatalf("unread response bytes = %d, want %d", got, tc.wantRemaining)
			}
			if !responseBody.closed.Load() {
				t.Error("response body was not closed")
			}
		})
	}
}

func TestRequestsRespectContextCancellationAndDeadline(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		started := make(chan struct{})
		client := newRoundTripClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
			close(started)
			<-req.Context().Done()
			return nil, req.Context().Err()
		}))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := client.Head(ctx, "key")
			done <- err
		}()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v want context.Canceled", err)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		client := newRoundTripClient(roundTripFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := client.Head(ctx, "key")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error=%v want context.DeadlineExceeded", err)
		}
	})
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "missing endpoint", cfg: Config{Bucket: "b"}, want: "endpoint is required"},
		{name: "missing bucket", cfg: Config{Endpoint: "https://objects.example.com"}, want: "bucket is required"},
		{name: "access key only", cfg: Config{Endpoint: "https://objects.example.com", Bucket: "b", AccessKey: "ak"}, want: "must be set together"},
		{name: "secret key only", cfg: Config{Endpoint: "https://objects.example.com", Bucket: "b", SecretKey: "sk"}, want: "must be set together"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(context.Background(), tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

// TestInsecureConfigSkipsTLSVerification pins the TLS behaviour of the
// Insecure config: a self-signed certificate (as TLS-intercepting proxies
// present) fails strict verification but is accepted when the endpoint is
// configured insecure. The two clients coexist against the same endpoint —
// an insecure configuration never relaxes another client's verification.
func TestInsecureConfigSkipsTLSVerification(t *testing.T) {
	// One attempt only: the strict client's failure is a connection
	// error, which the retryer would otherwise back off and retry.
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	setDefaultCredentialEnvironment(t)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"tls-etag"`)
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, "G1\n")
		}
	}))
	t.Cleanup(server.Close)

	newClient := func(insecure bool) *Client {
		t.Helper()
		client, err := New(context.Background(), Config{
			Endpoint:  server.URL,
			Bucket:    "test-bucket",
			PathStyle: true,
			Insecure:  insecure,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return client
	}

	if _, _, err := newClient(false).Get(context.Background(), "__meta/generations"); err == nil {
		t.Fatal("strict client accepted a certificate from an unknown authority")
	} else if msg := err.Error(); !strings.Contains(msg, "certificate") && !strings.Contains(msg, "tls:") {
		t.Fatalf("strict client error is not a TLS verification failure: %v", err)
	}

	insecure := newClient(true)
	if _, err := insecure.Put(context.Background(), "objects/key", []byte("payload"), stores3.PutOptions{}); err != nil {
		t.Fatalf("insecure client Put: %v", err)
	}
	body, meta, err := insecure.Get(context.Background(), "objects/key")
	if err != nil {
		t.Fatalf("insecure client Get: %v", err)
	}
	if string(body) != "G1\n" || meta.ETag != `"tls-etag"` {
		t.Fatalf("insecure client read = %q, etag = %q", body, meta.ETag)
	}
}

// TestInsecureTransportDoesNotReachCredentialProviders pins the isolation
// the Insecure config promises: only object traffic to the configured S3
// endpoint skips certificate verification. The default credential chain's
// identity fetches must keep the strict transport they captured during
// LoadDefaultConfig. The web-identity chain is pointed at a self-signed
// TLS "STS" via AWS_ENDPOINT_URL_STS: with the isolation in place the
// token exchange fails certificate verification and the object Get fails
// with it; if the insecure transport leaked into the chain, the exchange
// would succeed against the mock and the Get would complete.
func TestInsecureTransportDoesNotReachCredentialProviders(t *testing.T) {
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	// Default chain with every earlier-winning provider neutralised so
	// the web-identity provider is the one that runs.
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "no-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "no-credentials"))

	tokenFile := filepath.Join(t.TempDir(), "identity-token")
	if err := os.WriteFile(tokenFile, []byte("mock-identity-token"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenFile)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/mock-role")

	// The STS mock presents a certificate no strict client trusts. If
	// the insecure transport reaches it, it hands out valid temporary
	// credentials and the object Get succeeds — the leak this test
	// guards against.
	stsMock := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = io.WriteString(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAMOCKCREDENTIALS</AccessKeyId>
      <SecretAccessKey>mock-secret</SecretAccessKey>
      <SessionToken>mock-session-token</SessionToken>
      <Expiration>2099-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
  <ResponseMetadata><RequestId>mock-request</RequestId></ResponseMetadata>
</AssumeRoleWithWebIdentityResponse>`)
	}))
	t.Cleanup(stsMock.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", stsMock.URL)

	// Plain-HTTP S3 endpoint: a completed request means credentials were
	// obtained from the mock.
	s3Endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"leak-etag"`)
		_, _ = io.WriteString(w, "G1\n")
	}))
	t.Cleanup(s3Endpoint.Close)

	client, err := New(context.Background(), Config{
		Endpoint:  s3Endpoint.URL,
		Bucket:    "test-bucket",
		PathStyle: true,
		Insecure:  true, // S3 traffic skips verification; the chain must not.
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, _, err = client.Get(context.Background(), "__meta/generations")
	if err == nil {
		t.Fatal("object Get succeeded: the insecure transport reached the credential provider's STS fetch")
	}
	if msg := err.Error(); !strings.Contains(msg, "certificate") && !strings.Contains(msg, "tls:") {
		t.Fatalf("error is not a TLS verification failure from the identity fetch: %v", err)
	}
}
