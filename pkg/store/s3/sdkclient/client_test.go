package sdkclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

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
		Endpoint: server.URL,
		Bucket:   "test-bucket",
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
	awsCfg := aws.Config{
		Region: "us-east-1",
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider("test-ak", "test-sk", "")),
		HTTPClient: &http.Client{Transport: rt},
	}
	api := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String("https://objects.example.com")
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	return &Client{api: api, bucket: "test-bucket"}
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
