package plugin_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/secret"
	"github.com/kuasar-sandbox/accelerator/pkg/secret/plugin"
)

func resolveRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/secrets/resolve", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func keyRequest(t *testing.T, key string) *http.Request {
	t.Helper()
	body, err := json.Marshal(map[string]string{"key": key})
	if err != nil {
		t.Fatal(err)
	}
	return resolveRequest(string(body))
}

func responseValue(t *testing.T, response *httptest.ResponseRecorder, key string) []byte {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d", response.Code)
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatal("missing JSON content type")
	}
	var result struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Key != key || !strings.HasPrefix(result.Value, "base64:") {
		t.Fatal("invalid key echo or value prefix")
	}
	encoded := strings.TrimPrefix(result.Value, "base64:")
	value, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(value) != encoded {
		t.Fatal("value is not canonical padded Base64")
	}
	return value
}

func TestResolveOpaqueKeyAndBytes(t *testing.T) {
	const key = "Tenant/a_B/%2F?value#fragment/密钥"
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "request-context")
	want := []byte{0, 255, 1, 17, 0, 9, 254}
	var calls int
	handler := plugin.NewHandler(func(gotCtx context.Context, gotKey string) ([]byte, error) {
		calls++
		if gotCtx != ctx || gotCtx.Value(contextKey{}) != "request-context" {
			t.Error("resolver did not receive the request context")
		}
		if gotKey != key {
			t.Error("opaque key was changed")
		}
		return want, nil
	})
	r := keyRequest(t, key).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	if got := responseValue(t, response, key); !bytes.Equal(got, want) {
		t.Fatal("resolved bytes changed")
	}
	if calls != 1 {
		t.Fatalf("resolver calls = %d", calls)
	}
}

func TestEmptyValueRemainsConsumerDecision(t *testing.T) {
	handler := plugin.NewHandler(func(context.Context, string) ([]byte, error) { return nil, nil })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, keyRequest(t, "empty"))
	if value := responseValue(t, response, "empty"); len(value) != 0 {
		t.Fatal("empty result changed")
	}
}

func TestInvalidRequestsDoNotCallResolver(t *testing.T) {
	const valid = `{"key":"DO_NOT_ECHO"}`
	for _, tc := range []struct {
		name, method, target, contentType, body string
		status                                  int
	}{
		{"path", "POST", "/v1/secrets/derive", "application/json", valid, 404},
		{"method", "GET", "/v1/secrets/resolve", "application/json", valid, 405},
		{"query", "POST", "/v1/secrets/resolve?resolve=true", "application/json", valid, 404},
		{"empty-query", "POST", "/v1/secrets/resolve?", "application/json", valid, 404},
		{"missing-content-type", "POST", "/v1/secrets/resolve", "", valid, 415},
		{"wrong-content-type", "POST", "/v1/secrets/resolve", "text/plain", valid, 415},
		{"malformed-json", "POST", "/v1/secrets/resolve", "application/json", `{"key":`, 400},
		{"second-json-object", "POST", "/v1/secrets/resolve", "application/json", valid + `{}`, 400},
		{"trailing-data", "POST", "/v1/secrets/resolve", "application/json", valid + ` trailing`, 400},
		{"unknown-field", "POST", "/v1/secrets/resolve", "application/json", `{"key":"DO_NOT_ECHO","salt":"private"}`, 400},
		{"duplicate-key", "POST", "/v1/secrets/resolve", "application/json", `{"key":"first","key":"DO_NOT_ECHO"}`, 400},
		{"field-case", "POST", "/v1/secrets/resolve", "application/json", `{"Key":"DO_NOT_ECHO"}`, 400},
		{"missing-key", "POST", "/v1/secrets/resolve", "application/json", `{}`, 400},
		{"null-key", "POST", "/v1/secrets/resolve", "application/json", `{"key":null}`, 400},
		{"numeric-key", "POST", "/v1/secrets/resolve", "application/json", `{"key":42}`, 400},
		{"empty-key", "POST", "/v1/secrets/resolve", "application/json", `{"key":""}`, 400},
		{"colon-key", "POST", "/v1/secrets/resolve", "application/json", `{"key":"DO_NOT_ECHO:version"}`, 400},
		{"space-key", "POST", "/v1/secrets/resolve", "application/json", `{"key":"DO_NOT_ECHO value"}`, 400},
		{"unicode-space-key", "POST", "/v1/secrets/resolve", "application/json", `{"key":"DO_NOT_ECHO\u2003value"}`, 400},
		{"non-object", "POST", "/v1/secrets/resolve", "application/json", `[{"key":"DO_NOT_ECHO"}]`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			handler := plugin.NewHandler(func(context.Context, string) ([]byte, error) {
				calls++
				return []byte("private"), nil
			})
			r := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			if response.Code != tc.status || calls != 0 {
				t.Fatalf("HTTP status = %d, resolver calls = %d", response.Code, calls)
			}
			if strings.Contains(response.Body.String(), "DO_NOT_ECHO") || strings.Contains(response.Body.String(), "private") {
				t.Fatal("invalid request content was exposed")
			}
			if tc.status == 405 && response.Header().Get("Allow") != http.MethodPost {
				t.Fatal("missing Allow header")
			}
		})
	}
}

func TestRequestSizeBoundary(t *testing.T) {
	const limit = 1 << 20
	const body = `{"key":"key"}`
	for _, size := range []int{limit, limit + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			calls := 0
			handler := plugin.NewHandler(func(context.Context, string) ([]byte, error) {
				calls++
				return []byte{1}, nil
			})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, resolveRequest(body+strings.Repeat(" ", size-len(body))))
			if size == limit {
				if value := responseValue(t, response, "key"); !bytes.Equal(value, []byte{1}) || calls != 1 {
					t.Fatal("request at size limit was not resolved")
				}
			} else if response.Code != http.StatusRequestEntityTooLarge || calls != 0 {
				t.Fatalf("oversized request: HTTP status = %d, resolver calls = %d", response.Code, calls)
			}
		})
	}
}

func TestResolverErrorsAreSanitized(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"not-found", fs.ErrNotExist, 404},
		{"permission", fs.ErrPermission, 403},
		{"deadline", context.DeadlineExceeded, 504},
		{"canceled", context.Canceled, 500},
		{"other", errors.New("backend failure"), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := plugin.NewHandler(func(context.Context, string) ([]byte, error) {
				return []byte("PRIVATE_VALUE"), fmt.Errorf("PRIVATE_ERROR: %w", tc.err)
			})
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, keyRequest(t, "PRIVATE_KEY"))
			if response.Code != tc.status || strings.Contains(response.Body.String(), "PRIVATE_") || strings.Contains(response.Body.String(), tc.err.Error()) {
				t.Fatalf("error response: status = %d, body = %q", response.Code, response.Body.String())
			}
		})
	}
	t.Run("nil-resolver", func(t *testing.T) {
		response := httptest.NewRecorder()
		plugin.NewHandler(nil).ServeHTTP(response, keyRequest(t, "PRIVATE_KEY"))
		if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "PRIVATE_KEY") {
			t.Fatal("nil resolver did not fail safely")
		}
	})
}

func TestEncodedResponseSizeBoundary(t *testing.T) {
	const limit = 1 << 20
	const overhead = len(`{"key":"","value":"base64:"}`)
	value := bytes.Repeat([]byte{0xa5}, 3*((limit-overhead-1)/4))
	key := strings.Repeat("k", limit-overhead-base64.StdEncoding.EncodedLen(len(value)))
	for _, tc := range []struct {
		name, key string
		value     []byte
		status    int
	}{
		{"exact-limit", key, value, 200},
		{"base64-over-limit", key, append(bytes.Clone(value), 0), 500},
		{"escaped-key-over-limit", strings.Repeat("&", limit/6+1), nil, 500},
		{"raw-value-over-limit", "key", make([]byte, limit+1), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := plugin.NewHandler(func(context.Context, string) ([]byte, error) { return tc.value, nil })
			response := httptest.NewRecorder()
			request := keyRequest(t, tc.key)
			if tc.name == "escaped-key-over-limit" {
				// Literal ampersands are valid JSON. Keep the request below its
				// limit so this case measures the response encoder's expansion.
				request = resolveRequest(`{"key":"` + tc.key + `"}`)
			}
			handler.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("HTTP status = %d", response.Code)
			}
			if tc.status == 200 {
				if response.Body.Len() != limit || !bytes.Equal(responseValue(t, response, tc.key), tc.value) {
					t.Fatal("response at the encoded size limit changed")
				}
			} else if response.Body.Len() > 100 || strings.Contains(response.Body.String(), "base64:") {
				t.Fatal("oversized success response was partially written")
			}
		})
	}
}

func TestConcurrentRequestsPreserveCancellation(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	handler := plugin.NewHandler(func(ctx context.Context, key string) ([]byte, error) {
		started <- key
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return []byte(key), nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	responses := make(chan *httptest.ResponseRecorder, 2)
	first := keyRequest(t, "canceled").WithContext(ctx)
	second := keyRequest(t, "continued")
	for _, request := range []*http.Request{first, second} {
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			responses <- response
		}()
	}
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	defer releaseAll()
	for range 2 {
		select {
		case <-started:
		case <-timeout.C:
			t.Fatal("resolvers were not called concurrently")
		}
	}
	cancel()
	select {
	case response := <-responses:
		if response.Code != http.StatusInternalServerError {
			t.Fatal("canceled request did not finish with its own error")
		}
	case <-timeout.C:
		t.Fatal("request cancellation did not reach resolver")
	}
	releaseAll()
	select {
	case response := <-responses:
		if !bytes.Equal(responseValue(t, response, "continued"), []byte("continued")) {
			t.Fatal("other request was affected by cancellation")
		}
	case <-timeout.C:
		t.Fatal("independent request did not finish")
	}
}

func TestClientOverUnixSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plugin.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string][]byte{
		"tenant/manifest-v1": bytes.Repeat([]byte{0x71}, 32),
		"opaque/%2F?x#y":     {0, 255, 1, 2, 3},
		"empty":              {},
	}
	var calls atomic.Int32
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: plugin.NewHandler(func(_ context.Context, key string) ([]byte, error) {
		calls.Add(1)
		if value, ok := values[key]; ok {
			return value, nil
		}
		return nil, fmt.Errorf("PRIVATE_BACKEND_DETAIL: %w", fs.ErrNotExist)
	})}
	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := <-serveError; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("HTTP server: %v", err)
		}
	})
	client, err := secret.New("example::" + path + ":cache_ttl=0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.CloseIdleConnections)
	for key, want := range values {
		got, err := client.Resolve(context.Background(), "@example/"+key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("SDK interoperability: error = %v, byte count = %d", err, len(got))
		}
	}
	if _, err := client.Resolve(context.Background(), "@example/missing"); err == nil || !strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "PRIVATE_BACKEND_DETAIL") {
		t.Fatalf("SDK failure response = %v", err)
	}
	if got, want := calls.Load(), int32(len(values)+1); got != want {
		t.Fatalf("resolver calls = %d, want %d", got, want)
	}
}
