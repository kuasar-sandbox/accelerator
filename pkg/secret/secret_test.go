package secret

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func broker(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "secret-test-")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "broker.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close(); os.RemoveAll(dir) })
	return path
}

func requestKey(t *testing.T, r *http.Request) string {
	t.Helper()
	if r.Method != http.MethodPost || r.URL.Path != "/v1/secrets/resolve" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/json" {
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.Error(err)
	}
	return req.Key
}

func reply(w http.ResponseWriter, key string, value []byte) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": "base64:" + base64.StdEncoding.EncodeToString(value)})
}

func TestConfig(t *testing.T) {
	for _, config := range []string{
		"kms::relative.sock", "_kms::/run/s.sock", "kms_2::/run/s.sock", "1kms::/run/s.sock",
		"kms::/run/s.sock kms::/run/t.sock", "kms::/run/s.sock:cache_ttl=1s:cache_ttl=2s",
		"kms::/run/s.sock:unknown=1", "kms::/run/s.sock:cache_ttl=-1s", "kms::/run/s.sock:cache_ttl=bad",
		"kms::/run/s.sock:cache_size=-1", "kms::/run/s.sock:cache_size=1m", "kms::/run/s.sock:cache_size=1K",
		"kms::/run/s.sock:cache_size=18446744073709551615k", "kms::/run/s.sock:cache_size=1.5k",
		"kms::/run/s.sock:", "kms::/run/s.sock:cache_size=", "kms::/run/socket name",
	} {
		t.Run(config, func(t *testing.T) {
			if _, err := New(config); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
	c, err := New("kms::/run/a.sock enc-2::/run/b.sock:cache_size=2k:cache_ttl=5m\nKMS::/run/c.sock:cache_ttl=0")
	if err != nil {
		t.Fatal(err)
	}
	if c.plugins["kms"].size != 4096 || c.plugins["kms"].ttl != 30*time.Second || c.plugins["enc-2"].size != 2048 || c.plugins["enc-2"].ttl != 5*time.Minute || c.plugins["KMS"].ttl != 0 {
		t.Fatal("incorrect defaults or parameters")
	}
}

func TestNativeAndReferenceBoundary(t *testing.T) {
	c, _ := New("")
	b, err := c.Resolve(context.Background(), "00ff")
	if err != nil || !bytes.Equal(b, []byte{0, 255}) {
		t.Fatalf("hex: %x %v", b, err)
	}
	b, err = c.Resolve(context.Background(), "literal", Literal)
	if err != nil || string(b) != "literal" {
		t.Fatalf("literal: %q %v", b, err)
	}
	path := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(path, []byte("pem content"), 0600); err != nil {
		t.Fatal(err)
	}
	b, err = c.Resolve(context.Background(), path, os.ReadFile)
	if err != nil || string(b) != "pem content" {
		t.Fatalf("file: %q %v", b, err)
	}
	called := false
	fallback := func(string) ([]byte, error) { called = true; return []byte("fallback"), nil }
	for _, ref := range []string{"@", "@kms", "@kms/", "@/key", "@kms/a:b", "@kms/a b", "@kms/a\nb", "@bad_name/key", "@kms/opaque"} {
		if _, err := c.Resolve(context.Background(), ref, fallback); err == nil {
			t.Errorf("accepted %q", ref)
		}
	}
	if called {
		t.Fatal("plugin reference fell back to native decoder")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Resolve(ctx, "00"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestProtocolAndOpaqueKey(t *testing.T) {
	want := []byte{0, 255, 1, 17, 0, 9}
	path := broker(t, func(w http.ResponseWriter, r *http.Request) {
		key := requestKey(t, r)
		if key != "Tenant/a_B/%2F?value#fragment" {
			t.Errorf("key changed: %q", key)
		}
		reply(w, key, want)
	})
	c, err := New("kms::" + path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	value, err := c.Resolve(context.Background(), "@kms/Tenant/a_B/%2F?value#fragment")
	if err != nil || !bytes.Equal(value, want) {
		t.Fatalf("resolve: %x %v", value, err)
	}
}

func TestInvalidResponsesAreNotCachedOrExposed(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"status", "PRIVATE MATERIAL", http.StatusForbidden},
		{"json", "PRIVATE MATERIAL", 200},
		{"missing", `{"key":"test"}`, 200},
		{"echo", `{"key":"wrong","value":"base64:AA=="}`, 200},
		{"prefix", `{"key":"test","value":"00"}`, 200},
		{"encoding", `{"key":"test","value":"base64:PRIVATE MATERIAL"}`, 200},
		{"padding", `{"key":"test","value":"base64:AA"}`, 200},
		{"trailing", `{"key":"test","value":"base64:AA=="} {}`, 200},
		{"oversize", strings.Repeat("x", maxResponseSize+1), 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			path := broker(t, func(w http.ResponseWriter, r *http.Request) {
				requestKey(t, r)
				calls.Add(1)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			})
			c, err := New("kms::" + path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.CloseIdleConnections)
			for range 2 {
				if _, err := c.Resolve(context.Background(), "@kms/test"); err == nil || strings.Contains(err.Error(), "PRIVATE MATERIAL") {
					t.Fatalf("error = %v", err)
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("failed result cached: %d", calls.Load())
			}
		})
	}
}

func TestMalformedHTTPResponseDoesNotExposeMaterial(t *testing.T) {
	path := broker(t, func(w http.ResponseWriter, r *http.Request) {
		requestKey(t, r)
		conn, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = buffer.WriteString("PRIVATE MATERIAL\r\n\r\n")
		_ = buffer.Flush()
	})
	c, err := New("kms::" + path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	if _, err := c.Resolve(context.Background(), "@kms/test"); err == nil || strings.Contains(err.Error(), "PRIVATE MATERIAL") {
		t.Fatalf("unsafe transport error: %v", err)
	}
}

func TestRequestTimeout(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			path := broker(t, func(w http.ResponseWriter, r *http.Request) {
				requestKey(t, r)
				if phase == "body" {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			})
			c, err := New("kms::" + path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.CloseIdleConnections)
			c.plugins["kms"].http.Timeout = 25 * time.Millisecond
			if _, err := c.Resolve(context.Background(), "@kms/test"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("request timeout not preserved: %v", err)
			}
		})
	}
}

func TestLRUTTLIsolationAndCopies(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	path := broker(t, func(w http.ResponseWriter, r *http.Request) {
		key := requestKey(t, r)
		mu.Lock()
		calls[key]++
		mu.Unlock()
		reply(w, key, []byte(key))
	})
	c, err := New("a::" + path + ":cache_size=2:cache_ttl=10s b::" + path + ":cache_size=1:cache_ttl=10s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	var now atomic.Int64
	base := time.Now()
	for _, p := range c.plugins {
		p.now = func() time.Time { return base.Add(time.Duration(now.Load())) }
	}
	resolve := func(ref string) []byte {
		t.Helper()
		v, e := c.Resolve(context.Background(), ref)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	first := resolve("@a/one")
	first[0] = 'X'
	if string(resolve("@a/one")) != "one" {
		t.Fatal("caller modified cached bytes")
	}
	resolve("@a/two")
	resolve("@a/one")
	resolve("@a/three")
	resolve("@a/two") // two was LRU
	resolve("@b/one") // b has an independent cache even at the same socket
	now.Store(int64(9 * time.Second))
	resolve("@a/three") // hit must not extend TTL
	now.Store(int64(10 * time.Second))
	resolve("@a/three")
	mu.Lock()
	defer mu.Unlock()
	if calls["one"] != 2 || calls["two"] != 2 || calls["three"] != 2 {
		t.Fatalf("unexpected calls: %v", calls)
	}
}

func TestTTLStartsAfterSuccessfulResponse(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	path := broker(t, func(w http.ResponseWriter, r *http.Request) {
		key := requestKey(t, r)
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		reply(w, key, []byte("value"))
	})
	c, _ := New("kms::" + path + ":cache_ttl=10s")
	t.Cleanup(c.CloseIdleConnections)
	var now atomic.Int64
	base := time.Now()
	c.plugins["kms"].now = func() time.Time { return base.Add(time.Duration(now.Load())) }
	done := make(chan error, 1)
	go func() { _, err := c.Resolve(context.Background(), "@kms/test"); done <- err }()
	<-started
	now.Store(int64(40 * time.Second))
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	now.Store(int64(45 * time.Second))
	if _, err := c.Resolve(context.Background(), "@kms/test"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("TTL started before the response completed")
	}
}

func TestPluginRedirectIsNotFollowed(t *testing.T) {
	var calls atomic.Int32
	path := broker(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/v1/secrets/resolve" {
			w.Header().Set("Location", "/elsewhere")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		reply(w, "test", []byte("unexpected"))
	})
	c, _ := New("kms::" + path)
	t.Cleanup(c.CloseIdleConnections)
	if _, err := c.Resolve(context.Background(), "@kms/test"); err == nil {
		t.Fatal("redirect accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("redirect followed")
	}
}

func TestDisabledCacheAndExpiredFailure(t *testing.T) {
	for _, param := range []string{"cache_ttl=0", "cache_size=0"} {
		t.Run(param, func(t *testing.T) {
			var calls atomic.Int32
			path := broker(t, func(w http.ResponseWriter, r *http.Request) {
				key := requestKey(t, r)
				calls.Add(1)
				reply(w, key, []byte("value"))
			})
			c, _ := New("kms::" + path + ":" + param)
			t.Cleanup(c.CloseIdleConnections)
			for range 2 {
				if _, err := c.Resolve(context.Background(), "@kms/test"); err != nil {
					t.Fatal(err)
				}
			}
			if calls.Load() != 2 {
				t.Fatal("cache not disabled")
			}
		})
	}
	var fail atomic.Bool
	path := broker(t, func(w http.ResponseWriter, r *http.Request) {
		key := requestKey(t, r)
		if fail.Load() {
			w.WriteHeader(503)
			return
		}
		reply(w, key, []byte("old"))
	})
	c, _ := New("kms::" + path)
	t.Cleanup(c.CloseIdleConnections)
	var now atomic.Int64
	base := time.Now()
	c.plugins["kms"].now = func() time.Time { return base.Add(time.Duration(now.Load())) }
	if _, err := c.Resolve(context.Background(), "@kms/test"); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	now.Store(int64(defaultTTL))
	if value, err := c.Resolve(context.Background(), "@kms/test"); err == nil || value != nil {
		t.Fatal("expired value returned after plugin failure")
	}
}

func TestConcurrentWaitersCancelIndependently(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	path := broker(t, func(w http.ResponseWriter, r *http.Request) {
		key := requestKey(t, r)
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		reply(w, key, []byte("shared"))
	})
	c, _ := New("kms::" + path + ":cache_ttl=0")
	t.Cleanup(c.CloseIdleConnections)
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := c.Resolve(ctx, "@kms/test"); first <- err }()
	<-started
	const waiters = 8
	ready := make(chan struct{}, waiters)
	results := make(chan []byte, waiters)
	errs := make(chan error, waiters)
	for range waiters {
		go func() {
			v, err := c.Resolve(waitingContext{Context: context.Background(), ready: ready}, "@kms/test")
			results <- v
			errs <- err
		}()
	}
	for range waiters {
		select {
		case <-ready:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("waiters did not join the in-flight request")
		}
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first cancellation: %v", err)
	}
	close(release)
	for range waiters {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if value := <-results; string(value) != "shared" {
			t.Fatalf("value: %q", value)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("requests not coalesced: %d", calls.Load())
	}
}

// Done is evaluated when Resolve begins waiting for the shared request. This
// makes the concurrency check deterministic without sleeps or cache hits.
type waitingContext struct {
	context.Context
	ready chan<- struct{}
}

func (c waitingContext) Done() <-chan struct{} {
	c.ready <- struct{}{}
	return c.Context.Done()
}

func TestProcessClientInitializesOnlyOnReference(t *testing.T) {
	if os.Getenv("KUASAR_SECRET_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcessClientInitializesOnlyOnReference$")
		cmd.Env = append(os.Environ(), "KUASAR_SECRET_TEST_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated process: %v\n%s", err, out)
		}
		return
	}
	t.Setenv(Env, "invalid configuration")
	if _, err := Resolve(context.Background(), "00"); err != nil {
		t.Fatal(err)
	}
	path := broker(t, func(w http.ResponseWriter, r *http.Request) { reply(w, requestKey(t, r), []byte("fixed")) })
	t.Setenv(Env, "kms::"+path)
	if value, err := Resolve(context.Background(), "@kms/first"); err != nil || string(value) != "fixed" {
		t.Fatalf("initialization: %q %v", value, err)
	}
	t.Setenv(Env, "")
	if value, err := Resolve(context.Background(), "@kms/second"); err != nil || string(value) != "fixed" {
		t.Fatalf("configuration unexpectedly reloaded: %q %v", value, err)
	}
}
