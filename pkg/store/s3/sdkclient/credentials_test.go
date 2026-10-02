package sdkclient

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type credentialsProviderFunc func(context.Context) (Credentials, error)

func (f credentialsProviderFunc) Retrieve(ctx context.Context) (Credentials, error) {
	return f(ctx)
}

func newCredentialTestClient(t *testing.T, server *httptest.Server, cfg Config) *Client {
	t.Helper()
	setDefaultCredentialEnvironment(t)
	cfg.Endpoint = server.URL
	cfg.Bucket = "test-bucket"
	cfg.PathStyle = true
	client, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestCredentialSelection(t *testing.T) {
	requests := make(chan *http.Request, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Clone(context.Background())
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Run("static unchanged", func(t *testing.T) {
		client := newCredentialTestClient(t, server, Config{AccessKey: "static-ak", SecretKey: "static-sk"})
		if _, err := client.Head(context.Background(), "key"); err != nil {
			t.Fatal(err)
		}
		if auth := (<-requests).Header.Get("Authorization"); !strings.Contains(auth, "Credential=static-ak/") {
			t.Fatalf("Authorization did not use static credentials: %q", auth)
		}
	})

	t.Run("provider overrides legacy static fields", func(t *testing.T) {
		client := newCredentialTestClient(t, server, Config{
			AccessKey: "stale-legacy-ak",
			Credentials: credentialsProviderFunc(func(context.Context) (Credentials, error) {
				return Credentials{AccessKeyID: "bound-ak", SecretAccessKey: "bound-sk", SessionToken: "bound-token"}, nil
			}),
		})
		if _, err := client.Head(context.Background(), "key"); err != nil {
			t.Fatal(err)
		}
		r := <-requests
		if auth := r.Header.Get("Authorization"); !strings.Contains(auth, "Credential=bound-ak/") || strings.Contains(auth, "stale-legacy-ak") {
			t.Fatalf("Authorization did not use bound credentials: %q", auth)
		}
		if token := r.Header.Get("X-Amz-Security-Token"); token != "bound-token" {
			t.Fatalf("session token = %q", token)
		}
	})
}

func TestCredentialProviderRefreshAndErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	t.Run("expiry refresh uses SDK cache", func(t *testing.T) {
		var calls atomic.Int32
		client := newCredentialTestClient(t, server, Config{Credentials: credentialsProviderFunc(func(context.Context) (Credentials, error) {
			calls.Add(1)
			return Credentials{AccessKeyID: "ak", SecretAccessKey: "sk", SessionToken: "token", CanExpire: true, Expires: time.Now().Add(40 * time.Millisecond)}, nil
		})})
		if _, err := client.Head(context.Background(), "one"); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Head(context.Background(), "two"); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 1 {
			t.Fatalf("provider calls before expiry = %d, want 1", calls.Load())
		}
		time.Sleep(60 * time.Millisecond)
		if _, err := client.Head(context.Background(), "three"); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatalf("provider calls after expiry = %d, want 2", calls.Load())
		}
	})

	t.Run("failure is authoritative", func(t *testing.T) {
		want := errors.New("bound provider failed")
		client := newCredentialTestClient(t, server, Config{Credentials: credentialsProviderFunc(func(context.Context) (Credentials, error) {
			return Credentials{}, want
		})})
		_, err := client.Head(context.Background(), "key")
		if !errors.Is(err, want) {
			t.Fatalf("Head error = %v, want provider error", err)
		}
	})

	t.Run("invalid credentials", func(t *testing.T) {
		client := newCredentialTestClient(t, server, Config{Credentials: credentialsProviderFunc(func(context.Context) (Credentials, error) {
			return Credentials{AccessKeyID: "ak"}, nil
		})})
		_, err := client.Head(context.Background(), "key")
		if err == nil || !strings.Contains(err.Error(), "empty access key or secret key") {
			t.Fatalf("Head error = %v", err)
		}
	})
}

func TestCloseCancelsAndWaitsForCredentialCallback(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	provider := credentialsProviderFunc(func(ctx context.Context) (Credentials, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return Credentials{}, ctx.Err()
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	client := newCredentialTestClient(t, server, Config{Credentials: provider})
	requestDone := make(chan error, 1)
	go func() {
		_, err := client.Head(context.Background(), "key")
		requestDone <- err
	}()
	<-started
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before provider callback finished")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := <-requestDone; err == nil {
		t.Fatal("request unexpectedly succeeded")
	}
	if _, err := client.lifetime.Retrieve(context.Background()); !errors.Is(err, errProviderClosed) {
		t.Fatalf("Retrieve after Close = %v", err)
	}
}

func TestClientsHaveIndependentProviderLifetimes(t *testing.T) {
	var calls atomic.Int32
	provider := credentialsProviderFunc(func(context.Context) (Credentials, error) {
		calls.Add(1)
		return Credentials{AccessKeyID: "ak", SecretAccessKey: "sk"}, nil
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	a := newCredentialTestClient(t, server, Config{Credentials: provider})
	b := newCredentialTestClient(t, server, Config{Credentials: provider})
	if _, err := a.Head(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	if _, err := b.Head(context.Background(), "b"); err != nil {
		t.Fatalf("closing first client affected second: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("shared provider calls = %d, want independent caches", calls.Load())
	}
}

func TestCloseUsesResolvedDefaultsModeTransport(t *testing.T) {
	t.Setenv("AWS_DEFAULTS_MODE", "in-region")
	var idle, closed atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateIdle:
			idle.Add(1)
		case http.StateClosed:
			closed.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	client := newCredentialTestClient(t, server, Config{})
	if _, ok := client.api.Options().HTTPClient.(*ownedHTTPClient); !ok {
		t.Fatalf("resolved HTTP client = %T, want *ownedHTTPClient", client.api.Options().HTTPClient)
	}
	if _, err := client.Head(context.Background(), "key"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for idle.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if idle.Load() == 0 {
		t.Fatal("request connection did not become idle")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for closed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if closed.Load() == 0 {
		t.Fatal("Close did not close the actual idle object transport")
	}
}

func TestOwnedHTTPClientPreservesSDKRedirectBehavior(t *testing.T) {
	var redirectedToken string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedToken = r.Header.Get("X-Amz-Security-Token")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	redirect := func(status int, location string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if location != "" {
				w.Header().Set("Location", location)
			}
			w.WriteHeader(status)
		}))
	}

	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := redirect(status, target.URL)
			defer server.Close()
			req, _ := http.NewRequest(http.MethodHead, server.URL, nil)
			req.Header.Set("X-Amz-Security-Token", "secret")
			resp, err := newOwnedHTTPClient(http.DefaultTransport.(*http.Transport).Clone(), 0).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent || redirectedToken != "" {
				t.Fatalf("status = %d, redirected token = %q", resp.StatusCode, redirectedToken)
			}
		})
	}

	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := redirect(status, target.URL)
			defer server.Close()
			resp, err := newOwnedHTTPClient(http.DefaultTransport.(*http.Transport).Clone(), 0).Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, status)
			}
		})
	}

	t.Run("suppresses missing Location error", func(t *testing.T) {
		server := redirect(http.StatusMovedPermanently, "")
		defer server.Close()
		resp, err := newOwnedHTTPClient(http.DefaultTransport.(*http.Transport).Clone(), 0).Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMovedPermanently {
			t.Fatalf("status = %d", resp.StatusCode)
		}
	})
}

func TestProviderLifetimeAdmissionDoesNotRaceClose(t *testing.T) {
	provider := credentialsProviderFunc(func(ctx context.Context) (Credentials, error) {
		select {
		case <-ctx.Done():
			return Credentials{}, ctx.Err()
		default:
			return Credentials{AccessKeyID: "ak", SecretAccessKey: "sk"}, nil
		}
	})
	lifetime := newProviderLifetime(provider)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = lifetime.Retrieve(context.Background())
		}()
	}
	lifetime.close()
	wg.Wait()
}
