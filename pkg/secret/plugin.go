package secret

import (
	"bytes"
	"container/list"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	requestTimeout  = 5 * time.Second
	maxResponseSize = 1 << 20
)

type cachedValue struct {
	key     string
	value   []byte
	expires time.Time
}

type pending struct {
	done  chan struct{}
	value []byte
	err   error
}

type plugin struct {
	http    *http.Client
	ttl     time.Duration
	size    int
	now     func() time.Time
	mu      sync.Mutex
	cache   map[string]*list.Element
	lru     list.List
	pending map[string]*pending
}

func newPlugin(path string, ttl time.Duration, size int) *plugin {
	dialer := &net.Dialer{Timeout: requestTimeout}
	transport := &http.Transport{
		DialContext:           func(ctx context.Context, _, _ string) (net.Conn, error) { return dialer.DialContext(ctx, "unix", path) },
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: requestTimeout,
	}
	return &plugin{
		http: &http.Client{Transport: transport, Timeout: requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		ttl: ttl, size: size, now: time.Now,
		cache: make(map[string]*list.Element), pending: make(map[string]*pending),
	}
}

func (p *plugin) resolve(ctx context.Context, key string) ([]byte, error) {
	p.mu.Lock()
	if item := p.cache[key]; item != nil {
		entry := item.Value.(*cachedValue)
		if p.now().Before(entry.expires) {
			p.lru.MoveToFront(item)
			value := bytes.Clone(entry.value)
			p.mu.Unlock()
			return value, nil
		}
		p.remove(item)
	}
	call := p.pending[key]
	if call == nil {
		call = &pending{done: make(chan struct{})}
		p.pending[key] = call
		// Each waiter has its own context. The shared request has a bounded
		// lifetime independent of the first caller's cancellation/deadline.
		go p.fetch(key, call)
	}
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.done:
		return bytes.Clone(call.value), call.err
	}
}

func (p *plugin) fetch(key string, call *pending) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	value, err := p.request(ctx, key)
	p.mu.Lock()
	defer p.mu.Unlock()
	call.value, call.err = value, err
	if err == nil && p.ttl > 0 && p.size > 0 {
		entry := &cachedValue{key: key, value: bytes.Clone(value), expires: p.now().Add(p.ttl)}
		p.cache[key] = p.lru.PushFront(entry)
		for p.lru.Len() > p.size {
			p.remove(p.lru.Back())
		}
	}
	delete(p.pending, key)
	close(call.done)
}

func (p *plugin) remove(item *list.Element) {
	entry := item.Value.(*cachedValue)
	delete(p.cache, entry.key)
	p.lru.Remove(item)
	clear(entry.value)
}

func (p *plugin) request(ctx context.Context, key string) ([]byte, error) {
	body, err := json.Marshal(struct {
		Key string `json:"key"`
	}{key})
	if err != nil {
		return nil, errors.New("encode request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://secret/v1/secrets/resolve", bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("create request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, context.DeadlineExceeded
		}
		// A transport error can include a malformed status line or header
		// supplied by the broker. Do not expose response bytes as diagnostics.
		return nil, errors.New("resolve request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("resolve HTTP status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, context.DeadlineExceeded
	}
	if err != nil || len(raw) > maxResponseSize {
		return nil, errors.New("invalid resolve response")
	}
	var result struct {
		Key   *string `json:"key"`
		Value *string `json:"value"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Key == nil || result.Value == nil || *result.Key != key || !strings.HasPrefix(*result.Value, "base64:") {
		return nil, errors.New("invalid resolve response")
	}
	encoded := strings.TrimPrefix(*result.Value, "base64:")
	value, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(value) != encoded {
		return nil, errors.New("invalid resolve value encoding")
	}
	return value, nil
}
