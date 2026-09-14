// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestLeaseIsReleasedOnEveryWireExit — the runtime half of the no-bypass
// tripwire (review round on the 2026-09-12 change). The source pin can
// count release calls per Acquire, but "every exit path between Acquire
// and Do releases" is a per-path property a count cannot see: dropping
// the release on the NewRequest-error arm of HTTPClient.Get left one
// release() in the function and the count pin green, while a request
// that could not even be built held an in-flight slot forever. So the
// three exits of each wire path are DRIVEN and the pool's in-flight
// counters must read zero after each: a request that cannot be built
// (control byte in the URL), a Do that fails (server that never
// answers, ctx deadline), and a Do that succeeds.
func TestLeaseIsReleasedOnEveryWireExit(t *testing.T) {
	t.Cleanup(SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil }))

	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{"x":1},"ok":true}`)
	}))
	defer ok.Close()
	// The handler must also exit on a test-owned signal: a POST whose
	// body the handler never reads gets no client-disconnect cancel from
	// net/http, and Server.Close waits for every handler.
	done := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-done:
		}
	}))
	defer hang.Close()
	defer close(done)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	assertIdle := func(t *testing.T, kp *KeyPool, exit string) {
		t.Helper()
		_, inflight := kp.Snapshot()
		kp.mu.Lock()
		perKey := kp.keys[0].inflight
		kp.mu.Unlock()
		if inflight != 0 || perKey != 0 {
			t.Errorf("%s: pool inflight=%d key inflight=%d after the call returned, want 0/0 — the lease leaked on this exit", exit, inflight, perKey)
		}
	}

	type wire struct {
		name string
		call func(ctx context.Context, c *HTTPClient) error
	}
	for _, w := range []wire{
		{"Get", func(ctx context.Context, c *HTTPClient) error { _, err := c.Get(ctx, "/x"); return err }},
		{"GraphQL", func(ctx context.Context, c *HTTPClient) error {
			var dest struct{}
			return c.GraphQL(ctx, "query{x}", nil, &dest)
		}},
	} {
		t.Run(w.name+"/request-cannot-be-built", func(t *testing.T) {
			kp := NewKeyPool([]string{"k"}, logger)
			c := NewHTTPClient("http://127.0.0.1:1/\x01", kp, logger, AuthGitHub)
			if err := w.call(context.Background(), c); err == nil {
				t.Fatal("a URL with a control byte must fail to build a request")
			}
			assertIdle(t, kp, "NewRequest error")
		})
		t.Run(w.name+"/do-fails", func(t *testing.T) {
			kp := NewKeyPool([]string{"k"}, logger)
			c := NewHTTPClient(hang.URL, kp, logger, AuthGitHub)
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			if err := w.call(ctx, c); err == nil {
				t.Fatal("a server that never answers must surface the ctx deadline")
			}
			assertIdle(t, kp, "Do error")
		})
		t.Run(w.name+"/do-succeeds", func(t *testing.T) {
			kp := NewKeyPool([]string{"k"}, logger)
			c := NewHTTPClient(ok.URL, kp, logger, AuthGitHub)
			if err := w.call(context.Background(), c); err != nil {
				t.Fatalf("200 from the server: %v", err)
			}
			assertIdle(t, kp, "success")
		})
	}
}
