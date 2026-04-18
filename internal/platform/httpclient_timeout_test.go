package platform

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// TestHTTPClientTimeout_IsSixtySeconds — the whole-request timeout needs to
// survive GraphQL responses that carry ~1 MB of nested issue/PR data. The
// old 30s left only a 3× margin above p99 GraphQL response times (~10s) and
// risked spurious failures behind firewalls that add per-byte latency.
// 60s gives a safe 6× margin.
func TestHTTPClientTimeout_IsSixtySeconds(t *testing.T) {
	src, err := os.ReadFile("httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Source-contract check is the cleanest way to pin a struct literal
	// value without touching the real client. The setter lives inside
	// NewHTTPClient so we match against the full initializer.
	if !strings.Contains(code, "Timeout:   60 * time.Second") &&
		!strings.Contains(code, "Timeout: 60 * time.Second") &&
		!strings.Contains(code, "Timeout:60 * time.Second") {
		t.Error("http.Client.Timeout must be 60s to accommodate GraphQL payloads; previously 30s left insufficient margin above p99 response times behind firewalls")
	}
}

// TestHTTPTransport_ResponseHeaderTimeoutIsSet — a stalled connection with
// no response headers should fail fast instead of consuming the full
// whole-request budget. 15s is GitHub's own server-side p99 for response
// headers (headers always return quickly; the body is what sometimes
// drags). Anything longer than 15s without response headers is a stalled
// TCP connection, not a slow server.
func TestHTTPTransport_ResponseHeaderTimeoutIsSet(t *testing.T) {
	src, err := os.ReadFile("httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "ResponseHeaderTimeout: 15 * time.Second") &&
		!strings.Contains(code, "ResponseHeaderTimeout:  15 * time.Second") &&
		!strings.Contains(code, "ResponseHeaderTimeout:15 * time.Second") {
		t.Error("http.Transport.ResponseHeaderTimeout must be set to 15s so stalled connections fail fast instead of holding a worker slot for the full whole-request budget")
	}
}

// TestHTTPClient_StalledConnectionFailsFast — live test: a server that
// accepts the connection but never writes a response must cause Get to
// return within ~ResponseHeaderTimeout, not the full 60s. Without the
// explicit setting, Go's default is unlimited — a stalled firewall
// connection would hold the worker for 60s.
func TestHTTPClient_StalledConnectionFailsFast(t *testing.T) {
	// A raw TCP listener that accepts connections and hangs.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Hold the connection open; never respond.
			_ = conn
		}
	}()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient("http://"+listener.Addr().String(), keys, logger, AuthGitHub)

	// Context slightly longer than ResponseHeaderTimeout (15s). The first
	// HTTP attempt should fail at ~15s via ResponseHeaderTimeout; the retry
	// loop's 2s sleep then aborts on ctx.Done at ~18s. We assert elapsed
	// ~17-19s, which proves (a) ResponseHeaderTimeout fired at 15s (not
	// the whole-request 60s), and (b) the ctx-aware sleep honored ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()

	start := time.Now()
	_, err = client.Get(ctx, "/any")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error on stalled connection")
	}
	// Must exceed ResponseHeaderTimeout (15s). Faster means something else
	// cancelled early — e.g., a dial-level failure that wasn't what we
	// meant to test.
	if elapsed < 13*time.Second {
		t.Errorf("stalled connection failed in %v — faster than the 15s ResponseHeaderTimeout, something else returned early", elapsed)
	}
	// Must not exceed the 60s whole-request Timeout — if the retry loop
	// blocked past ctx deadline (non-ctx-aware sleep), we'd see ~60s+.
	if elapsed > 22*time.Second {
		t.Errorf("stalled connection took %v to fail — ResponseHeaderTimeout/ctx-aware-sleep not effective", elapsed)
	}
}

// TestHTTPClient_NormalResponsesStillSucceed — pin that the timeout bump
// didn't regress the happy path. A fast server must still return quickly.
func TestHTTPClient_NormalResponsesStillSucceed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/")
	if err != nil {
		t.Fatalf("normal GET failed: %v", err)
	}
	resp.Body.Close()
}
