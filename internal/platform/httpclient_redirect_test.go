package platform

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

// silentLogger returns a slog.Logger that writes to stderr but only at ERROR
// level, keeping test output readable while still capturing real problems.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestGet301FollowsLocation — the client must follow a 301 Moved Permanently
// to the Location header's URL and return the target response. GitHub uses
// 301 when a repository was renamed or transferred; failing to follow means
// every endpoint silently 10x-retries and times out.
func TestGet301FollowsLocation(t *testing.T) {
	var targetHits int32
	mux := http.NewServeMux()
	mux.HandleFunc("/old/path", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/new/path")
		w.WriteHeader(http.StatusMovedPermanently)
		w.Write([]byte(`{"message":"Moved Permanently","url":"/new/path"}`))
	})
	mux.HandleFunc("/new/path", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&targetHits, 1)
		w.Write([]byte(`{"ok":true}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	resp, err := client.Get(context.Background(), "/old/path")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("final status = %d, want 200 — redirect should have been followed to target", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"ok":true}` {
		t.Errorf("body = %q, want target response — redirect not actually followed", string(body))
	}
	if atomic.LoadInt32(&targetHits) != 1 {
		t.Errorf("target hit %d times, want exactly 1 — follow should not loop or re-hit source", targetHits)
	}
}

// TestGet302FollowsLocation — 302 Found is temporary but still must be followed
// on the current request. Go's default follower handles this when Location is
// valid, but we need our switch to handle it too for the case where Go's
// follower gave up (looped or bad Location).
func TestGet302FollowsLocation(t *testing.T) {
	var targetHits int32
	mux := http.NewServeMux()
	mux.HandleFunc("/src", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/dst")
		w.WriteHeader(http.StatusFound)
	})
	mux.HandleFunc("/dst", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&targetHits, 1)
		w.Write([]byte(`{"ok":true}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	resp, err := client.Get(context.Background(), "/src")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if atomic.LoadInt32(&targetHits) != 1 {
		t.Errorf("target hit %d times, want 1", targetHits)
	}
}

// TestGet307FollowsLocation — 307 Temporary Redirect (used by GitHub for
// release asset uploads). Must be followed.
func TestGet307FollowsLocation(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/src", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/dst")
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/dst", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ok":true}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	resp, err := client.Get(context.Background(), "/src")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (307 should follow Location)", resp.StatusCode)
	}
}

// TestGet308FollowsLocation — 308 Permanent Redirect (strict variant of 301).
// Must be followed.
func TestGet308FollowsLocation(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/src", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/dst")
		w.WriteHeader(http.StatusPermanentRedirect)
	})
	mux.HandleFunc("/dst", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ok":true}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	resp, err := client.Get(context.Background(), "/src")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (308 should follow Location)", resp.StatusCode)
	}
}

// TestGet301EmptyLocationReturnsErrGone — when the 3xx response has no
// Location header (the observed GitHub case: body `{"url":""}`), the switch
// must NOT retry 10 times. It must log once and return an error wrapping
// ErrGone so the caller skips the item via isOptionalEndpointSkip.
func TestGet301EmptyLocationReturnsErrGone(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		// No Location header. GitHub does this when it cannot determine the target.
		w.WriteHeader(http.StatusMovedPermanently)
		w.Write([]byte(`{"message":"Moved Permanently","url":""}`))
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	_, err := client.Get(context.Background(), "/gone")
	if err == nil {
		t.Fatal("expected error when 3xx has no Location")
	}
	if !errors.Is(err, ErrGone) {
		t.Errorf("err = %v, want errors.Is(err, ErrGone) — caller needs a non-retryable sentinel so isOptionalEndpointSkip can skip the item cleanly", err)
	}
	// Critical: the original bug was 10 retries each hitting 301 → ~1 min wasted.
	// With the fix, one server hit is enough; we may allow a second if the
	// implementation does a belt-and-braces read, but absolutely no retries.
	if h := atomic.LoadInt32(&hits); h > 2 {
		t.Errorf("server hit %d times, want ≤ 2 — 3xx with empty Location must not retry through the default path", h)
	}
}

// TestGet410ReturnsErrGone — 410 Gone is the explicit "this resource no
// longer exists" status. Must be non-retryable and return ErrGone.
func TestGet410ReturnsErrGone(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusGone)
		w.Write([]byte(`{"message":"This issue was deleted","status":"410"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	_, err := client.Get(context.Background(), "/repos/o/r/issues/999")
	if err == nil {
		t.Fatal("expected error for 410 Gone")
	}
	if !errors.Is(err, ErrGone) {
		t.Errorf("err = %v, want errors.Is(err, ErrGone) — 410 is the canonical 'this resource is gone' status", err)
	}
	if h := atomic.LoadInt32(&hits); h != 1 {
		t.Errorf("server hit %d times, want exactly 1 — 410 must never be retried", h)
	}
}

// TestGet301RedirectLoopBailsOut — a 3xx that loops back to itself must not
// hang or retry forever. The client must cap hops and return ErrGone once
// the cap is exceeded. Prevents pathological GitHub chains from burning
// minutes per endpoint.
func TestGet301RedirectLoopBailsOut(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		// Always redirect to self → loop.
		w.Header().Set("Location", r.URL.Path)
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	_, err := client.Get(context.Background(), "/loop")
	if err == nil {
		t.Fatal("expected error when redirect loops forever")
	}
	if !errors.Is(err, ErrGone) {
		t.Errorf("err = %v, want errors.Is(err, ErrGone) — loop exhaustion is effectively 'resource unreachable'", err)
	}
	// Allow a reasonable hop cap but nothing approaching 10×10 = 100 hits from
	// the old "retry everything" behavior.
	if h := atomic.LoadInt32(&hits); h > 8 {
		t.Errorf("server hit %d times, want ≤ 8 (hop cap should prevent runaway) — the whole point of this fix is to stop pathological chains", h)
	}
}

// TestErrGoneIsExported — sentinel must be exported and distinct from
// ErrNotFound so callers can tell "never existed" apart from "existed, now
// gone" for different treatment if they want to.
func TestErrGoneIsExported(t *testing.T) {
	if ErrGone == nil {
		t.Fatal("ErrGone must be a non-nil exported sentinel")
	}
	if ErrGone.Error() == "" {
		t.Error("ErrGone must have a non-empty message")
	}
	if errors.Is(ErrGone, ErrNotFound) {
		t.Error("ErrGone must be distinct from ErrNotFound — they mean different things (410 vs 404) and callers may want to log them differently")
	}
}

// Silence "declared and not used" for fmt when the package compiles fine.
var _ = fmt.Sprintf
