package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestOnPermanentRedirect_Fires301 verifies the hook fires on a 301 and
// receives the from/to URLs. Use case: GitHub renamed the repo; we need to
// update repos.repo_git in the DB. Without the hook, httpclient silently
// follows the redirect and the DB entry stays stale — the repo keeps getting
// collected under its old name.
func TestOnPermanentRedirect_Fires301(t *testing.T) {
	var finalRequested atomic.Int32
	var target *httptest.Server

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/repos/new-owner/new-repo")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer origin.Close()

	target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		finalRequested.Add(1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer target.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(origin.URL, keys, logger, AuthGitHub)

	var mu sync.Mutex
	var fires []struct{ from, to string }
	client.OnPermanentRedirect(func(from, to string) {
		mu.Lock()
		defer mu.Unlock()
		fires = append(fires, struct{ from, to string }{from, to})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/repos/old-owner/old-repo")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if finalRequested.Load() == 0 {
		t.Fatal("target server was never hit — redirect was not followed")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(fires) != 1 {
		t.Fatalf("expected exactly 1 hook call, got %d: %+v", len(fires), fires)
	}
	if fires[0].from != origin.URL+"/repos/old-owner/old-repo" {
		t.Errorf("from = %q, want origin+path", fires[0].from)
	}
	if fires[0].to != target.URL+"/repos/new-owner/new-repo" {
		t.Errorf("to = %q, want target+path", fires[0].to)
	}
}

// TestOnPermanentRedirect_Fires308 — 308 has the same semantic as 301 (must
// preserve method on redirect) and must also trigger the hook. Covered
// separately from 301 because implementations sometimes forget one.
func TestOnPermanentRedirect_Fires308(t *testing.T) {
	var target *httptest.Server
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/new")
		w.WriteHeader(http.StatusPermanentRedirect)
	}))
	defer origin.Close()
	target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(origin.URL, keys, logger, AuthGitHub)

	var fired atomic.Bool
	client.OnPermanentRedirect(func(from, to string) {
		fired.Store(true)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/old")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if !fired.Load() {
		t.Error("OnPermanentRedirect should fire on 308 — same semantic as 301 for repo renames")
	}
}

// TestOnPermanentRedirect_DoesNotFire302 — a 302 is temporary (auth flow,
// transient endpoint redirect). The hook must NOT fire: mutating repo_git
// on a temporary redirect would mis-point the DB.
func TestOnPermanentRedirect_DoesNotFire302(t *testing.T) {
	var target *httptest.Server
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/endpoint")
		w.WriteHeader(http.StatusFound) // 302
	}))
	defer origin.Close()
	target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(origin.URL, keys, logger, AuthGitHub)

	var fired atomic.Bool
	client.OnPermanentRedirect(func(from, to string) {
		fired.Store(true)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/endpoint")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if fired.Load() {
		t.Error("OnPermanentRedirect must NOT fire on 302 — it's a temporary redirect and the repo identity is still at the old URL")
	}
}

// TestOnPermanentRedirect_DoesNotFire307 — 307 is method-preserving
// temporary redirect. Same reasoning as 302: don't fire the hook.
func TestOnPermanentRedirect_DoesNotFire307(t *testing.T) {
	var target *httptest.Server
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/endpoint")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(origin.URL, keys, logger, AuthGitHub)

	var fired atomic.Bool
	client.OnPermanentRedirect(func(from, to string) {
		fired.Store(true)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/endpoint")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if fired.Load() {
		t.Error("OnPermanentRedirect must NOT fire on 307 — it's a temporary redirect")
	}
}

// TestOnPermanentRedirect_MultipleHops_FiresOncePerHop verifies the hook
// fires for every 301/308 in a chain, so a repo that moved twice (A→B→C)
// yields two hook invocations and the DB can track the final destination.
// Cap is maxRedirectHops (5); within that range, fire for each permanent
// hop. Temporary hops in the chain do not fire.
func TestOnPermanentRedirect_MultipleHops_FiresOncePerHop(t *testing.T) {
	var hopC *httptest.Server
	var hopB *httptest.Server

	hopA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", hopB.URL+"/B")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer hopA.Close()
	hopB = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", hopC.URL+"/C")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer hopB.Close()
	hopC = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer hopC.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(hopA.URL, keys, logger, AuthGitHub)

	var fires atomic.Int32
	client.OnPermanentRedirect(func(from, to string) {
		fires.Add(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/A")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	if got := fires.Load(); got != 2 {
		t.Errorf("expected 2 hook calls for two-hop chain, got %d", got)
	}
}

// TestOnPermanentRedirect_NilSafe — if no hook is installed, redirects must
// still follow normally. A nil-callback panic would break everyone who
// didn't opt in to the hook.
func TestOnPermanentRedirect_NilSafe(t *testing.T) {
	var target *httptest.Server
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL+"/new")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer origin.Close()
	target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"t"}, logger)
	client := NewHTTPClient(origin.URL, keys, logger, AuthGitHub)
	// Intentionally do NOT call OnPermanentRedirect.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Get(ctx, "/old")
	if err != nil {
		t.Fatalf("Get should succeed even without a hook installed: %v", err)
	}
	resp.Body.Close()
}
