// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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

// hop is one redirect a sameHostRedirector serves: requests to the map key's
// path get status with an ABSOLUTE same-host Location to `to`.
type hop struct {
	status int
	to     string
}

// sameHostRedirector serves the given redirects on ONE server and answers
// 200 on any other path, counting those final hits. Until v0.29.12 these
// tests redirected from one httptest server to another — a different
// host:port — which is exactly the cross-host follow v0.29.12 refuses
// (httpclient_redirect_host_test.go); the hook's contract is about status
// codes, not hosts, so every hop now stays on one host.
func sameHostRedirector(t *testing.T, hops map[string]hop) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var finalHits atomic.Int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h, ok := hops[r.URL.Path]; ok {
			w.Header().Set("Location", srv.URL+h.to)
			w.WriteHeader(h.status)
			return
		}
		finalHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &finalHits
}

func hookClient(baseURL string) *HTTPClient {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHTTPClient(baseURL, NewKeyPool([]string{"t"}, logger), logger, AuthGitHub)
}

func getOK(t *testing.T, c *HTTPClient, path string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Get(ctx, path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
}

// TestOnPermanentRedirect_Fires301 verifies the hook fires on a 301 and
// receives the from/to URLs. Use case: GitHub renamed the repo; we need to
// update repos.repo_git in the DB. Without the hook, httpclient silently
// follows the redirect and the DB entry stays stale — the repo keeps getting
// collected under its old name.
func TestOnPermanentRedirect_Fires301(t *testing.T) {
	srv, finalHits := sameHostRedirector(t, map[string]hop{
		"/repos/old-owner/old-repo": {http.StatusMovedPermanently, "/repos/new-owner/new-repo"},
	})
	client := hookClient(srv.URL)

	var mu sync.Mutex
	var fires []struct{ from, to string }
	client.OnPermanentRedirect(func(from, to string) {
		mu.Lock()
		defer mu.Unlock()
		fires = append(fires, struct{ from, to string }{from, to})
	})
	getOK(t, client, "/repos/old-owner/old-repo")

	if finalHits.Load() == 0 {
		t.Fatal("target was never hit — redirect was not followed")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(fires) != 1 {
		t.Fatalf("expected exactly 1 hook call, got %d: %+v", len(fires), fires)
	}
	if fires[0].from != srv.URL+"/repos/old-owner/old-repo" {
		t.Errorf("from = %q, want origin+path", fires[0].from)
	}
	if fires[0].to != srv.URL+"/repos/new-owner/new-repo" {
		t.Errorf("to = %q, want target+path", fires[0].to)
	}
}

// TestOnPermanentRedirect_Fires308 — 308 has the same semantic as 301 (must
// preserve method on redirect) and must also trigger the hook. Covered
// separately from 301 because implementations sometimes forget one.
func TestOnPermanentRedirect_Fires308(t *testing.T) {
	srv, _ := sameHostRedirector(t, map[string]hop{"/old": {http.StatusPermanentRedirect, "/new"}})
	client := hookClient(srv.URL)
	var fired atomic.Bool
	client.OnPermanentRedirect(func(from, to string) { fired.Store(true) })
	getOK(t, client, "/old")
	if !fired.Load() {
		t.Error("OnPermanentRedirect should fire on 308 — same semantic as 301 for repo renames")
	}
}

// TestOnPermanentRedirect_DoesNotFire302 — a 302 is temporary (auth flow,
// transient endpoint redirect). The hook must NOT fire: mutating repo_git
// on a temporary redirect would mis-point the DB.
func TestOnPermanentRedirect_DoesNotFire302(t *testing.T) {
	srv, finalHits := sameHostRedirector(t, map[string]hop{"/endpoint": {http.StatusFound, "/endpoint-now"}})
	client := hookClient(srv.URL)
	var fired atomic.Bool
	client.OnPermanentRedirect(func(from, to string) { fired.Store(true) })
	getOK(t, client, "/endpoint")
	if finalHits.Load() != 1 {
		t.Fatalf("the 302 was not followed (final hits %d)", finalHits.Load())
	}
	if fired.Load() {
		t.Error("OnPermanentRedirect must NOT fire on 302 — it's a temporary redirect and the repo identity is still at the old URL")
	}
}

// TestOnPermanentRedirect_DoesNotFire307 — 307 is method-preserving
// temporary redirect. Same reasoning as 302: don't fire the hook.
func TestOnPermanentRedirect_DoesNotFire307(t *testing.T) {
	srv, finalHits := sameHostRedirector(t, map[string]hop{"/endpoint": {http.StatusTemporaryRedirect, "/endpoint-now"}})
	client := hookClient(srv.URL)
	var fired atomic.Bool
	client.OnPermanentRedirect(func(from, to string) { fired.Store(true) })
	getOK(t, client, "/endpoint")
	if finalHits.Load() != 1 {
		t.Fatalf("the 307 was not followed (final hits %d)", finalHits.Load())
	}
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
	srv, finalHits := sameHostRedirector(t, map[string]hop{
		"/A": {http.StatusMovedPermanently, "/B"},
		"/B": {http.StatusMovedPermanently, "/C"},
	})
	client := hookClient(srv.URL)
	var fires atomic.Int32
	client.OnPermanentRedirect(func(from, to string) { fires.Add(1) })
	getOK(t, client, "/A")
	if finalHits.Load() != 1 {
		t.Fatalf("the chain did not end at /C (final hits %d)", finalHits.Load())
	}
	if got := fires.Load(); got != 2 {
		t.Errorf("expected 2 hook calls for two-hop chain, got %d", got)
	}
}

// TestOnPermanentRedirect_NilSafe — if no hook is installed, redirects must
// still follow normally. A nil-callback panic would break everyone who
// didn't opt in to the hook.
func TestOnPermanentRedirect_NilSafe(t *testing.T) {
	srv, finalHits := sameHostRedirector(t, map[string]hop{"/old": {http.StatusMovedPermanently, "/new"}})
	client := hookClient(srv.URL)
	// Intentionally do NOT call OnPermanentRedirect.
	getOK(t, client, "/old")
	if finalHits.Load() != 1 {
		t.Errorf("redirect not followed without a hook (final hits %d)", finalHits.Load())
	}
}
