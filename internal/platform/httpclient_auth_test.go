// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// v0.22.12 — every HTTP request must carry the platform-appropriate
// auth header BEFORE reaching GitHub/GitLab. On 2026-05-18 an
// operator suspected our requests were going out unauthenticated
// during a 500-flood incident. The hypothesis turned out to be
// wrong (the 500s were a transient GitHub-side issue), but the
// concern revealed a coverage gap: nothing in the test suite proves
// the auth header is set on every code path, including the retry
// path.
//
// These tests fill that gap. They use httptest to capture every
// incoming request's headers and assert the platform-specific auth
// shape is always present. If a future refactor accidentally drops
// the auth header — or strips it on a retry — these tests fail.

// captureHeaders is a test handler that records every incoming
// request's Authorization and PRIVATE-TOKEN headers. Returns a
// fail-then-succeed handler so the retry path is exercised.
func captureHeaders(failCount int) (http.HandlerFunc, *[]http.Header, *sync.Mutex) {
	var (
		mu       sync.Mutex
		captured []http.Header
		hits     int
	)
	h := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		captured = append(captured, r.Header.Clone())
		hits++
		shouldFail := hits <= failCount
		mu.Unlock()
		if shouldFail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}
	return h, &captured, &mu
}

// TestHTTPClientAlwaysAttachesAuthorizationHeaderGitHub is the
// regression tripwire requested on 2026-05-18 ("I would expect tests
// to catch these kinds of regressions"). Captures every request the
// server receives (including retries) and asserts each one carries
// `Authorization: token <key>`.
//
// Designed so that a future refactor that, say, bypasses GetKey()
// on the retry path, or strips the header before calling
// client.Do(), fires this test immediately.
func TestHTTPClientAlwaysAttachesAuthorizationHeaderGitHub(t *testing.T) {
	const failCount = 1 // one retry so we exercise the retry-path header attach
	handler, captured, mu := captureHeaders(failCount)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"secret-token"}, silentLogger()), silentLogger(), AuthGitHub)
	resp, err := client.Get(context.Background(), "/")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(*captured) != failCount+1 {
		t.Fatalf("server saw %d requests, want %d (retry must happen so the retry-path auth is exercised)", len(*captured), failCount+1)
	}
	for i, hdrs := range *captured {
		got := hdrs.Get("Authorization")
		if got != "token secret-token" {
			t.Errorf("request %d: Authorization header = %q, want %q — "+
				"every request including retries MUST carry the GitHub PAT in `token <key>` form. "+
				"A regression that strips the header (or runs an unauthenticated path on retry) "+
				"would let GitHub respond with 60/hr unauthenticated rate-limit, masquerading as "+
				"a healthy collection.", i, got, "token secret-token")
		}
		if hdrs.Get("PRIVATE-TOKEN") != "" {
			t.Errorf("request %d: PRIVATE-TOKEN header should NOT be set on GitHub-style client — got %q", i, hdrs.Get("PRIVATE-TOKEN"))
		}
	}
}

// TestHTTPClientAlwaysAttachesAuthorizationHeaderGitLab — same
// regression tripwire, GitLab variant. Asserts `PRIVATE-TOKEN: <key>`
// rather than `Authorization: token <key>`.
func TestHTTPClientAlwaysAttachesAuthorizationHeaderGitLab(t *testing.T) {
	const failCount = 1
	handler, captured, mu := captureHeaders(failCount)
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"glpat-secret"}, silentLogger()), silentLogger(), AuthGitLab)
	resp, err := client.Get(context.Background(), "/")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(*captured) != failCount+1 {
		t.Fatalf("server saw %d requests, want %d", len(*captured), failCount+1)
	}
	for i, hdrs := range *captured {
		got := hdrs.Get("PRIVATE-TOKEN")
		if got != "glpat-secret" {
			t.Errorf("request %d: PRIVATE-TOKEN header = %q, want %q — "+
				"GitLab-style client must send the PAT in PRIVATE-TOKEN form on every request",
				i, got, "glpat-secret")
		}
		if hdrs.Get("Authorization") != "" {
			t.Errorf("request %d: Authorization header should NOT be set on GitLab-style client — got %q", i, hdrs.Get("Authorization"))
		}
	}
}
