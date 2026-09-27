// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestResolveTokenDistinguishesAStoreErrorFromABadToken pins worklist
// follow-up 6 (the PR #207 review) at the API boundary, as a class (review
// round 1 of the batch): each of the three lookups behind a Bearer token
// (validate, admin flag, scope) can fail as a STORE. That is not "invalid or
// expired" — the GUI deletes its token on every 401 — so a store failure
// answers 503 with a try-again body and is logged; only a token the store
// says is unknown or expired answers 401. In best-effort mode (require_auth
// off, or an exempt client) a presented token that cannot be resolved
// because the store failed is refused the same way, not run unscoped.
func TestResolveTokenDistinguishesAStoreErrorFromABadToken(t *testing.T) {
	const invalidBody = "invalid or expired session token"
	for name, tc := range map[string]struct {
		store   *fakeSessionStore
		require bool
		token   string
		code    int
		body    string
	}{
		"unknown token":              {&fakeSessionStore{userID: 7, valid: map[string]bool{"good": true}}, true, "nope", http.StatusUnauthorized, invalidBody},
		"validate: store error":      {&fakeSessionStore{userID: 7, validateErr: contextErr("connection reset"), valid: map[string]bool{"good": true}}, true, "good", http.StatusServiceUnavailable, "try again"},
		"admin flag: store error":    {&fakeSessionStore{userID: 7, admin: true, adminErr: contextErr("connection reset"), valid: map[string]bool{"good": true}}, true, "good", http.StatusServiceUnavailable, "try again"},
		"scope: store error":         {&fakeSessionStore{userID: 7, scopeErr: contextErr("connection reset"), valid: map[string]bool{"good": true}}, true, "good", http.StatusServiceUnavailable, "try again"},
		"best-effort, store error":   {&fakeSessionStore{userID: 7, adminErr: contextErr("connection reset"), valid: map[string]bool{"good": true}}, false, "good", http.StatusServiceUnavailable, "try again"},
		"best-effort, unknown token": {&fakeSessionStore{userID: 7, valid: map[string]bool{"good": true}}, false, "nope", http.StatusOK, ""},
		"good token":                 {&fakeSessionStore{userID: 7, valid: map[string]bool{"good": true}}, true, "good", http.StatusOK, ""},
	} {
		t.Run(name, func(t *testing.T) {
			h := authedChain(t, tc.store, Options{RequireAuth: tc.require}, okHandler())
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/api/v1/repos/1/stats", nil)
			req.RemoteAddr = "203.0.113.5:1"
			req.Header.Set("Authorization", "Bearer "+tc.token)
			h.ServeHTTP(rec, req)
			if rec.Code != tc.code || !strings.Contains(rec.Body.String(), tc.body) {
				t.Errorf("= %d %q; want %d with %q", rec.Code, rec.Body.String(), tc.code, tc.body)
			}
			if tc.code == http.StatusServiceUnavailable && strings.Contains(rec.Body.String(), invalidBody) {
				t.Errorf("a store error must not read as %q (the GUI drops its token on that)", invalidBody)
			}
		})
	}
}

// TestCanceledRequestIsNotAStoreFailure pins batch-2 review round 2: a
// client that disconnects mid-lookup cancels r.Context(), and the store
// returns a context.Canceled-wrapped error — that is neither an invalid
// token nor a store failure. No ERROR line (the operator would chase the
// wrong layer), and the request is simply not served.
func TestCanceledRequestIsNotAStoreFailure(t *testing.T) {
	var logs bytes.Buffer
	store := &fakeSessionStore{userID: 7, validateErr: fmt.Errorf("validate: %w", context.Canceled), valid: map[string]bool{"good": true}}
	rl, err := newRateLimiter(Options{RequireAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	a := newAuthenticator(store, true, slog.New(slog.NewTextHandler(&logs, nil)))
	served := false
	h := a.middleware(rl, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served = true }))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/repos/1/stats", nil)
	req.RemoteAddr = "203.0.113.5:1"
	req.Header.Set("Authorization", "Bearer good")
	h.ServeHTTP(rec, req)
	if served {
		t.Error("a request cancelled during the session lookup was served")
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a cancelled request logged as a store failure:\n%s", logs.String())
	}
	if strings.Contains(rec.Body.String(), "invalid or expired session token") {
		t.Error("a cancelled request read as an invalid token")
	}
}
