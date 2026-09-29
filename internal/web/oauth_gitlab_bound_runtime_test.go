// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
)

// TestGitLabCallbackUserReadIsBounded drives the GitLab callback against a
// fixture whose token endpoint answers and whose /api/v4/user stalls (batch
// 7c review round 4): oauth2's client copies http.DefaultClient's zero
// Timeout and uses the ctx to refresh a token and to select the base
// client (oauth2.HTTPClient), never as a per-request deadline, so a plain client.Get
// ran unbounded and a stalled GitLab held the callback for as long as the
// browser waited. The handler must return within the (shortened) bound.
// The test also pins WHICH phase failed (round 5): a token endpoint the
// oauth2 library stops accepting, or a fixture path typo, would 500 in
// 0 ms from the exchange and read as a pass — so the stalled read must
// have been reached, and the body must be the user-read failure.
func TestGitLabCallbackUserReadIsBounded(t *testing.T) {
	saved := oauthCallbackTimeout
	oauthCallbackTimeout = 200 * time.Millisecond
	t.Cleanup(func() { oauthCallbackTimeout = saved })

	var userReads atomic.Int32
	forge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"tok","token_type":"bearer"}`)
		case "/api/v4/user":
			userReads.Add(1)
			select {
			case <-r.Context().Done(): // the bounded request gave up
			case <-time.After(5 * time.Second):
				fmt.Fprint(w, `{"id":1}`) // no username: refused before the store (round 7)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer forge.Close()

	s := New(nil, config.WebConfig{DevMode: true, GitLabClientID: "id", GitLabClientSecret: "secret", GitLabBaseURL: forge.URL}, nil, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/auth/gitlab/callback?code=c&state=st", nil)
	req.AddCookie(&http.Cookie{Name: "oauth_state", Value: "st"})
	start := time.Now()
	s.handleGitLabCallback(rec, req)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the callback took %s against a stalled /api/v4/user; want it to return at the %s bound", took, oauthCallbackTimeout)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("a stalled user read answered %d; want 500", rec.Code)
	}
	if userReads.Load() < 1 {
		t.Fatal("the fixture's /api/v4/user was never read — the exchange failed first, so the bound on the user read was not exercised")
	}
	if body := rec.Body.String(); !strings.Contains(body, "Failed to get user info") {
		t.Errorf("the 500 body is %q; want the user-read failure, not the exchange's", strings.TrimSpace(body))
	}
}
