// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/aveloxis/aveloxis/internal/config"
)

// rewriteToFixture is a base transport that sends every request — the
// github.com token endpoint and the api.github.com reads alike — to the
// test's fixture server. It is installed through oauth2.HTTPClient on the
// request context: oauth2's ContextClient reads that key for both Exchange
// and Client, so the GitHub callback is driven end to end with no
// production seam and its literal api.github.com URLs intact.
type rewriteToFixture struct{ target *url.URL }

func (rt rewriteToFixture) RoundTrip(req *http.Request) (*http.Response, error) {
	r2 := req.Clone(req.Context())
	r2.URL.Scheme = rt.target.Scheme
	r2.URL.Host = rt.target.Host
	r2.Host = rt.target.Host
	return http.DefaultTransport.RoundTrip(r2)
}

// TestGitHubCallbackUserReadIsBounded is the GitHub twin of
// TestGitLabCallbackUserReadIsBounded (batch 7c review round 6): the GitHub
// reads rested on a spelling-list pin that a wrapper function, a request
// literal, `http.NewRequest (` with a space (gofmt is not gated) and
// `NewRequestWithContext(context.Background(), …)` all escaped with every
// gate green. The fixture's token endpoint answers and its /user stalls; the
// handler must return at the (shortened) bound, both phases must have been
// reached, and the body must be the user-read failure.
func TestGitHubCallbackUserReadIsBounded(t *testing.T) {
	saved := oauthCallbackTimeout
	oauthCallbackTimeout = 200 * time.Millisecond
	t.Cleanup(func() { oauthCallbackTimeout = saved })

	var tokenHits, userReads atomic.Int32
	forge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/oauth/access_token":
			tokenHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"tok","token_type":"bearer"}`)
		case "/user":
			userReads.Add(1)
			select {
			case <-r.Context().Done(): // the bounded request gave up
			case <-time.After(5 * time.Second):
				fmt.Fprint(w, `{"id":1,"login":"x"}`)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer forge.Close()
	target, err := url.Parse(forge.URL)
	if err != nil {
		t.Fatal(err)
	}

	s := New(nil, config.WebConfig{DevMode: true, GitHubClientID: "id", GitHubClientSecret: "secret"}, nil, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/auth/github/callback?code=c&state=st", nil)
	req.AddCookie(&http.Cookie{Name: "oauth_state", Value: "st"})
	req = req.WithContext(context.WithValue(req.Context(), oauth2.HTTPClient, &http.Client{Transport: rewriteToFixture{target}}))
	start := time.Now()
	s.handleGitHubCallback(rec, req)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the GitHub callback took %s against a stalled /user; want it to return at the %s bound", took, oauthCallbackTimeout)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("a stalled user read answered %d; want 500", rec.Code)
	}
	if tokenHits.Load() < 1 || userReads.Load() < 1 {
		t.Fatalf("phases reached: token=%d user=%d; want both — otherwise the bound on the user read was not exercised", tokenHits.Load(), userReads.Load())
	}
	if body := rec.Body.String(); !strings.Contains(body, "Failed to get user info") {
		t.Errorf("the 500 body is %q; want the user-read failure, not the exchange's", strings.TrimSpace(body))
	}
}
