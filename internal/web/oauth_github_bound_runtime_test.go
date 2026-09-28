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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// rewriteToFixture is a base transport that sends every request — the
// github.com token endpoint and the api.github.com reads alike — to the
// test's fixture server over the real transport it captured (base). It is
// installed through oauth2.HTTPClient on the request context: oauth2's
// ContextClient reads that key for both Exchange and Client, so the GitHub
// callback is driven end to end with no production seam and its literal
// api.github.com URLs intact.
type rewriteToFixture struct {
	target *url.URL
	base   http.RoundTripper
}

func (rt rewriteToFixture) RoundTrip(req *http.Request) (*http.Response, error) {
	r2 := req.Clone(req.Context())
	r2.URL.Scheme = rt.target.Scheme
	r2.URL.Host = rt.target.Host
	r2.Host = rt.target.Host
	return rt.base.RoundTrip(r2)
}

// refuseOffFixture records and refuses every request that reaches
// http.DefaultTransport during a GitHub-callback test (batch 7c review
// round 7): a handler that hands Exchange or Client a context without
// oauth2.HTTPClient, or sends a read through a client other than the one
// Client returned (round 8), reaches http.DefaultTransport, and the test used to
// go red only after that request left the machine for github.com with a
// message ("phases reached: token=0") that did not say why.
type refuseOffFixture struct {
	mu    sync.Mutex
	hosts []string
}

func (r *refuseOffFixture) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.hosts = append(r.hosts, req.URL.Host)
	r.mu.Unlock()
	return nil, fmt.Errorf("test: a request to %s bypassed the fixture", req.URL.Host)
}

func (r *refuseOffFixture) leaked() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.hosts...)
}

// githubCallbackFixture starts the fixture forge, swaps http.DefaultTransport
// for a refusing one for the test's duration (no t.Parallel in this
// package), and returns the request context value that routes oauth2's
// clients to the fixture.
func githubCallbackFixture(t *testing.T, handler http.HandlerFunc) (context.Context, *refuseOffFixture) {
	t.Helper()
	forge := httptest.NewServer(handler)
	t.Cleanup(forge.Close)
	target, err := url.Parse(forge.URL)
	if err != nil {
		t.Fatal(err)
	}
	realTransport := http.DefaultTransport
	refuse := &refuseOffFixture{}
	http.DefaultTransport = refuse
	t.Cleanup(func() { http.DefaultTransport = realTransport })
	client := &http.Client{Transport: rewriteToFixture{target: target, base: realTransport}}
	return context.WithValue(context.Background(), oauth2.HTTPClient, client), refuse
}

func githubCallbackRequest(ctx context.Context) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/auth/github/callback?code=c&state=st", nil)
	req.AddCookie(&http.Cookie{Name: "oauth_state", Value: "st"})
	return req.WithContext(ctx)
}

// TestGitHubCallbackUserReadIsBounded is the GitHub twin of
// TestGitLabCallbackUserReadIsBounded (batch 7c review round 6): the GitHub
// reads rested on a spelling-list pin that a wrapper function, a request
// literal, `http.NewRequest (` with a space (gofmt is not gated) and
// `NewRequestWithContext(context.Background(), …)` all escaped with every
// gate green. The fixture's token endpoint answers and its /user stalls; the
// handler must return at the (shortened) bound, both phases must have been
// reached, and the body must be the user-read failure. The stall's late
// answer carries no login, so an unbounded read ends in the handler's own
// 502 and this test's bound message — never in completeOAuthLogin's nil
// store (round 7: a panic there aborted the package's test binary).
func TestGitHubCallbackUserReadIsBounded(t *testing.T) {
	saved := oauthCallbackTimeout
	oauthCallbackTimeout = 200 * time.Millisecond
	t.Cleanup(func() { oauthCallbackTimeout = saved })

	var tokenHits, userReads atomic.Int32
	ctx, refuse := githubCallbackFixture(t, func(w http.ResponseWriter, r *http.Request) {
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
				fmt.Fprint(w, `{"id":1}`) // no login: refused before the store
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	s := New(nil, config.WebConfig{DevMode: true, GitHubClientID: "id", GitHubClientSecret: "secret"}, nil, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	rec := httptest.NewRecorder()
	start := time.Now()
	s.handleGitHubCallback(rec, githubCallbackRequest(ctx))
	if hosts := refuse.leaked(); len(hosts) > 0 {
		t.Fatalf("a request to %v left through http.DefaultTransport instead of the oauth2 client — a ctx without oauth2.HTTPClient handed to Exchange/Client, or a client other than the one Client returned", hosts)
	}
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

// TestGitHubCallbackEmailsReadIsBounded is the /user/emails read's runtime
// twin (batch 7c review round 7): fetchGitHubPrimaryEmail receives the
// handler's bounded ctx, but a Background ctx at its call site or inside
// its body passed every other test. The emails read is reached only after
// /user answers with an empty email, and the handler then completes the
// login — which needs a store, so this runs in the DB tier. The fixture's
// /user/emails stalls; the login must still complete (302, the email left
// empty for the /account/email prompt) within the bound, and the read must
// have been reached.
func TestGitHubCallbackEmailsReadIsBounded(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	saved := oauthCallbackTimeout
	oauthCallbackTimeout = 200 * time.Millisecond
	t.Cleanup(func() { oauthCallbackTimeout = saved })

	const login = "_avweb_emails_bound"
	bg := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(bg, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	clean := func() {
		if _, err := store.Pool().Exec(bg, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login); err != nil {
			t.Logf("cleanup of the probe user: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)

	var emailsReads atomic.Int32
	ctx, refuse := githubCallbackFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login/oauth/access_token":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"access_token":"tok","token_type":"bearer"}`)
		case "/user":
			fmt.Fprintf(w, `{"id":987654321,"login":%q,"email":""}`, login)
		case "/user/emails":
			emailsReads.Add(1)
			select {
			case <-r.Context().Done(): // the bounded request gave up
			case <-time.After(5 * time.Second):
				fmt.Fprint(w, `[]`)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	s := New(store, config.WebConfig{GitHubClientID: "id", GitHubClientSecret: "secret"}, nil, "", logger)
	rec := httptest.NewRecorder()
	start := time.Now()
	s.handleGitHubCallback(rec, githubCallbackRequest(ctx))
	if hosts := refuse.leaked(); len(hosts) > 0 {
		t.Fatalf("a request to %v left through http.DefaultTransport instead of the oauth2 client — a ctx without oauth2.HTTPClient handed to Exchange/Client, or a client other than the one Client returned", hosts)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the GitHub callback took %s against a stalled /user/emails; want it to return at the %s bound", took, oauthCallbackTimeout)
	}
	if emailsReads.Load() < 1 {
		t.Fatal("the fixture's /user/emails was never read — the fallback the bound covers was not exercised")
	}
	if rec.Code != http.StatusFound {
		t.Errorf("a stalled emails read answered %d %q; want the login to complete (302) with the email left for the prompt", rec.Code, strings.TrimSpace(rec.Body.String()))
	}
}
