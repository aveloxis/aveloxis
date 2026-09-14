// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.29.12 — redirects never carry a key off the client's own host. Through
// v0.29.11, HTTPClient.Get followed an absolute Location to ANY host and
// scheme and re-set Authorization / PRIVATE-TOKEN on the next attempt, so a
// redirect issued by the forge sent the token to the redirect target
// (including over plain http). Production evidence before the fix
// (chaoss.tv, 2026-09-06..13): 3,060 redirects followed, every one
// api.github.com → https://api.github.com — refusing everything else costs
// nothing. The same code joined a relative Location onto the base URL, so a
// GitLab base ending in /api/v4 doubled the path.

package platform

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedirectTarget(t *testing.T) {
	const gh = "https://api.github.com"
	for _, tc := range []struct {
		name, base, current, location, want string
		refused                             bool
	}{
		{"same host, absolute", gh, gh + "/repos/a/b", gh + "/repositories/123", gh + "/repositories/123", false},
		{"host compared case-insensitively", gh, gh + "/repos/a/b", "https://API.GitHub.com/repositories/1", "https://API.GitHub.com/repositories/1", false},
		{"absolute-path reference under a GitLab /api/v4 base", "https://gitlab.com/api/v4", "https://gitlab.com/api/v4/projects/1", "/api/v4/projects/9", "https://gitlab.com/api/v4/projects/9", false},
		{"relative-path reference", "https://gitlab.com/api/v4", "https://gitlab.com/api/v4/projects/1", "9", "https://gitlab.com/api/v4/projects/9", false},
		{"query preserved", gh, gh + "/repos/a/b/issues?page=2", "/repositories/5/issues?page=2", gh + "/repositories/5/issues?page=2", false},
		{"another host", gh, gh + "/repos/a/b", "https://evil.example/x", "", true},
		{"a subdomain is another host", gh, gh + "/repos/a/b", "https://objects.api.github.com/x", "", true},
		{"scheme downgrade on the same host", gh, gh + "/repos/a/b", "http://api.github.com/repositories/1", "", true},
		{"protocol-relative to another host", gh, gh + "/repos/a/b", "//evil.example/x", "", true},
		{"userinfo trick", gh, gh + "/repos/a/b", "https://api.github.com@evil.example/x", "", true},
		{"explicit default port is a different authority", gh, gh + "/repos/a/b", "https://api.github.com:443/x", "", true},
		{"unparseable Location", gh, gh + "/repos/a/b", "https://api.github.com/%zz", "", false},
		{"plain-http test base keeps its own scheme and host", "http://127.0.0.1:8080", "http://127.0.0.1:8080/old", "/new", "http://127.0.0.1:8080/new", false},
		{"plain-http base, another port", "http://127.0.0.1:8080", "http://127.0.0.1:8080/old", "http://127.0.0.1:9090/new", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := redirectTarget(tc.base, tc.current, tc.location)
			if tc.name == "unparseable Location" {
				// Unfollowable, not hostile: ErrGone, never ErrOffHostRefused.
				if !errors.Is(err, ErrGone) || errors.Is(err, ErrOffHostRefused) {
					t.Fatalf("redirectTarget(%q) = (%q, %v), want ErrGone", tc.location, got, err)
				}
				return
			}
			if tc.refused {
				if !errors.Is(err, ErrOffHostRefused) {
					t.Fatalf("redirectTarget(%q) = (%q, %v), want ErrOffHostRefused", tc.location, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Errorf("redirectTarget(%q) = (%q, %v), want (%q, nil)", tc.location, got, err, tc.want)
			}
		})
	}
}

func TestErrOffHostRefusedIsASkip(t *testing.T) {
	if got := ClassifyError(ErrOffHostRefused); got != ClassSkip {
		t.Errorf("ClassifyError(ErrOffHostRefused) = %v, want ClassSkip — the endpoint is skipped, the collection continues", got)
	}
}

// Behavior, both auth styles: a redirect to another host is not followed —
// the other host receives nothing, the token is never sent there, the
// permanent-redirect hook (which mutates durable repo state) does not fire,
// and the refusal is logged at ERROR.
func TestGetRefusesCrossHostRedirect(t *testing.T) {
	for _, style := range []AuthStyle{AuthGitHub, AuthGitLab} {
		t.Run(map[AuthStyle]string{AuthGitHub: "github", AuthGitLab: "gitlab"}[style], func(t *testing.T) {
			var foreignHits atomic.Int32
			var foreignAuth sync.Map
			foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				foreignHits.Add(1)
				foreignAuth.Store(r.Header.Get("Authorization")+r.Header.Get("PRIVATE-TOKEN"), true)
				_, _ = io.WriteString(w, `{"ok":true}`)
			}))
			defer foreign.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", foreign.URL+"/stolen")
				w.WriteHeader(http.StatusMovedPermanently)
			}))
			defer origin.Close()

			var logBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logBuf, nil))
			client := NewHTTPClient(origin.URL, NewKeyPool([]string{"secret-token"}, logger), logger, style)
			hookFired := false
			client.OnPermanentRedirect(func(from, to string) { hookFired = true })

			resp, err := client.Get(context.Background(), "/repos/a/b")
			if resp != nil {
				resp.Body.Close()
			}
			if !errors.Is(err, ErrOffHostRefused) {
				t.Fatalf("err = %v, want ErrOffHostRefused", err)
			}
			if n := foreignHits.Load(); n != 0 {
				t.Fatalf("the other host received %d request(s) — a redirect carried the client to a host its keys do not belong to", n)
			}
			if hookFired {
				t.Error("the permanent-redirect hook fired for a refused redirect — it would record a foreign URL as the repo's new location")
			}
			if !strings.Contains(logBuf.String(), "level=ERROR") || !strings.Contains(logBuf.String(), "redirect refused") {
				t.Errorf("a refused redirect must be logged at ERROR naming the refusal; log:\n%s", logBuf.String())
			}
			if strings.Contains(logBuf.String(), "secret-token") {
				t.Error("the token appeared in the log")
			}
		})
	}
}

// A relative Location resolves against the URL that was requested, not by
// concatenation onto the base: under a GitLab base ending in /api/v4 the old
// join doubled the path and the follow 404'd.
func TestGetRelativeRedirectResolvesAgainstRequestedURL(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v4/projects/1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/api/v4/projects/9")
		w.WriteHeader(http.StatusMovedPermanently)
	})
	var targetHits atomic.Int32
	mux.HandleFunc("/api/v4/projects/9", func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		_, _ = io.WriteString(w, `{"id":9}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewHTTPClient(server.URL+"/api/v4", NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitLab)
	resp, err := client.Get(context.Background(), "/projects/1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	if targetHits.Load() != 1 {
		t.Errorf("target hit %d times, want 1 — the relative Location was not resolved against the requested URL", targetHits.Load())
	}
}

// Review of v0.29.12: redirects were not the only way a keyed request could
// leave the host. The layer that attaches the key enforces the rule for every
// request it sends, whatever built the URL.

// A request path that turns the client's URL into another host (a path with
// no leading slash is joined straight onto the base) is refused before any key
// is leased or any byte is sent. On this test server's base (which has a
// port) only the userinfo shape parses into another host; the dot and bare
// shapes change the host on a portless base — TestGetRefusesPathThatChangesTheHost.
func TestGetRefusesOffHostRequestURL(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	client := NewHTTPClient(srv.URL, NewKeyPool([]string{"secret-token"}, silentLogger()), silentLogger(), AuthGitHub)
	for _, path := range []string{"@evil.example/x?page=2"} {
		resp, err := client.Get(context.Background(), path)
		if resp != nil {
			resp.Body.Close()
		}
		if !errors.Is(err, ErrOffHostRefused) {
			t.Errorf("Get(%q): err = %v, want ErrOffHostRefused", path, err)
		}
	}
	if hits.Load() != 0 {
		t.Errorf("the origin received %d request(s) for refused URLs", hits.Load())
	}
}

// The reviewer's end-to-end leak: page 1 of a GitHub listing carries a Link
// continuation whose target is another host in an opaque form
// (`<http:@FOREIGN/x?page=2>`). Pre-fix the path was joined onto the base
// and the foreign host received the token. Now the listing stops with
// ErrOffHostRefused and the foreign host receives nothing.
func TestPaginateRefusesOffHostLinkContinuation(t *testing.T) {
	var foreignHits atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignHits.Add(1)
		_, _ = io.WriteString(w, `[]`)
	}))
	defer foreign.Close()
	foreignHost := strings.TrimPrefix(foreign.URL, "http://")
	for _, link := range []string{
		"<http:@" + foreignHost + "/x?page=2>; rel=\"next\"",
		"<" + foreign.URL + "/repos/o/r/issues?page=2>; rel=\"next\"",
	} {
		// Only page 1 carries the continuation, so a checker that failed to
		// refuse it ends the walk (wrong result, crisp failure) instead of
		// looping back onto the origin forever.
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("page") == "" {
				w.Header().Set("Link", link)
				_, _ = io.WriteString(w, `[{"id":1}]`)
				return
			}
			_, _ = io.WriteString(w, `[]`)
		}))
		client := NewHTTPClient(origin.URL, NewKeyPool([]string{"secret-token"}, silentLogger()), silentLogger(), AuthGitHub)
		var gotErr error
		items := 0
		for _, err := range PaginateGitHub[map[string]any](context.Background(), client, "/repos/o/r/issues") {
			if err != nil {
				gotErr = err
				break
			}
			items++
		}
		origin.Close()
		if !errors.Is(gotErr, ErrOffHostRefused) || !errors.Is(gotErr, ErrListingTruncated) {
			t.Errorf("Link %s: iteration error = %v, want ErrListingTruncated wrapping ErrOffHostRefused (a silent stop would truncate the listing unnoticed)", link, gotErr)
		}
		// A refusal after page 1 leaves the listing incomplete. As a skip, the
		// job would go green and last_collected would advance past the pages
		// never listed (review of this change) — it must fail instead.
		if c := ClassifyError(gotErr); c == ClassSkip {
			t.Errorf("Link %s: a mid-listing refusal classified %v — the endpoint must fail so the window is re-listed, not be skipped", link, c)
		}
		if items != 1 {
			t.Errorf("Link %s: yielded %d items, want the 1 item of page 1", link, items)
		}
	}
	if foreignHits.Load() != 0 {
		t.Fatalf("the foreign host received %d request(s) — a Link continuation carried the key off the host", foreignHits.Load())
	}
}

// A GitLab Link continuation is absolute and includes /api/v4; pre-fix it was
// returned as "/api/v4/projects/…" and joined onto a base already ending in
// /api/v4, so page 2 was requested at /api/v4/api/v4/… and 404'd.
func TestPaginateGitLabLinkContinuationStaysUnderAPIBase(t *testing.T) {
	var srv *httptest.Server
	var page2Hits atomic.Int32
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v4/projects/1/issues" && r.URL.Query().Get("page") == "2":
			page2Hits.Add(1)
			_, _ = io.WriteString(w, `[{"id":2}]`)
		case r.URL.Path == "/api/v4/projects/1/issues":
			w.Header().Set("Link", "<"+srv.URL+"/api/v4/projects/1/issues?page=2&per_page=100>; rel=\"next\"")
			_, _ = io.WriteString(w, `[{"id":1}]`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	client := NewHTTPClient(srv.URL+"/api/v4", NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitLab)
	items := 0
	for _, err := range PaginateGitLab[map[string]any](context.Background(), client, "/projects/1/issues") {
		if err != nil {
			t.Fatalf("iteration: %v", err)
		}
		items++
	}
	if page2Hits.Load() != 1 || items != 2 {
		t.Errorf("page 2 hits = %d, items = %d — want 1 and 2 (the Link continuation must stay under /api/v4, not double it)", page2Hits.Load(), items)
	}
}

// GraphQLAt takes an explicit endpoint; a keyed POST to another host is
// refused before the loop leases a key.
func TestGraphQLAtRefusesOffHostEndpoint(t *testing.T) {
	var hits atomic.Int32
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `{"data":{}}`)
	}))
	defer foreign.Close()
	client := NewHTTPClient("https://gitlab.com/api/v4", NewKeyPool([]string{"secret-token"}, silentLogger()), silentLogger(), AuthGitLab)
	var dest struct{}
	err := client.GraphQLAt(context.Background(), foreign.URL+"/api/graphql", "query{x}", nil, &dest)
	if !errors.Is(err, ErrOffHostRefused) {
		t.Errorf("err = %v, want ErrOffHostRefused", err)
	}
	if hits.Load() != 0 {
		t.Errorf("the foreign GraphQL endpoint received %d request(s)", hits.Load())
	}
}

// A redirect refused on page 2 of a listing truncates it exactly like a
// refused Link continuation, and must fail the same way (not a skip).
func TestPaginateMidListingRedirectRefusalIsNotASkip(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "" {
			w.Header().Set("Link", "<"+srv.URL+"/repos/o/r/issues?page=2>; rel=\"next\"")
			_, _ = io.WriteString(w, `[{"id":1}]`)
			return
		}
		w.Header().Set("Location", "https://evil.example/stolen")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer srv.Close()
	client := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	var gotErr error
	for _, err := range PaginateGitHub[map[string]any](context.Background(), client, "/repos/o/r/issues") {
		if err != nil {
			gotErr = err
			break
		}
	}
	if !errors.Is(gotErr, ErrListingTruncated) || ClassifyError(gotErr) == ClassSkip {
		t.Errorf("err = %v (class %v), want ErrListingTruncated and not ClassSkip", gotErr, ClassifyError(gotErr))
	}
}

// A request URL that does not parse is not an off-host request: nothing can
// be sent, no key is at risk, and the operator must not be told a host was
// hijacked (review of this change: a GitHub contents name like `100%-cover`
// produced "off-host request refused" at ERROR). It keeps its pre-v0.29.12
// failure — the request-construction error — and is not ClassSkip'd as
// ErrOffHostRefused. Same for a base URL with no scheme.
func TestMalformedRequestURLIsNotReportedAsOffHost(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	for _, tc := range []struct{ name, base, path string }{
		{"invalid escape in a contents path", srv.URL, "/repos/o/r/contents/100%-cover/package.json"},
		{"base URL without a scheme", "gitlab.example.com/api/v4", "/projects/1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logBuf, nil))
			client := NewHTTPClient(tc.base, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resp, err := client.Get(ctx, tc.path)
			if resp != nil {
				resp.Body.Close()
			}
			if err == nil {
				t.Fatal("a malformed request URL must fail")
			}
			if errors.Is(err, ErrOffHostRefused) {
				t.Errorf("err = %v — a malformed URL is not an off-host request", err)
			}
			if strings.Contains(logBuf.String(), "off-host") {
				t.Errorf("a malformed URL was logged as an off-host refusal:\n%s", logBuf.String())
			}
		})
	}
	if hits.Load() != 0 {
		t.Errorf("the server received %d request(s)", hits.Load())
	}
}

// The host comparison itself, through Get, on a base WITHOUT a port: there a
// path with no leading slash really does turn the URL into another host
// (`http://origin.test` + `.evil.example/x` → host origin.test.evil.example).
// The client's transport dials the test listener for EVERY host, so an
// unguarded Get would deliver the request; the guard must stop it.
func TestGetRefusesPathThatChangesTheHost(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	listener := strings.TrimPrefix(srv.URL, "http://")
	client := NewHTTPClient("http://origin.test", NewKeyPool([]string{"secret-token"}, silentLogger()), silentLogger(), AuthGitHub)
	client.inner.Transport = &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, listener)
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Sanity: the rigged transport really reaches the listener for the base host.
	resp, err := client.Get(ctx, "/ok")
	if err != nil {
		t.Fatalf("base-host request through the rigged transport: %v", err)
	}
	resp.Body.Close()
	before := hits.Load()
	for _, path := range []string{".evil.example/x", "evil.example/x"} {
		resp, err := client.Get(ctx, path)
		if resp != nil {
			resp.Body.Close()
		}
		if !errors.Is(err, ErrOffHostRefused) {
			t.Errorf("Get(%q): err = %v, want ErrOffHostRefused", path, err)
		}
	}
	if got := hits.Load() - before; got != 0 {
		t.Errorf("%d request(s) for another host reached the listener", got)
	}
}

// The other side of the truncation rule: a refusal on the FIRST request of a
// listing truncates nothing (no page was yielded), so it stays ClassSkip — the
// endpoint is skipped, the job is not failed (review round 3: dropping the
// page ≥ 2 condition left every test green).
func TestPaginateFirstPageRefusalStaysASkip(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(c *HTTPClient) error
	}{
		{"github", func(c *HTTPClient) error {
			for _, err := range PaginateGitHub[map[string]any](context.Background(), c, "/repos/o/r/issues?state=all") {
				if err != nil {
					return err
				}
			}
			return nil
		}},
		{"gitlab", func(c *HTTPClient) error {
			for _, err := range PaginateGitLab[map[string]any](context.Background(), c, "/projects/1/issues") {
				if err != nil {
					return err
				}
			}
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "https://evil.example/stolen")
				w.WriteHeader(http.StatusMovedPermanently)
			}))
			defer srv.Close()
			client := NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
			err := tc.run(client)
			if !errors.Is(err, ErrOffHostRefused) || errors.Is(err, ErrListingTruncated) {
				t.Fatalf("err = %v, want ErrOffHostRefused without ErrListingTruncated", err)
			}
			if c := ClassifyError(err); c != ClassSkip {
				t.Errorf("a first-page refusal classified %v, want ClassSkip", c)
			}
		})
	}
}
