// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestSearchCommitByAuthorEmail_Hit(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":3,"items":[{"author":{"login":"ijuma","id":1234}}]}`))
	}))
	login, id, err := client.SearchCommitByAuthorEmail(context.Background(), "ismael@juma.me.uk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if login != "ijuma" || id != 1234 {
		t.Errorf("got (%q,%d), want (ijuma,1234)", login, id)
	}
}

func TestSearchCommitByAuthorEmail_NoHit(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":0,"items":[]}`))
	}))
	login, id, err := client.SearchCommitByAuthorEmail(context.Background(), "nobody@example.com")
	if err != nil || login != "" || id != 0 {
		t.Errorf("no-hit must return (\"\",0,nil); got (%q,%d,%v)", login, id, err)
	}
}

// A commit whose author GitHub couldn't map to an account has author=null.
func TestSearchCommitByAuthorEmail_NullAuthor(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":1,"items":[{"author":null}]}`))
	}))
	login, _, err := client.SearchCommitByAuthorEmail(context.Background(), "x@y.com")
	if err != nil || login != "" {
		t.Errorf("null author must yield (\"\",...); got (%q,%v)", login, err)
	}
}

// Non-email input must short-circuit WITHOUT an API call (no server hit).
func TestSearchCommitByAuthorEmail_NonEmailNoCall(t *testing.T) {
	called := false
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, _ = w.Write([]byte(`{"total_count":0,"items":[]}`))
	}))
	if login, _, _ := client.SearchCommitByAuthorEmail(context.Background(), "not-an-email"); login != "" {
		t.Error("non-email should return empty")
	}
	if called {
		t.Error("non-email input must NOT make an API call")
	}
}

// The request must use the author-email: qualifier with the escaped email.
func TestSearchCommitByAuthorEmail_QueryShape(t *testing.T) {
	var gotQuery string
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"total_count":0,"items":[]}`))
	}))
	_, _, _ = client.SearchCommitByAuthorEmail(context.Background(), "ismael@juma.me.uk")
	if !strings.Contains(gotQuery, "author-email") || !strings.Contains(gotQuery, "juma.me.uk") {
		t.Errorf("query must carry author-email:{escaped email}; got %q", gotQuery)
	}
}

// Live canary (gated): real /search/commits for a known committer email.
// Set AVELOXIS_TEST_NETWORK=1 and AVELOXIS_TEST_GITHUB_TOKEN=<token>.
func TestSearchCommitByAuthorEmailLive(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITHUB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITHUB_TOKEN to run the live commit-search canary")
	}
	lg := slog.Default()
	keys := platform.NewKeyPool([]string{tok}, lg)
	httpClient := platform.NewHTTPClient("https://api.github.com", keys, lg, platform.AuthGitHub)
	c := &Client{http: httpClient, logger: lg}

	login, id, err := c.SearchCommitByAuthorEmail(context.Background(), "ismael@juma.me.uk")
	if err != nil {
		t.Fatalf("live search-commits: %v", err)
	}
	if login != "ijuma" || id == 0 {
		t.Errorf("expected ijuma with a non-zero id; got (%q,%d)", login, id)
	}
}
