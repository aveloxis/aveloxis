// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pass 30 (v0.28.18): a job that fetched a repo's listing pages and then
// FAILED left their ETags cached, so the retry in the same process was
// answered 304 on those pages — zero items, and last_collected advanced
// past a window that was never stored. ForgetETagsWithPrefix drops the
// repo's entries: the next read of a forgotten path is unconditional,
// a path outside the prefix keeps revalidating.
func TestForgetETagsWithPrefixMakesTheNextReadUnconditional(t *testing.T) {
	var mu sync.Mutex
	conditional := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Header.Get("If-None-Match") != "" {
			conditional[r.URL.Path]++
		}
		mu.Unlock()
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"t"}, logger), logger, AuthGitHub)

	prime := func(path string) {
		t.Helper()
		resp, err := c.Get(t.Context(), path)
		if err != nil {
			t.Fatalf("prime %s: %v", path, err)
		}
		resp.Body.Close()
	}
	for _, p := range []string{"/repos/o/r/issues?state=all", "/repos/o/r/pulls?state=all", "/repos/o/other/issues?state=all"} {
		prime(p)
	}
	if n := c.ForgetETagsWithPrefix("/repos/o/r/"); n != 2 {
		t.Fatalf("ForgetETagsWithPrefix dropped %d entries, want 2 (the two under the prefix)", n)
	}
	// Forgotten paths read unconditionally (a 200 body, no If-None-Match);
	// the untouched path still revalidates (304).
	for _, p := range []string{"/repos/o/r/issues?state=all", "/repos/o/r/pulls?state=all"} {
		resp, err := c.Get(t.Context(), p)
		if err != nil {
			t.Fatalf("re-read %s after forgetting: %v", p, err)
		}
		resp.Body.Close()
	}
	if _, err := c.Get(t.Context(), "/repos/o/other/issues?state=all"); err == nil || !strings.Contains(err.Error(), "304") {
		t.Errorf("the path outside the prefix must still revalidate to a 304, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if conditional["/repos/o/r/issues"] != 0 || conditional["/repos/o/r/pulls"] != 0 {
		t.Errorf("forgotten paths must not send If-None-Match on the re-read: %v", conditional)
	}
	if conditional["/repos/o/other/issues"] != 1 {
		t.Errorf("the untouched path must have revalidated once, got %d", conditional["/repos/o/other/issues"])
	}
}

// Copilot round 7 (v0.28.18): the pass-30 ForgetETagsWithPrefix walked
// every cached path under the write lock on EVERY failed job — at the
// 500K cap that is a stall on the lock every Get shares. The cache now
// carries a per-repo index; forgetting a repo touches only its own
// entries. These pins cover the index contract: the prefix key
// function, index consistency across the three cache writers, and the
// one-shape prefix contract (a non-repo prefix forgets nothing, loudly).
func TestETagRepoPrefix(t *testing.T) {
	for in, want := range map[string]string{
		"/repos/o/r/issues?state=all&since=2026-01-01": "/repos/o/r/",
		"/repos/o/r/pulls/12/files":                    "/repos/o/r/",
		"/repos/o/r":                                   "/repos/o/r/", // the repo-root read belongs to its namespace
		"/repos/o/r/":                                  "/repos/o/r/",
		"/projects/o%2Fr/merge_requests?state=all":     "/projects/o%2Fr/",
		"/projects/o%2Fr":                              "/projects/o%2Fr/",
		"/projects/12345/issues":                       "/projects/12345/",
		"/repos/o":                                     "",
		"/repos//r/issues":                             "",
		"/users/x/events":                              "",
		"/orgs/x/repos":                                "",
		"/search/users?q=x":                            "",
		"/rate_limit":                                  "",
		"":                                             "",
	} {
		if got := etagRepoPrefix(in); got != want {
			t.Errorf("etagRepoPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestETagIndexStaysConsistentAcrossTheCacheWriters(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient("http://unused", NewKeyPool([]string{"t"}, logger), logger, AuthGitHub)
	put := func(path string) {
		c.etagMu.Lock()
		c.etagCache[path] = `"v1"`
		c.indexETagLocked(path)
		c.etagMu.Unlock()
	}
	for _, p := range []string{"/repos/o/r/issues?since=a", "/repos/o/r/pulls", "/repos/o/r", "/repos/o/other/issues", "/projects/o%2Fr/merge_requests", "/users/x/events"} {
		put(p)
	}
	if got := len(c.etagIndex["/repos/o/r/"]); got != 3 {
		t.Fatalf("index for /repos/o/r/ holds %d paths, want 3", got)
	}
	if _, ok := c.etagIndex["/users/x/events"]; ok {
		t.Fatalf("a non-repo path must not be indexed: %v", c.etagIndex)
	}
	// forgetETag (the mid-body retry path) keeps the index in step and
	// drops an emptied prefix.
	c.forgetETag("/repos/o/other/issues")
	if _, ok := c.etagIndex["/repos/o/other/"]; ok {
		t.Errorf("forgetETag must drop an emptied prefix from the index")
	}
	if _, ok := c.etagCache["/repos/o/other/issues"]; ok {
		t.Errorf("forgetETag must drop the cache entry")
	}
	// ForgetETagsWithPrefix removes exactly the repo's entries — cache
	// AND index — and leaves every other namespace alone.
	if n := c.ForgetETagsWithPrefix("/repos/o/r/"); n != 3 {
		t.Fatalf("ForgetETagsWithPrefix(/repos/o/r/) = %d, want 3", n)
	}
	if _, ok := c.etagIndex["/repos/o/r/"]; ok {
		t.Errorf("the forgotten prefix must leave the index")
	}
	for _, p := range []string{"/repos/o/r/issues?since=a", "/repos/o/r/pulls", "/repos/o/r"} {
		if _, ok := c.etagCache[p]; ok {
			t.Errorf("%s must be forgotten", p)
		}
	}
	for _, p := range []string{"/projects/o%2Fr/merge_requests", "/users/x/events"} {
		if _, ok := c.etagCache[p]; !ok {
			t.Errorf("%s (another namespace) must survive", p)
		}
	}
	if n := c.ForgetETagsWithPrefix("/projects/o%2Fr/"); n != 1 {
		t.Errorf("GitLab prefix forget = %d, want 1", n)
	}
	// The one-shape contract: a prefix that is not a repo namespace —
	// or is one spelled without its trailing slash — forgets nothing
	// (and logs; a silent 0 would read as "no entries").
	put("/repos/o/r/issues")
	for _, bad := range []string{"/users/", "/repos/o/r", "/repos/o/", "/repos/"} {
		if n := c.ForgetETagsWithPrefix(bad); n != 0 {
			t.Errorf("ForgetETagsWithPrefix(%q) = %d, want 0 (not a repo prefix)", bad, n)
		}
	}
	if _, ok := c.etagCache["/repos/o/r/issues"]; !ok {
		t.Errorf("a malformed prefix must forget nothing")
	}
	// The cap reset drops cache and index together — a stale index over
	// an emptied cache would report phantom forgets.
	c.etagMu.Lock()
	c.resetETagCacheLocked()
	c.etagMu.Unlock()
	if len(c.etagCache) != 0 || len(c.etagIndex) != 0 {
		t.Errorf("reset must clear both maps: cache=%d index=%d", len(c.etagCache), len(c.etagIndex))
	}
}

// Source pins: the forget path must go through the index (never a
// cache walk), and every cache writer must keep the index in step.
func TestForgetETagsWithPrefixNeverWalksTheCache(t *testing.T) {
	src := srctest.Read(t, "internal/platform/httpclient.go")
	forget := srctest.StripGoComments(srctest.FuncBody(t, src, "func (c *HTTPClient) ForgetETagsWithPrefix("))
	if strings.Contains(forget, "range c.etagCache") || strings.Contains(forget, "HasPrefix(") {
		t.Errorf("ForgetETagsWithPrefix must not walk the whole cache (O(entries) under the lock every Get shares); use etagIndex")
	}
	if !strings.Contains(forget, "c.etagIndex[prefix]") {
		t.Errorf("ForgetETagsWithPrefix must read the repo's entries from etagIndex")
	}
	body := srctest.StripGoComments(src)
	w := strings.Index(body, "c.etagCache[path] = etag")
	if w < 0 || !strings.Contains(body[w:w+120], "c.indexETagLocked(path)") {
		t.Errorf("the cache write must index the path right after storing the ETag")
	}
	reset := srctest.StripGoComments(srctest.FuncBody(t, src, "func (c *HTTPClient) resetETagCacheLocked("))
	if !strings.Contains(reset, "c.etagCache = make(") || !strings.Contains(reset, "c.etagIndex = make(") {
		t.Errorf("resetETagCacheLocked must rebuild BOTH the cache and the index (a stale index over an emptied cache reports phantom forgets)")
	}
	if !strings.Contains(srctest.StripGoComments(srctest.FuncBody(t, src, "func (c *HTTPClient) forgetETag(")), "c.unindexETagLocked(path)") {
		t.Errorf("forgetETag must unindex the path")
	}
	get := srctest.StripGoComments(srctest.FuncBody(t, src, "func (c *HTTPClient) Get("))
	if strings.Contains(get, "c.etagCache = make(") {
		t.Errorf("Get must reset the cache through resetETagCacheLocked so the index resets with it")
	}
}

// Pass 34 (v0.28.18): GitHub's Link header spells continuation pages
// under the numeric /repositories/{id}/ alias, so page ≥2 of every
// listing was cached under a key ForgetRepoETags could never reach —
// a failed job's retry re-read page 1 unconditionally and was answered
// 304 from page 2 on (the pass-30 truncation behind the pass-30 fix).
// The paginator now rebases continuations onto the listing's own
// namespace; this walk-fail-forget-walk drives the exact shape.
func TestGitHubContinuationPagesAreForgottenWithTheirListing(t *testing.T) {
	var mu sync.Mutex
	conditional := map[string]int{}
	var srvURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Header.Get("If-None-Match") != "" {
			conditional[r.URL.Path+"?page="+r.URL.Query().Get("page")]++
		}
		mu.Unlock()
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "" || r.URL.Query().Get("page") == "1" {
			// The documented continuation form: the numeric alias.
			w.Header().Set("Link", "<"+srvURL+"/repositories/1300192/issues?per_page=100&page=2>; rel=\"next\"")
			_, _ = w.Write([]byte(`[{"n":1}]`))
			return
		}
		_, _ = w.Write([]byte(`[{"n":2}]`))
	}))
	defer srv.Close()
	srvURL = srv.URL
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"t"}, logger), logger, AuthGitHub)

	walk := func() int {
		t.Helper()
		n := 0
		for _, err := range PaginateGitHub[map[string]any](t.Context(), c, "/repos/o/r/issues?state=all") {
			if err != nil {
				t.Fatalf("walk: %v", err)
			}
			n++
		}
		return n
	}
	if got := walk(); got != 2 {
		t.Fatalf("first walk yielded %d items, want 2 (two pages)", got)
	}
	// The failed-job hook: both pages must be under the repo namespace.
	if n := c.ForgetETagsWithPrefix("/repos/o/r/"); n != 2 {
		t.Fatalf("ForgetETagsWithPrefix forgot %d entries, want 2 — the continuation page is cached outside the listing's namespace", n)
	}
	if got := walk(); got != 2 {
		t.Fatalf("retry walk yielded %d items, want 2 — page 2 was answered 304 from a stale key", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for k, v := range conditional {
		if v != 0 {
			t.Errorf("the retry must re-read every page unconditionally; %s sent If-None-Match %d time(s)", k, v)
		}
	}
}

func TestRebaseContinuation(t *testing.T) {
	for _, tc := range []struct{ next, base, want string }{
		{"/repositories/1300192/issues?per_page=100&page=2", "/repos/o/r/issues?state=all&per_page=100", "/repos/o/r/issues?per_page=100&page=2"},
		{"/repositories/7/pulls/12/files?page=3", "/repos/o/r/pulls/12/files?per_page=100", "/repos/o/r/pulls/12/files?page=3"},
		{"/repos/o/r/issues?page=2", "/repos/o/r/issues", "/repos/o/r/issues?page=2"},         // already namespaced
		{"/repositories/7/issues?page=2", "/users/x/events", "/repositories/7/issues?page=2"}, // not a repo listing
		{"/repositories/7?page=2", "/repos/o/r/issues", "/repositories/7?page=2"},             // no resource after the id
		{"/orgs/x/repos?page=2", "/repos/o/r/issues", "/orgs/x/repos?page=2"},                 // not the alias
		{"", "/repos/o/r/issues", ""},
	} {
		if got := rebaseContinuation(tc.next, tc.base); got != tc.want {
			t.Errorf("rebaseContinuation(%q, %q) = %q, want %q", tc.next, tc.base, got, tc.want)
		}
	}
}
