// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// D4 (0.29.123, summary/63 §9): the administrator's view of the api's
// caches — what they hold, how they are used, and, per repository, whether
// each held answer is current. Read-only (SR-7).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func cacheViewCall(t *testing.T, s *Server, h http.HandlerFunc, path string, info *authInfo) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	if info != nil {
		r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, *info))
	}
	if strings.Contains(path, "/repos/") {
		r.SetPathValue("repoID", strings.Split(strings.SplitN(path, "/repos/", 2)[1], "?")[0])
	}
	w := httptest.NewRecorder()
	h(w, r)
	var body map[string]any
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s: %v (%s)", path, err, w.Body.String())
		}
	}
	return w, body
}

func num(t *testing.T, m map[string]any, path ...string) float64 {
	t.Helper()
	var cur any = m
	for _, p := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("%v: not an object at %q", path, p)
		}
		cur = mm[p]
	}
	f, ok := cur.(float64)
	if !ok {
		t.Fatalf("%v = %v, not a number", path, cur)
	}
	return f
}

// The counters move with what happened: a miss stores, a repeat hits, a
// client holding the tag revalidates, and the budget evicts.
func TestCacheViewCountsWhatTheCacheDid(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	admin := &authInfo{UserID: 1, IsAdmin: true}
	etag := hn.get("/api/v1/repos/7/thing?a=1").Header().Get("ETag") // miss, stored
	hn.get("/api/v1/repos/7/thing?a=1")                              // hit
	hn.get("/api/v1/repos/7/thing?a=1", ifNoneMatch(etag))           // revalidated (304)
	hn.get("/api/v1/repos/8/thing?a=1")                              // miss, stored
	_, v := cacheViewCall(t, hn.s, hn.s.handleAdminCache, "/api/v1/admin/cache", admin)
	for name, want := range map[string]float64{"misses": 2, "hits": 1, "revalidated": 1, "stores": 2, "evictions": 0, "entries": 2, "repositories": 2} {
		if got := num(t, v, "page_cache", name); got != want {
			t.Errorf("page_cache.%s = %v, want %v", name, got, want)
		}
	}
	if num(t, v, "page_cache", "bytes") <= 0 || num(t, v, "page_cache", "max_bytes") != 1<<20 {
		t.Errorf("bytes %v of max_bytes %v", num(t, v, "page_cache", "bytes"), num(t, v, "page_cache", "max_bytes"))
	}
	// A budget of one answer: the second store evicts the first.
	small := newPageCacheHarness(t, pageExact, 0, okBody)
	small.s.pageCache = newRepoPageCache(1)
	small.s.pageCache.maxBytes = int64(len(`{"run":1,"q":"a=1"}`)) + 200
	small.get("/api/v1/repos/7/thing?a=1")
	small.get("/api/v1/repos/7/thing?a=2")
	if _, v := cacheViewCall(t, small.s, small.s.handleAdminCache, "/api/v1/admin/cache", admin); num(t, v, "page_cache", "evictions") != 1 {
		t.Errorf("evictions = %v, want 1 (the budget held one answer)", num(t, v, "page_cache", "evictions"))
	}
}

// Per repository: each held answer and whether it is current — or why not:
// the repository changed, or its window moved (D6) — and its saved
// requests not yet replayed.
func TestCacheViewRepositoryShowsWhyAnAnswerIsNotCurrent(t *testing.T) {
	hn, _, _ := rewarmHarness(t, okBody)
	admin := &authInfo{UserID: 1, IsAdmin: true}
	setTestPageWindow(t, "week-1", true)
	hn.get("/api/v1/repos/7/thing?a=1")
	_, v := cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepo, "/api/v1/admin/cache/repos/7", admin)
	answers, _ := v["answers"].([]any)
	if len(answers) != 1 || answers[0].(map[string]any)["status"] != "current" {
		t.Fatalf("answers = %v, want one current", v["answers"])
	}
	setTestPageWindow(t, "week-2", true)
	_, v = cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepo, "/api/v1/admin/cache/repos/7", admin)
	if got := v["answers"].([]any)[0].(map[string]any)["status"]; got != "outdated: its window moved" {
		t.Errorf("status %v, want outdated: its window moved", got)
	}
	hn.setState(func(st *db.RepoCacheState) { st.LastCollected = st.LastCollected.Add(time.Hour) })
	_, v = cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepo, "/api/v1/admin/cache/repos/7", admin)
	if got := v["answers"].([]any)[0].(map[string]any)["status"]; got != "outdated: the repository changed" {
		t.Errorf("status %v, want outdated: the repository changed", got)
	}
	hn.s.pageCache.addSeed(7, "/api/v1/repos/7/thing?a=9", time.Second)
	_, v = cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepo, "/api/v1/admin/cache/repos/7", admin)
	if seeds, _ := v["saved_requests"].([]any); len(seeds) != 1 {
		t.Errorf("saved_requests = %v, want the one seed", v["saved_requests"])
	}
	if w, _ := cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepo, "/api/v1/admin/cache/repos/x", admin); w.Code != http.StatusBadRequest {
		t.Errorf("a repository id that is not a number: %d, want 400", w.Code)
	}
	hn.mu.Lock()
	hn.known = false
	hn.mu.Unlock()
	if w, _ := cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepo, "/api/v1/admin/cache/repos/7", admin); w.Code != http.StatusNotFound {
		t.Errorf("a repository nobody has: %d, want the repository routes' 404", w.Code)
	}
}

// The fleet listing: one row per repository with held answers, sorted by an
// allowlisted key (an unknown one falls back, and the answer echoes the
// effective key), at most the page size.
func TestCacheViewListsRepositoriesBySize(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	admin := &authInfo{UserID: 1, IsAdmin: true}
	hn.get("/api/v1/repos/7/thing?a=1")
	hn.get("/api/v1/repos/8/thing?a=1")
	hn.get("/api/v1/repos/8/thing?a=2")
	hn.get("/api/v1/repos/8/thing?a=2")
	_, v := cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepos, "/api/v1/admin/cache/repos?sort=entries", admin)
	rows, _ := v["repositories"].([]any)
	if len(rows) != 2 || num(t, rows[0].(map[string]any), "repo_id") != 8 || num(t, rows[0].(map[string]any), "entries") != 2 {
		t.Fatalf("repositories = %v, want repo 8 (two answers) first", v["repositories"])
	}
	if v["sort"] != "entries" {
		t.Errorf("sort = %v, want the effective key", v["sort"])
	}
	_, v = cacheViewCall(t, hn.s, hn.s.handleAdminCacheRepos, "/api/v1/admin/cache/repos?sort=drop%20table", admin)
	if v["sort"] != cacheRepoSortDefault {
		t.Errorf("an unknown sort key must fall back to %q and say so, got %v", cacheRepoSortDefault, v["sort"])
	}
}

// Administrators only, a session (an API token never administers), and
// never stored by anyone.
func TestCacheViewIsForAdministrators(t *testing.T) {
	hn := newPageCacheHarness(t, pageExact, 1<<20, okBody)
	for name, h := range map[string]http.HandlerFunc{"summary": hn.s.handleAdminCache, "list": hn.s.handleAdminCacheRepos, "repo": hn.s.handleAdminCacheRepo} {
		for who, info := range map[string]*authInfo{
			"a user":           {UserID: 2},
			"an admin's token": {UserID: 1, APITokenID: 9},
		} {
			path := "/api/v1/admin/cache/repos/7"
			if w, _ := cacheViewCall(t, hn.s, h, path, info); w.Code != http.StatusForbidden {
				t.Errorf("%s for %s: %d, want 403", name, who, w.Code)
			}
		}
		w, _ := cacheViewCall(t, hn.s, h, "/api/v1/admin/cache/repos/7", &authInfo{UserID: 1, IsAdmin: true})
		if w.Code != http.StatusOK || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
			t.Errorf("%s for an admin: %d, Cache-Control %q (want 200, no-store)", name, w.Code, w.Header().Get("Cache-Control"))
		}
	}
}
