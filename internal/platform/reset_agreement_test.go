// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// TestEndpointTemplate — the reset-agreement counts are keyed by endpoint,
// so the template must have bounded cardinality: owner, repo, login,
// numbers and SHAs are placeholders, and only the resource segment is kept.
func TestEndpointTemplate(t *testing.T) {
	for path, want := range map[string]string{
		"/repos/chaoss/augur/pulls/123/files":         "/repos/{owner}/{repo}/pulls/…",
		"/repos/chaoss/augur/commits/abcdef1234567":   "/repos/{owner}/{repo}/commits/…",
		"/repos/chaoss/augur":                         "/repos/{owner}/{repo}",
		"/repos/chaoss/augur/issues":                  "/repos/{owner}/{repo}/issues",
		"/users/octocat":                              "/users/{login}",
		"/users/octocat/repos":                        "/users/{login}/repos",
		"/orgs/chaoss/repos":                          "/orgs/{org}/repos",
		"/search/users":                               "/search/users",
		"/graphql":                                    "/graphql",
		"/rate_limit":                                 "/rate_limit",
		"/api/v4/projects/123/merge_requests/5/notes": "/api/v4/projects/{id}/merge_requests/…",
		"": "/",
	} {
		if got := endpointTemplate(path); got != want {
			t.Errorf("endpointTemplate(%q) = %q; want %q", path, got, want)
		}
	}
}

// TestResetAgreementCounts — Phase 0 (worklist items 27/81, observation
// only): whether a response's rate-limit reset agrees with the window the
// pool tracks decides the tracking model (item 28), and nothing counted it.
// Each response is classified per bucket and endpoint before the window
// guard updates the tracked window; the counts drain on read.
func TestResetAgreementCounts(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	kp.RecordResetAgreement()
	key := kp.keys[0]
	base := time.Now().Add(30 * time.Minute).Truncate(time.Second)
	resp := func(path, resource string, reset time.Time) *http.Response {
		h := http.Header{}
		h.Set("X-RateLimit-Resource", resource)
		h.Set("X-RateLimit-Remaining", "100")
		if !reset.IsZero() {
			h.Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
		}
		return &http.Response{StatusCode: 200, Header: h, Request: &http.Request{URL: &url.URL{Path: path}}}
	}
	kp.mu.Lock()
	key.ResetAt = time.Time{}
	kp.mu.Unlock()
	kp.UpdateFromResponse(key, resp("/repos/o/r/pulls/1", "core", base))                  // untracked
	kp.UpdateFromResponse(key, resp("/repos/o/r/pulls/2", "core", base))                  // equal
	kp.UpdateFromResponse(key, resp("/repos/o/r/issues", "core", base.Add(-time.Minute))) // earlier
	kp.UpdateFromResponse(key, resp("/repos/o/r/pulls/3", "core", base.Add(time.Hour)))   // later
	kp.UpdateFromResponse(key, resp("/graphql", "graphql", base))                         // untracked (graphql)
	got := kp.DrainResetAgreement()
	want := map[ResetAgreementKey]ResetAgreementCounts{
		{Bucket: "core", Endpoint: "/repos/{owner}/{repo}/pulls/…"}: {Untracked: 1, Equal: 1, Later: 1},
		{Bucket: "core", Endpoint: "/repos/{owner}/{repo}/issues"}:  {Earlier: 1},
		{Bucket: "graphql", Endpoint: "/graphql"}:                   {Untracked: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d: %+v", len(got), len(want), got)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%+v = %+v; want %+v", k, got[k], w)
		}
	}
	if again := kp.DrainResetAgreement(); len(again) != 0 {
		t.Errorf("a drain must reset the counts, got %+v", again)
	}
	// The per-key snapshot carries the tracked windows (Phase 0 item 2).
	snap, _ := kp.Snapshot()
	if !snap[0].CoreResetAt.Equal(base.Add(time.Hour)) || !snap[0].GraphQLResetAt.Equal(base) {
		t.Errorf("snapshot windows core=%v graphql=%v", snap[0].CoreResetAt, snap[0].GraphQLResetAt)
	}
}

// TestResetAgreementIsOptIn — review round 1 F2: only the pool whose
// summary drains the counts records them. A pool that never opted in (the
// GitLab pool, which nothing drains) keeps nothing, however many
// responses it sees — before, its map grew for the life of the process.
func TestResetAgreementIsOptIn(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	key := kp.keys[0]
	h := http.Header{}
	h.Set("RateLimit-Remaining", "100")
	h.Set("RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
	for i := 0; i < 3; i++ {
		kp.UpdateFromResponse(key, &http.Response{StatusCode: 200, Header: h,
			Request: &http.Request{URL: &url.URL{Path: "/api/v4/projects/g/sub" + strconv.Itoa(i) + "/p/issues"}}})
	}
	kp.mu.Lock()
	n := len(kp.resetAgreement)
	kp.mu.Unlock()
	if n != 0 {
		t.Errorf("a pool that did not opt in recorded %d reset-agreement keys, want 0", n)
	}
}
