// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Copilot round 5 on PR #191 (v0.28.18) — the GitLab twin of the GitHub
// test: zero-since walks bypass the ETag cache (two walks, both yield,
// no If-None-Match); same-since incremental walks keep 304 = nothing new.
func TestFullSnapshotListingsBypassETagIncrementalKeeps304(t *testing.T) {
	var conditional atomic.Int64
	client, _ := newTestClientWithCapture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			conditional.Add(1)
			if r.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/merge_requests"):
			_, _ = w.Write([]byte(`[{"id":11,"iid":1,"title":"mr","state":"opened","updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
		default:
			_, _ = w.Write([]byte(`[{"id":21,"iid":2,"title":"issue","state":"opened","updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
		}
	}))

	countIssues := func(since time.Time) int {
		t.Helper()
		n := 0
		for _, err := range client.ListIssues(t.Context(), "owner", "repo", since) {
			if err != nil {
				t.Fatalf("ListIssues: %v", err)
			}
			n++
		}
		return n
	}
	countMRs := func(since time.Time) int {
		t.Helper()
		n := 0
		for _, err := range client.ListPullRequests(t.Context(), "owner", "repo", since) {
			if err != nil {
				t.Fatalf("ListPullRequests: %v", err)
			}
			n++
		}
		return n
	}
	for i := 1; i <= 2; i++ {
		if n := countIssues(time.Time{}); n != 1 {
			t.Errorf("full-snapshot ListIssues walk %d yielded %d, want 1 (never a 304)", i, n)
		}
		if n := countMRs(time.Time{}); n != 1 {
			t.Errorf("full-snapshot ListPullRequests walk %d yielded %d, want 1 (never a 304)", i, n)
		}
	}
	if c := conditional.Load(); c != 0 {
		t.Errorf("full-snapshot walks must not send If-None-Match, sent %d", c)
	}
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if n := countMRs(since); n != 1 {
		t.Fatalf("first incremental ListPullRequests yielded %d, want 1", n)
	}
	if n := countMRs(since); n != 0 {
		t.Errorf("second incremental ListPullRequests yielded %d, want 0 (304 = nothing new)", n)
	}
	if conditional.Load() == 0 {
		t.Error("the repeat incremental walk must have sent If-None-Match")
	}
}
