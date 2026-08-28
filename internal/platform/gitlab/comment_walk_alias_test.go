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

// Pass 30 (v0.28.18) — the v0.26.3 two-pass alias on the GitLab side: the
// three comment walks re-list issues / MRs on the byte-identical URL the
// listing phase just walked with the same since. With the listing's ETag
// cached, the driver read was 304'd and the walk yielded nothing — zero
// issue notes and zero MR notes/review comments on every INCREMENTAL
// GitLab cycle. Red-proof shape: list first (caches the ETag), then walk
// comments with the SAME since against an If-None-Match-honoring mock;
// the per-item endpoints must be reached.
func TestCommentWalksReachItemsAfterTheListingCachedTheETag(t *testing.T) {
	var issueNotesHits, mrNotesHits, discussionHits atomic.Int64
	client, _ := newTestClientWithCapture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/issues/7/notes"):
			issueNotesHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":501,"body":"a note","system":false,"created_at":"2026-08-01T00:00:00Z","author":{"id":9,"username":"u"}}]`))
			return
		case strings.HasSuffix(path, "/merge_requests/3/notes"):
			mrNotesHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":601,"body":"an mr note","system":false,"created_at":"2026-08-01T00:00:00Z","author":{"id":9,"username":"u"}}]`))
			return
		case strings.HasSuffix(path, "/merge_requests/3/discussions"):
			discussionHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
			return
		}
		// The listing endpoints honor If-None-Match like gitlab.com does.
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(path, "/merge_requests"):
			_, _ = w.Write([]byte(`[{"id":31,"iid":3,"title":"mr","state":"opened","user_notes_count":1,"updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
		default:
			_, _ = w.Write([]byte(`[{"id":71,"iid":7,"title":"issue","state":"opened","user_notes_count":1,"updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
		}
	}))
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) // incremental — the fleet's steady state

	// The listing phase walks first and caches the page ETag.
	for _, err := range client.ListIssues(t.Context(), "owner", "repo", since) {
		if err != nil {
			t.Fatalf("ListIssues: %v", err)
		}
	}
	for _, err := range client.ListPullRequests(t.Context(), "owner", "repo", since) {
		if err != nil {
			t.Fatalf("ListPullRequests: %v", err)
		}
	}

	// The comment walks with the SAME since must still reach the items.
	notes := 0
	for _, err := range client.ListIssueComments(t.Context(), "owner", "repo", since) {
		if err != nil {
			t.Fatalf("ListIssueComments: %v", err)
		}
		notes++
	}
	if notes != 1 || issueNotesHits.Load() == 0 {
		t.Errorf("issue notes after the listing cached the ETag: yielded %d, notes endpoint hits %d — the driver listing was answered 304 (the two-pass alias)", notes, issueNotesHits.Load())
	}
	for _, err := range client.ListPRComments(t.Context(), "owner", "repo", since) {
		if err != nil {
			t.Fatalf("ListPRComments: %v", err)
		}
	}
	if mrNotesHits.Load() == 0 {
		t.Error("MR notes walk never reached /merge_requests/3/notes — its driver listing was answered 304")
	}
	for _, err := range client.ListReviewComments(t.Context(), "owner", "repo", since) {
		if err != nil {
			t.Fatalf("ListReviewComments: %v", err)
		}
	}
	if discussionHits.Load() == 0 {
		t.Error("review-comment walk never reached /merge_requests/3/discussions — its driver listing was answered 304")
	}
}
