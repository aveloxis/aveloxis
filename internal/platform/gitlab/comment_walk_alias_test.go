// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"net/http"
	"strconv"
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
			_, _ = w.Write([]byte(`[{"id":601,"body":"an mr note","system":false,"created_at":"2026-08-01T00:00:00Z","author":{"id":9,"username":"u"}},{"id":602,"body":"a diff note","system":false,"created_at":"2026-08-01T00:00:00Z","author":{"id":9,"username":"u"},"position":{"new_path":"a.go","new_line":3}}]`))
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
	mrNotes := 0
	for msg, err := range client.ListPRComments(t.Context(), "owner", "repo", since) {
		if err != nil {
			t.Fatalf("ListPRComments: %v", err)
		}
		mrNotes++
		if msg.PRRef == nil || msg.PRRef.PlatformPRNumber != 3 {
			t.Errorf("MR conversation note must carry its MR number, got %+v", msg.PRRef)
		}
	}
	if mrNotesHits.Load() == 0 {
		t.Error("MR notes walk never reached /merge_requests/3/notes — its driver listing was answered 304")
	}
	// The diff-positioned note belongs to ListReviewComments — the two
	// kinds stay disjoint.
	if mrNotes != 1 {
		t.Errorf("ListPRComments yielded %d notes, want exactly the 1 conversation note (the positioned one is a review comment)", mrNotes)
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

// Pass 31: a per-item notes read is ascending with no since, so a NEW
// note lands on the LAST page while page 1 stays byte-stable — a cached
// page-1 ETag would 304 and end the walk before page 2. Cycle 1 sees 100
// + 1 notes; a note is added; cycle 2 (same since, page 1 unchanged)
// must see 102, never 0.
func TestCommentWalkReadsEveryNotesPageAcrossCycles(t *testing.T) {
	var page2Notes atomic.Int64
	page2Notes.Store(1)
	page1 := strings.Builder{}
	page1.WriteString("[")
	for i := 1; i <= 100; i++ {
		if i > 1 {
			page1.WriteString(",")
		}
		page1.WriteString(`{"id":` + itoa(i) + `,"body":"n","system":false,"created_at":"2026-08-01T00:00:00Z","author":{"id":9,"username":"u"}}`)
	}
	page1.WriteString("]")
	client, _ := newTestClientWithCapture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/issues/7/notes"):
			if r.URL.Query().Get("page") == "2" {
				w.Header().Set("X-Next-Page", "")
				b := strings.Builder{}
				b.WriteString("[")
				for i := int64(1); i <= page2Notes.Load(); i++ {
					if i > 1 {
						b.WriteString(",")
					}
					b.WriteString(`{"id":` + itoa(int(100+i)) + `,"body":"late","system":false,"created_at":"2026-08-02T00:00:00Z","author":{"id":9,"username":"u"}}`)
				}
				b.WriteString("]")
				_, _ = w.Write([]byte(b.String()))
				return
			}
			// Page 1 is byte-stable across cycles and honors If-None-Match.
			if r.Header.Get("If-None-Match") == `"p1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", `"p1"`)
			w.Header().Set("X-Next-Page", "2")
			w.Header().Set("X-Per-Page", "100")
			_, _ = w.Write([]byte(page1.String()))
			return
		}
		// The driver listing (also honors If-None-Match like gitlab.com).
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte(`[{"id":71,"iid":7,"title":"issue","state":"opened","user_notes_count":101,"updated_at":"2026-08-02T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
	}))
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	count := func() int {
		t.Helper()
		n := 0
		for _, err := range client.ListIssueComments(t.Context(), "owner", "repo", since) {
			if err != nil {
				t.Fatalf("ListIssueComments: %v", err)
			}
			n++
		}
		return n
	}
	if n := count(); n != 101 {
		t.Fatalf("cycle 1 collected %d notes, want 101 (two pages)", n)
	}
	page2Notes.Store(2)
	if n := count(); n != 102 {
		t.Errorf("cycle 2 collected %d notes, want 102 — page 1's cached ETag must not end the walk before the page the new note is on", n)
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// Pass 32: the per-item MR-notes twin (the refresher's and gap fill's
// reader) must skip diff-positioned notes exactly as the repo-wide walk
// does — the refresher reads ListReviewCommentsForPR for the same MR a
// few lines later, so a positioned note yielded here was staged TWICE
// (message + review comment; a silent duplicate under the msg_kind
// arbiter). And a walk must continue past ONE item's 404: MR #2's notes
// are gone, MR #3's must still be yielded.
func TestPerItemMRNotesSkipDiffNotesAndWalksContinuePastOneItemsSkip(t *testing.T) {
	client, _ := newTestClientWithCapture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/merge_requests/2/notes"):
			http.NotFound(w, r) // deleted between the driver page and this read
		case strings.HasSuffix(path, "/merge_requests/3/notes"):
			_, _ = w.Write([]byte(`[{"id":701,"body":"conversation","system":false,"created_at":"2026-08-01T00:00:00Z","author":{"id":9,"username":"u"}},{"id":702,"body":"diff note","system":false,"created_at":"2026-08-01T00:00:00Z","author":{"id":9,"username":"u"},"position":{"new_path":"a.go","new_line":3}}]`))
		case strings.Contains(path, "/merge_requests"):
			_, _ = w.Write([]byte(`[{"id":32,"iid":2,"title":"gone","state":"opened","user_notes_count":1,"updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"},{"id":33,"iid":3,"title":"mr","state":"opened","user_notes_count":2,"updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	// The per-item twin skips the positioned note.
	n := 0
	for msg, err := range client.ListCommentsForPR(t.Context(), "owner", "repo", 3) {
		if err != nil {
			t.Fatalf("ListCommentsForPR: %v", err)
		}
		n++
		if msg.Message.PlatformMsgID != 701 {
			t.Errorf("per-item MR notes yielded note %d — diff-positioned notes belong to ListReviewCommentsForPR", msg.Message.PlatformMsgID)
		}
	}
	if n != 1 {
		t.Errorf("ListCommentsForPR yielded %d notes, want 1", n)
	}
	// The repo-wide walk continues past MR #2's 404 to MR #3.
	got := 0
	for msg, err := range client.ListPRComments(t.Context(), "owner", "repo", time.Time{}) {
		if err != nil {
			t.Fatalf("ListPRComments must not end on one item's 404: %v", err)
		}
		got++
		if msg.PRRef == nil || msg.PRRef.PlatformPRNumber != 3 {
			t.Errorf("expected MR #3's conversation note, got %+v", msg.PRRef)
		}
	}
	if got != 1 {
		t.Errorf("walk yielded %d notes after MR #2's 404, want MR #3's 1", got)
	}
}
