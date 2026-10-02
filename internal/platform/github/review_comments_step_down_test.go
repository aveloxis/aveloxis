// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.72 — the review-comment listings opt into the page-size
// step-down. A fake /pulls/comments whose old tail cannot be served at
// per_page=100 (the 2026-10-02 freeCodeCamp/zephyr measurement) must be
// walked to the end with every comment exactly once: this is the
// convergence that lets a force-full run succeed and clear its flag.
func TestReviewCommentListingsStepDownOverSlowPages(t *testing.T) {
	t.Cleanup(platform.SetServerErrorSleepForTest(func(ctx context.Context, _ time.Duration) error { return ctx.Err() }))
	// A millisecond budget for the test; the fake answers an over-budget
	// page after longer than that, as GitHub answers after its 10 s.
	old := reviewCommentRequestBudget
	reviewCommentRequestBudget = 20 * time.Millisecond
	t.Cleanup(func() { reviewCommentRequestBudget = old })
	const total = 300
	slow := func(id int) bool { return id > 200 } // the oldest third
	handler := func(path string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != path {
				t.Errorf("unexpected path %q", r.URL.Path)
				http.NotFound(w, r)
				return
			}
			q := r.URL.Query()
			pp, _ := strconv.Atoi(q.Get("per_page"))
			page, _ := strconv.Atoi(q.Get("page"))
			if page == 0 {
				page = 1
			}
			lo := (page-1)*pp + 1
			hi := min(lo+pp-1, total)
			n := 0
			for id := lo; id <= hi; id++ {
				if slow(id) {
					n++
				}
			}
			if n > 50 {
				time.Sleep(30 * time.Millisecond)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			if hi < total {
				q.Set("page", strconv.Itoa(page+1))
				w.Header().Set("Link", fmt.Sprintf(`<%s?%s>; rel="next"`, r.URL.Path, q.Encode()))
			}
			var out []map[string]any
			for id := lo; id <= hi; id++ {
				out = append(out, map[string]any{
					"id": id, "body": "c", "created_at": "2020-01-01T00:00:00Z", "updated_at": "2020-01-01T00:00:00Z",
					"user": map[string]any{"id": 1, "login": "a"},
				})
			}
			_ = json.NewEncoder(w).Encode(out)
		}
	}
	check := func(t *testing.T, seq func(func(platform.ReviewCommentWithRef, error) bool)) {
		t.Helper()
		next := int64(1)
		for rc, err := range seq {
			if err != nil {
				t.Fatalf("listing failed: %v", err)
			}
			if rc.Comment.PlatformSrcID != next {
				t.Fatalf("comment %d arrived where %d was expected — a gap or duplicate", rc.Comment.PlatformSrcID, next)
			}
			next++
		}
		if next-1 != total {
			t.Fatalf("listed %d comments, want %d", next-1, total)
		}
	}

	t.Run("repo-wide /pulls/comments, full walk", func(t *testing.T) {
		c := testGHClient(t, handler("/repos/o/r/pulls/comments"))
		check(t, c.ListReviewComments(context.Background(), "o", "r", time.Time{}))
	})
	t.Run("repo-wide /pulls/comments, incremental", func(t *testing.T) {
		c := testGHClient(t, handler("/repos/o/r/pulls/comments"))
		check(t, c.ListReviewComments(context.Background(), "o", "r", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)))
	})
	t.Run("per-PR /pulls/{n}/comments", func(t *testing.T) {
		c := testGHClient(t, handler("/repos/o/r/pulls/7/comments"))
		check(t, c.ListReviewCommentsForPR(context.Background(), "o", "r", 7))
	})
}

// The production budget is GitHub's documented 10 s processing limit. A
// smaller value would count 502s that came back before GitHub's limit (an
// outage, a gateway blip) as pages over budget and step down on them; a
// larger one could miss real timeouts, which arrive just past the limit
// (production saw 10-11 s), and never step.
func TestReviewCommentRequestBudgetIsGitHubsLimit(t *testing.T) {
	if reviewCommentRequestBudget != platform.GitHubRequestBudget || platform.GitHubRequestBudget != 10*time.Second {
		t.Errorf("reviewCommentRequestBudget = %v, GitHubRequestBudget = %v; want both GitHub's documented 10s",
			reviewCommentRequestBudget, platform.GitHubRequestBudget)
	}
}
