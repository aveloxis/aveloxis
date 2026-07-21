// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 5c tripwire (1), GitHub side (v0.27.43): every
// MessageWithRef yielded by the repo-wide comment iterators must carry
// a resolvable parent NUMBER — the processor resolves parents by
// number and silently skips refs without one. The GitLab twin
// (mainpath_comment_refs_test.go in the gitlab package) was written
// after that client shipped number-less refs for the product's whole
// life; this pins the GitHub side so the class can never appear here.

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestGitHubMainPathCommentRefsCarryParentNumber(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/o/r/issues/comments":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"id": 101, "body": "issue comment", "created_at": "2026-01-01T00:00:00Z",
				"user":      map[string]any{"id": 1, "login": "alice"},
				"issue_url": srvPath("api.github.com", "/repos/o/r/issues/42"),
				"html_url":  "https://github.com/o/r/issues/42#issuecomment-101",
			}, {
				"id": 102, "body": "PR conversation comment", "created_at": "2026-01-01T00:00:00Z",
				"user":      map[string]any{"id": 1, "login": "alice"},
				"issue_url": srvPath("api.github.com", "/repos/o/r/issues/7"),
				"html_url":  "https://github.com/o/r/pull/7#issuecomment-102",
			}})
		default:
			_ = json.NewEncoder(w).Encode([]any{})
		}
	}))
	defer srv.Close()

	logger := slog.New(slog.NewTextHandler(discardWriter{}, nil))
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)

	seen := 0
	for ref, err := range client.ListIssueComments(context.Background(), "o", "r", time.Time{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seen++
		switch {
		case ref.IssueRef != nil:
			if ref.IssueRef.PlatformIssueNumber == 0 {
				t.Error("issue-comment ref must carry PlatformIssueNumber — the processor drops number-less refs silently")
			}
		case ref.PRRef != nil:
			if ref.PRRef.PlatformPRNumber == 0 {
				t.Error("PR-comment ref must carry PlatformPRNumber — the processor drops number-less refs silently")
			}
		default:
			t.Error("every MessageWithRef must carry exactly one parent ref")
		}
	}
	if seen != 2 {
		t.Fatalf("expected 2 comments, got %d", seen)
	}
}

func srvPath(host, path string) string { return fmt.Sprintf("https://%s%s", host, path) }

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
