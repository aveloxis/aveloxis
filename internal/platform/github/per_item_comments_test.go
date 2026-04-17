package github

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestListCommentsForIssue — GET /repos/{o}/{r}/issues/{n}/comments should
// return one MessageWithRef per comment, tagged with an IssueRef whose
// PlatformIssueNumber matches the requested issue number. The ref type
// distinguishes this from a PR comment so the downstream processor writes
// into issue_message_ref rather than pull_request_message_ref.
func TestListCommentsForIssue(t *testing.T) {
	const issueNumber = 42
	payload := []map[string]any{
		{
			"id":         1001,
			"node_id":    "MDEyOklzc3VlQ29tbWVudDE=",
			"body":       "first comment on issue 42",
			"created_at": "2026-01-01T00:00:00Z",
			"issue_url":  "https://api.github.com/repos/o/r/issues/42",
			"html_url":   "https://github.com/o/r/issues/42#issuecomment-1001",
			"user":       map[string]any{"id": 5, "login": "alice"},
		},
		{
			"id":         1002,
			"node_id":    "MDEyOklzc3VlQ29tbWVudDI=",
			"body":       "second comment on issue 42",
			"created_at": "2026-01-02T00:00:00Z",
			"issue_url":  "https://api.github.com/repos/o/r/issues/42",
			"html_url":   "https://github.com/o/r/issues/42#issuecomment-1002",
			"user":       map[string]any{"id": 6, "login": "bob"},
		},
	}
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/issues/42/comments") {
			t.Errorf("expected path to contain /issues/42/comments, got %q — the per-item method must scope to a single issue, not fetch the repo-wide listing", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))

	var got int
	for ref, err := range client.ListCommentsForIssue(context.Background(), "o", "r", issueNumber) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got++
		if ref.IssueRef == nil {
			t.Error("IssueRef must be non-nil on issue comments so the processor writes to issue_message_ref")
		}
		if ref.PRRef != nil {
			t.Error("PRRef must be nil on issue comments — otherwise the processor double-writes or misclassifies")
		}
		if ref.IssueRef != nil && ref.IssueRef.PlatformIssueNumber != issueNumber {
			t.Errorf("PlatformIssueNumber = %d, want %d — the ref must carry the parent issue number so the processor can link to the right issues row", ref.IssueRef.PlatformIssueNumber, issueNumber)
		}
	}
	if got != 2 {
		t.Errorf("got %d comments, want 2", got)
	}
}

// TestListCommentsForPR — GET /repos/{o}/{r}/issues/{n}/comments (GitHub
// PR conversation comments live on the issue comments endpoint). Must tag
// PRRef not IssueRef so the processor writes to pull_request_message_ref.
func TestListCommentsForPR(t *testing.T) {
	const prNumber = 7
	payload := []map[string]any{
		{
			"id":         2001,
			"node_id":    "MDEyOlBSQ29tbWVudDE=",
			"body":       "conversation comment on PR 7",
			"created_at": "2026-01-01T00:00:00Z",
			"issue_url":  "https://api.github.com/repos/o/r/issues/7",
			"html_url":   "https://github.com/o/r/pull/7#issuecomment-2001",
			"user":       map[string]any{"id": 5, "login": "alice"},
		},
	}
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/issues/7/comments") {
			t.Errorf("expected path /issues/7/comments for PR conversation comments, got %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))

	var got int
	for ref, err := range client.ListCommentsForPR(context.Background(), "o", "r", prNumber) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got++
		if ref.PRRef == nil {
			t.Error("PRRef must be non-nil on PR conversation comments so the processor writes to pull_request_message_ref, not issue_message_ref")
		}
		if ref.IssueRef != nil {
			t.Error("IssueRef must be nil when the parent is a PR — misclassification would write the comment to the wrong bridge table")
		}
		if ref.PRRef != nil && ref.PRRef.PlatformPRNumber != prNumber {
			t.Errorf("PlatformPRNumber = %d, want %d", ref.PRRef.PlatformPRNumber, prNumber)
		}
	}
	if got != 1 {
		t.Errorf("got %d comments, want 1", got)
	}
}

// TestListReviewCommentsForPR — GET /repos/{o}/{r}/pulls/{n}/comments for
// inline review comments (linked to specific diff lines). Must return a
// ReviewCommentWithRef carrying path/line/hunk metadata.
func TestListReviewCommentsForPR(t *testing.T) {
	const prNumber = 99
	payload := []map[string]any{
		{
			"id":                     3001,
			"node_id":                "MDEyOlJldmlld0NvbW1lbnQx",
			"body":                   "inline comment on line 10",
			"created_at":             "2026-01-01T00:00:00Z",
			"updated_at":             "2026-01-01T00:00:00Z",
			"path":                   "src/foo.go",
			"diff_hunk":              "@@ -1,5 +1,5 @@",
			"line":                   10,
			"pull_request_review_id": 500,
			"html_url":               "https://github.com/o/r/pull/99#discussion_r3001",
			"user":                   map[string]any{"id": 5, "login": "alice"},
		},
	}
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/pulls/99/comments") {
			t.Errorf("expected /pulls/99/comments endpoint (not /issues/.../comments), got %q — inline review comments live on the pulls endpoint", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))

	var got int
	for ref, err := range client.ListReviewCommentsForPR(context.Background(), "o", "r", prNumber) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got++
		if ref.Comment.Path != "src/foo.go" {
			t.Errorf("Comment.Path = %q, want src/foo.go (proves the endpoint response is actually being parsed)", ref.Comment.Path)
		}
		if ref.Comment.DiffHunk == "" {
			t.Error("Comment.DiffHunk must be populated so reviewers can see the code context in downstream analysis")
		}
	}
	if got != 1 {
		t.Errorf("got %d review comments, want 1", got)
	}
}
