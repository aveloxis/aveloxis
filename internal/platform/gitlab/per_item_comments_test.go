package gitlab

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestGitLabListCommentsForIssue — GET /projects/:id/issues/:iid/notes for
// a single issue. System notes must be filtered out (those represent events,
// not user discussion, and they have a separate collection path).
func TestGitLabListCommentsForIssue(t *testing.T) {
	const issueIID = 42
	payload := []map[string]any{
		{
			"id":         2001,
			"body":       "first note on issue 42",
			"created_at": "2026-01-01T00:00:00Z",
			"system":     false,
			"author":     map[string]any{"id": 5, "username": "alice"},
		},
		{
			"id":         2002,
			"body":       "assigned to bob",
			"created_at": "2026-01-01T01:00:00Z",
			"system":     true, // must be skipped
			"author":     map[string]any{"id": 1, "username": "gitlab-bot"},
		},
		{
			"id":         2003,
			"body":       "second note on issue 42",
			"created_at": "2026-01-02T00:00:00Z",
			"system":     false,
			"author":     map[string]any{"id": 6, "username": "bob"},
		},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/issues/42/notes") {
			t.Errorf("expected /issues/42/notes path, got %q — the per-item method must scope to a single issue", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))

	var got int
	for ref, err := range client.ListCommentsForIssue(context.Background(), "owner", "repo", issueIID) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got++
		if ref.IssueRef == nil {
			t.Error("IssueRef must be non-nil so the processor links via issue_message_ref")
		}
		if ref.PRRef != nil {
			t.Error("PRRef must be nil for issue notes")
		}
	}
	if got != 2 {
		t.Errorf("got %d user notes, want 2 — system notes must be skipped because they duplicate timeline events", got)
	}
}

// TestGitLabListCommentsForPR — GET /projects/:id/merge_requests/:iid/notes
// for a single MR. System notes are filtered out.
func TestGitLabListCommentsForPR(t *testing.T) {
	const mrIID = 7
	payload := []map[string]any{
		{
			"id":         3001,
			"body":       "conversation note on MR 7",
			"created_at": "2026-01-01T00:00:00Z",
			"system":     false,
			"author":     map[string]any{"id": 5, "username": "alice"},
		},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/merge_requests/7/notes") {
			t.Errorf("expected /merge_requests/7/notes path, got %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))

	var got int
	for ref, err := range client.ListCommentsForPR(context.Background(), "owner", "repo", mrIID) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got++
		if ref.PRRef == nil {
			t.Error("PRRef must be non-nil so the processor links via pull_request_message_ref")
		}
		if ref.IssueRef != nil {
			t.Error("IssueRef must be nil for MR notes")
		}
	}
	if got != 1 {
		t.Errorf("got %d MR notes, want 1", got)
	}
}

// TestGitLabListReviewCommentsForPR — GET /projects/:id/merge_requests/:iid/discussions
// filtered to discussion notes with a position (= diff-anchored inline comments).
func TestGitLabListReviewCommentsForPR(t *testing.T) {
	const mrIID = 99
	payload := []map[string]any{
		{
			"id": "disc-1",
			"notes": []map[string]any{
				{
					"id":         4001,
					"body":       "inline comment",
					"created_at": "2026-01-01T00:00:00Z",
					"system":     false,
					"author":     map[string]any{"id": 5, "username": "alice"},
					"position": map[string]any{
						"new_path": "src/foo.go",
						"new_line": 10,
						"head_sha": "abc123",
						"base_sha": "def456",
					},
				},
				{
					// Same discussion, but no position → regular comment, skipped.
					"id":         4002,
					"body":       "regular non-inline note",
					"created_at": "2026-01-01T00:10:00Z",
					"system":     false,
					"author":     map[string]any{"id": 5, "username": "alice"},
				},
			},
		},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/merge_requests/99/discussions") {
			t.Errorf("expected /merge_requests/99/discussions path, got %q — inline review comments live in discussions, filtered to those with position", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(payload)
	}))

	var got int
	for ref, err := range client.ListReviewCommentsForPR(context.Background(), "owner", "repo", mrIID) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got++
		if ref.Comment.Path != "src/foo.go" {
			t.Errorf("Comment.Path = %q, want src/foo.go", ref.Comment.Path)
		}
	}
	if got != 1 {
		t.Errorf("got %d review comments, want 1 — only notes with a position count as inline review comments; non-positioned notes are conversation comments handled elsewhere", got)
	}
}
