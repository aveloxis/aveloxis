// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 1b (v0.27.37): the MAIN-PATH comment iterators
// (ListIssueComments / ListPRComments — the repo-wide walks
// collectMessages consumes) must set the parent NUMBER on every ref.
// Pre-fix they set only PlatformSrcID; the processor resolves parents
// EXCLUSIVELY via PlatformIssueNumber/PlatformPRNumber (fallback
// IssueID/PRID, also 0) and hit `return nil // no way to resolve
// parent — skip` — so every GitLab conversation comment on the main
// collection path was silently dropped since inception. Only the
// v0.16.12 per-item paths (which DO set the numbers) ever landed
// GitLab comments, which is why the loss stayed invisible.

package gitlab

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestGitLabMainPathIssueCommentsCarryParentNumber(t *testing.T) {
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/issues") && !strings.Contains(r.URL.Path, "/notes"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"iid": 42, "user_notes_count": 1,
				"created_at": time.Now().Format(time.RFC3339),
			}})
		case strings.Contains(r.URL.Path, "/issues/42/notes"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"id": 2001, "body": "a note", "system": false,
				"created_at": "2026-01-01T00:00:00Z",
				"author":     map[string]any{"id": 5, "username": "alice"},
			}})
		default:
			_ = json.NewEncoder(w).Encode([]any{})
		}
	}))

	seen := 0
	for ref, err := range client.ListIssueComments(context.Background(), "owner", "repo", time.Time{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seen++
		if ref.IssueRef == nil {
			t.Fatal("IssueRef must be non-nil")
		}
		if ref.IssueRef.PlatformIssueNumber != 42 {
			t.Errorf("PlatformIssueNumber = %d, want 42 — without it the processor drops the comment with `no way to resolve parent`", ref.IssueRef.PlatformIssueNumber)
		}
	}
	if seen == 0 {
		t.Fatal("expected at least one comment from the main path")
	}
}

func TestGitLabMainPathPRCommentsCarryParentNumber(t *testing.T) {
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/merge_requests") && !strings.Contains(r.URL.Path, "/notes"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"iid": 7, "user_notes_count": 1,
				"created_at": time.Now().Format(time.RFC3339),
			}})
		case strings.Contains(r.URL.Path, "/merge_requests/7/notes"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"id": 3001, "body": "an MR note", "system": false,
				"created_at": "2026-01-01T00:00:00Z",
				"author":     map[string]any{"id": 5, "username": "alice"},
			}})
		default:
			_ = json.NewEncoder(w).Encode([]any{})
		}
	}))

	seen := 0
	for ref, err := range client.ListPRComments(context.Background(), "owner", "repo", time.Time{}) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seen++
		if ref.PRRef == nil {
			t.Fatal("PRRef must be non-nil")
		}
		if ref.PRRef.PlatformPRNumber != 7 {
			t.Errorf("PlatformPRNumber = %d, want 7 — without it the processor drops the comment with `no way to resolve parent`", ref.PRRef.PlatformPRNumber)
		}
	}
	if seen == 0 {
		t.Fatal("expected at least one MR comment from the main path")
	}
}
