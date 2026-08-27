// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
)

// v0.28.18 — the count probes carry an error arm (SR-16). Pre-.18
// countGitLabResource returned 0 on ANY error and on a missing X-Total
// (GitLab omits it above 10,000 records), so the snapshot stored
// pr_count = 0 and the accurate prior snapshot rotated to history.
func countsRouter(mode string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/issues_statistics"):
			if mode == "issues-forbidden" {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"403 Forbidden"}`))
				return
			}
			_, _ = w.Write([]byte(`{"statistics":{"counts":{"all":9,"closed":4,"opened":5}}}`))
		case strings.Contains(path, "/merge_requests"):
			state := r.URL.Query().Get("state")
			switch {
			case mode == "merged-forbidden" && state == "merged":
				// 403 is the cheapest definitive-error shape the client does
				// not retry; a 5xx follows the same arm after the retry budget.
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"403 Forbidden"}`))
				return
			case mode == "no-x-total" && state == "opened":
				// GitLab above 10,000 records: 200, no X-Total.
				_, _ = w.Write([]byte(`[{"iid":1}]`))
				return
			}
			totals := map[string]string{"opened": "3", "closed": "2", "merged": "1"}
			w.Header().Set("X-Total", totals[state])
			_, _ = w.Write([]byte(`[{"iid":1}]`))
		case strings.HasSuffix(path, "/repository/tree"):
			_, _ = w.Write([]byte(`[]`))
		case strings.HasSuffix(path, "/languages"):
			_, _ = w.Write([]byte(`{}`))
		default:
			_, _ = w.Write([]byte(`{"id":101,"default_branch":"main","web_url":"https://gitlab.com/owner/repo","path_with_namespace":"owner/repo","star_count":1,"forks_count":0,"open_issues_count":5,"statistics":{"commit_count":12}}`))
		}
	})
}

func TestFetchRepoInfoCountsHealthy(t *testing.T) {
	client, capture := newTestClientWithCapture(t, countsRouter("healthy"))
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if info.PRCountUnknown || info.IssuesCountUnknown {
		t.Errorf("healthy probes must not mark counts unknown: pr=%v issues=%v", info.PRCountUnknown, info.IssuesCountUnknown)
	}
	if info.PRCount != 6 || info.PRsOpen != 3 || info.PRsClosed != 2 || info.PRsMerged != 1 {
		t.Errorf("PR counts = %d/%d/%d/%d, want 6/3/2/1", info.PRCount, info.PRsOpen, info.PRsClosed, info.PRsMerged)
	}
	if info.IssuesCount != 9 || info.IssuesClosed != 4 {
		t.Errorf("issue counts = %d/%d, want 9/4", info.IssuesCount, info.IssuesClosed)
	}
	if capture.has(slog.LevelWarn, "count unavailable") {
		t.Error("no unavailable-count WARN expected on the healthy path")
	}
}

func TestFetchRepoInfoMarksPRCountsUnknownOnProbeError(t *testing.T) {
	client, capture := newTestClientWithCapture(t, countsRouter("merged-forbidden"))
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo must not fail the whole snapshot on one count probe: %v", err)
	}
	if !info.PRCountUnknown {
		t.Fatal("a failed merge-request count probe must mark PRCountUnknown (pre-v0.28.18: silently stored 0)")
	}
	if info.PRCount != 0 || info.PRsOpen != 0 {
		t.Errorf("unknown PR counts must not carry partial sums (open probe succeeded, merged failed): %d/%d", info.PRCount, info.PRsOpen)
	}
	if info.IssuesCountUnknown || info.IssuesCount != 9 {
		t.Errorf("issue counts are independent of the MR probes: unknown=%v count=%d", info.IssuesCountUnknown, info.IssuesCount)
	}
	if !capture.has(slog.LevelWarn, "merge request count unavailable") {
		t.Error("expected a WARN naming the unavailable merge request count")
	}
}

func TestFetchRepoInfoMarksPRCountsUnknownWhenXTotalOmitted(t *testing.T) {
	client, capture := newTestClientWithCapture(t, countsRouter("no-x-total"))
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if !info.PRCountUnknown {
		t.Fatal("a 200 without X-Total means MORE than GitLab will count (>10,000 records), never zero — must mark PRCountUnknown")
	}
	if !capture.has(slog.LevelWarn, "merge request count unavailable") {
		t.Error("expected a WARN naming the unavailable merge request count")
	}
}

func TestFetchRepoInfoMarksIssueCountsUnknownOnStatsError(t *testing.T) {
	client, capture := newTestClientWithCapture(t, countsRouter("issues-forbidden"))
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if !info.IssuesCountUnknown {
		t.Fatal("a failed issues_statistics read must mark IssuesCountUnknown (pre-v0.28.18: 'counts will be zero')")
	}
	if info.PRCountUnknown || info.PRCount != 6 {
		t.Errorf("PR counts are independent of the issue stats: unknown=%v count=%d", info.PRCountUnknown, info.PRCount)
	}
	if !capture.has(slog.LevelWarn, "issue statistics unavailable") {
		t.Error("expected a WARN naming the unavailable issue statistics")
	}
}
