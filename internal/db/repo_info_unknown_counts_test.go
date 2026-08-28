// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestInsertRepoInfoCarriesUnknownCountsForward — v0.28.18 (fresh-context
// L1/SR-16 finding on the v0.28.17 GitLab count reader): GitLab's
// countGitLabResource returned 0 on ANY error and on GitLab's documented
// X-Total omission above 10,000 records, so one transient 5xx on a count
// probe stored pr_count = 0, rotated the accurate prior snapshot to
// history, and made GetGapHealCandidates' `pr_count > last_prs` unable to
// ever select the repo. The fetcher now marks the count UNKNOWN instead
// of fabricating a zero, and InsertRepoInfo carries the prior snapshot's
// counts forward (it runs after RotateRepoInfoToHistory, so the prior
// row lives in repo_info_history). Never fabricate.
func TestInsertRepoInfoCarriesUnknownCountsForward(t *testing.T) {
	src := readSourceFile(t, "postgres.go")
	body := extractFuncBody(t, src, "func (s *PostgresStore) InsertRepoInfo(")
	for _, needle := range []string{"PRCountUnknown", "IssuesCountUnknown", "repo_info_history", "NULLS LAST", "COALESCE(open_issues, 0)", "info.IssuesCount, info.IssuesClosed, info.OpenIssues = ", "info.OpenIssues = 0"} {
		if !strings.Contains(body, needle) {
			t.Errorf("InsertRepoInfo must consult %s (carry the prior snapshot's counts forward when the fetcher could not supply them)", needle)
		}
	}
	modelSrc := readSourceFile(t, "../model/repoinfo.go")
	for _, needle := range []string{"PRCountUnknown", "IssuesCountUnknown"} {
		if !strings.Contains(modelSrc, needle) {
			t.Errorf("model.RepoInfo must carry %s", needle)
		}
	}
}

// Behavioral (AVELOXIS_TEST_DB): an unknown-count snapshot after a known
// one keeps the known counts; the first-ever snapshot with unknown counts
// stores zero (nothing to carry) — the honest residual.
func TestInsertRepoInfoUnknownCountsEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	url := "https://gitlab.com/_avunknown/counts-" + t.Name()
	repoID, err := store.UpsertRepo(ctx, &model.Repo{GitURL: url, Owner: "_avunknown", Name: "counts", Platform: model.PlatformGitLab})
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_info_history WHERE repo_id = $1`, repoID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, repoID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	insert := func(info model.RepoInfo) {
		t.Helper()
		info.RepoID = repoID
		if err := store.RotateRepoInfoToHistory(ctx, repoID); err != nil {
			t.Fatalf("rotate: %v", err)
		}
		if err := store.InsertRepoInfo(ctx, &info); err != nil {
			t.Fatalf("InsertRepoInfo: %v", err)
		}
	}
	current := func() (prCount, prsOpen, issues, issuesClosed int) {
		t.Helper()
		err := store.pool.QueryRow(ctx, `SELECT pr_count, prs_open, issues_count, issues_closed FROM aveloxis_data.repo_info WHERE repo_id = $1 ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC LIMIT 1`, repoID).Scan(&prCount, &prsOpen, &issues, &issuesClosed)
		if err != nil {
			t.Fatalf("read snapshot: %v", err)
		}
		return
	}

	currentOpen := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT open_issues FROM aveloxis_data.repo_info WHERE repo_id = $1 ORDER BY data_collection_date DESC NULLS LAST, repo_info_id DESC LIMIT 1`, repoID).Scan(&n); err != nil {
			t.Fatalf("read open_issues: %v", err)
		}
		return n
	}

	// First-ever snapshot with unknown counts: nothing to carry → zeros,
	// open_issues included (the fetcher hands in the payload's 77 here).
	insert(model.RepoInfo{PRCountUnknown: true, IssuesCountUnknown: true, OpenIssues: 77, CommitCount: 5})
	if pr, _, is, _ := current(); pr != 0 || is != 0 {
		t.Fatalf("first snapshot with unknown counts: pr_count=%d issues_count=%d, want 0/0", pr, is)
	}
	if n := currentOpen(); n != 0 {
		t.Fatalf("first snapshot with unknown issue counts stored open_issues=%d, want 0 (the whole triple is 0)", n)
	}
	// A known snapshot.
	insert(model.RepoInfo{PRCount: 42, PRsOpen: 7, IssuesCount: 99, IssuesClosed: 90, OpenIssues: 9, CommitCount: 6})
	if pr, open, is, closed := current(); pr != 42 || open != 7 || is != 99 || closed != 90 {
		t.Fatalf("known snapshot stored %d/%d/%d/%d", pr, open, is, closed)
	}
	// Unknown PR counts, known issue counts: PRs carried, issues fresh.
	insert(model.RepoInfo{PRCountUnknown: true, IssuesCount: 100, IssuesClosed: 91, OpenIssues: 8, CommitCount: 7})
	if pr, open, is, closed := current(); pr != 42 || open != 7 || is != 100 || closed != 91 {
		t.Fatalf("PR-unknown snapshot stored %d/%d/%d/%d, want 42/7 carried + 100/91 fresh", pr, open, is, closed)
	}
	// Unknown issue counts too: both carried from the latest prior snapshot.
	insert(model.RepoInfo{PRCountUnknown: true, IssuesCountUnknown: true, CommitCount: 8})
	if pr, open, is, closed := current(); pr != 42 || open != 7 || is != 100 || closed != 91 {
		t.Fatalf("all-unknown snapshot stored %d/%d/%d/%d, want 42/7/100/91 carried", pr, open, is, closed)
	}
	// Copilot round 4: open_issues travels with the issue totals — an
	// unknown-count snapshot must not read total=100/closed=91/open=0.
	if openIssues := currentOpen(); openIssues != 8 {
		t.Fatalf("all-unknown snapshot stored open_issues=%d, want 8 carried with the issue totals", openIssues)
	}
}
