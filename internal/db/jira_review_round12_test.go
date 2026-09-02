// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"
	"time"
)

// TestUpsertJiraIssueFromAPILinksTitleOnlyNative (Copilot round 12 on
// PR #193): a migrated repo's imported native issue can carry its Jira
// key ONLY in the title ("… [KEY]") until backfill-issue-external-keys
// runs — external_key is empty, so the 23505 LINK arm never fires and
// the pre-fix insert minted the synthetic duplicate that
// LinkOrCreateIssueFromEmail's title fallback explicitly prevents. The
// API writer must LINK via the same key-then-title lookup, enrich the
// native row (external_key + jira_issue_id, fill-empty), and leave
// every forge-owned field untouched (rank-1 protection).
func TestUpsertJiraIssueFromAPILinksTitleOnlyNative(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round12-title-link")

	var nativeID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues
			(repo_id, platform_issue_id, issue_number, issue_title, issue_state, data_source)
		VALUES ($1, 7777, 12, 'Imported: fix segment locks [AVR12-7]', 'open', 'GitHub API')
		RETURNING issue_id`, repoID).Scan(&nativeID); err != nil {
		t.Fatalf("seed native: %v", err)
	}

	in := JiraAPIIssue{
		RepoID: repoID, ExternalKey: "AVR12-7", JiraIssueID: 999888,
		Title: "jira-side title", Status: "Resolved", Resolution: "Fixed",
		ResolutionDate: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		Created:        time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		Updated:        time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
	}
	got, err := store.UpsertJiraIssueFromAPI(ctx, in)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got != nativeID {
		t.Fatalf("returned issue_id = %d, want the title-matched native %d (a different id means the synthetic duplicate was minted)", got, nativeID)
	}

	var count int
	if err := store.pool.QueryRow(ctx, `
		SELECT count(*) FROM aveloxis_data.issues WHERE repo_id = $1`, repoID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("issues rows = %d, want 1 — the title-only native must absorb the Jira envelope, never coexist with a synthetic", count)
	}

	var key, state, title string
	var jiraID *int64
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(external_key, ''), issue_state, issue_title, jira_issue_id
		FROM aveloxis_data.issues WHERE issue_id = $1`, nativeID).Scan(&key, &state, &title, &jiraID); err != nil {
		t.Fatal(err)
	}
	if key != "AVR12-7" {
		t.Fatalf("external_key = %q, want the enriched key", key)
	}
	if jiraID == nil || *jiraID != 999888 {
		t.Fatalf("jira_issue_id = %v, want 999888", jiraID)
	}
	if state != "open" || title != "Imported: fix segment locks [AVR12-7]" {
		t.Fatalf("state=%q title=%q — forge-owned fields must be untouched by the LINK (rank 1)", state, title)
	}

	// Replay: the exact-key probe now hits the enriched native — still
	// one row, same id, key untouched.
	got2, err := store.UpsertJiraIssueFromAPI(ctx, in)
	if err != nil || got2 != nativeID {
		t.Fatalf("replay: got %d err=%v, want %d", got2, err, nativeID)
	}
	if err := store.pool.QueryRow(ctx, `
		SELECT count(*) FROM aveloxis_data.issues WHERE repo_id = $1`, repoID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("replay rows = %d err=%v, want 1", count, err)
	}
}
