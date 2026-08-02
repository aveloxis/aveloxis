// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestMirrorAndExternalKeyMethodsExist(t *testing.T) {
	data, err := os.ReadFile("email_message_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, sig := range []string{
		"func (s *PostgresStore) ResolveMirrorLink(",
		"func (s *PostgresStore) BackfillIssueExternalKeys(",
	} {
		if !strings.Contains(src, sig) {
			t.Errorf("email_message_store.go must declare %s", sig)
		}
	}
}

func TestResolveMirrorLinkAndExternalKeyBackfill(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub, GitURL: "https://github.com/apache/mirrortest",
		Owner: "apache", Name: "mirrortest",
	})
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	// Seed a PR #42, an issue #7 (with a [KEY-N] title for the backfill).
	var prID, issueID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number)
		VALUES ($1, 999042, 42) RETURNING pull_request_id`, repoID).Scan(&prID); err != nil {
		t.Fatalf("seed PR: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title)
		VALUES ($1, 999007, 7, 'lock files dont work [MIRRORTEST-7]') RETURNING issue_id`, repoID).Scan(&issueID); err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	defer func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.pull_requests WHERE pull_request_id=$1`, prID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE issue_id=$1`, issueID)
	}()

	// Mirror link: pull #42 → prID; issues #7 → issueID; missing → nils.
	if _, gotPR, _ := store.ResolveMirrorLink(ctx, "apache", "mirrortest", "pull", 42); gotPR == nil || *gotPR != prID {
		t.Errorf("ResolveMirrorLink pull = %v, want %d", gotPR, prID)
	}
	if gotIssue, _, _ := store.ResolveMirrorLink(ctx, "APACHE", "MirrorTest", "issues", 7); gotIssue == nil || *gotIssue != issueID {
		t.Errorf("ResolveMirrorLink issues (case-insensitive) = %v, want %d", gotIssue, issueID)
	}
	if i, p, _ := store.ResolveMirrorLink(ctx, "apache", "mirrortest", "pull", 99999); i != nil || p != nil {
		t.Error("missing PR must resolve to nils")
	}

	// External-key backfill parses [MIRRORTEST-7] from the title.
	if _, err := store.BackfillIssueExternalKeys(ctx); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	var key string
	if err := store.pool.QueryRow(ctx,
		`SELECT external_key FROM aveloxis_data.issues WHERE issue_id=$1`, issueID).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != "MIRRORTEST-7" {
		t.Errorf("external_key = %q, want MIRRORTEST-7", key)
	}
}
