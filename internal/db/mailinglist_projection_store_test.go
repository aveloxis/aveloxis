// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
	"time"
)

// TestSchemaHasEmailMessageProjectionColumns pins the Phase A NEW columns.
func TestSchemaHasEmailMessageProjectionColumns(t *testing.T) {
	src := readSchema(t)
	for _, needle := range []string{
		"linked_pr_review_id    BIGINT REFERENCES aveloxis_data.pull_request_reviews(pr_review_id)",
		"projected_kind         TEXT DEFAULT ''",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare email_message projection column: %q", needle)
		}
	}
	mig := readSourceFile(t, "migrate.go")
	for _, needle := range []string{`"linked_pr_review_id"`, `"projected_kind"`} {
		if !strings.Contains(mig, needle) {
			t.Errorf("migrate.go must addColumnIfMissing for %s", needle)
		}
	}
}

// TestSyntheticIssueIDIsNegativeDeterministic pins the synthetic-ID contract:
// negative (never collides with a real positive GitHub/GitLab id), stable
// across calls (idempotent upsert), and distinct keys map to distinct ids.
func TestSyntheticIssueIDIsNegativeDeterministic(t *testing.T) {
	a := syntheticIssueID("KAFKA-123")
	if a >= 0 {
		t.Errorf("syntheticIssueID must be negative, got %d", a)
	}
	if a != syntheticIssueID("KAFKA-123") {
		t.Error("syntheticIssueID must be deterministic for the same key")
	}
	if a == syntheticIssueID("KAFKA-124") {
		t.Error("distinct keys should map to distinct synthetic ids")
	}
}

func TestIssueNumberFromKey(t *testing.T) {
	cases := map[string]int{"KAFKA-123": 123, "ARROW-7": 7, "NOPE": 0, "A-B-9": 9}
	for k, want := range cases {
		if got := issueNumberFromKey(k); got != want {
			t.Errorf("issueNumberFromKey(%q) = %d, want %d", k, got, want)
		}
	}
}

// TestLinkOrCreateIssueFromEmail exercises create → idempotent re-create →
// link-existing, plus the comment bridge + comment_count recompute, live.
func TestLinkOrCreateIssueFromEmail(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()

	const repoGit = "https://github.com/_av_proj/repo"
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE data_source='_av_proj_list'`)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	clean()
	t.Cleanup(clean)

	var repoID int64
	if err := store.pool.QueryRow(ctx,
		`INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name) VALUES (1,$1,'_av_proj','repo') RETURNING repo_id`,
		repoGit).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}

	now := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)

	// CREATE.
	id1, created1, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "PROJ-1", "first issue", "body one", "JIRA", nil, now)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id1 == 0 || !created1 {
		t.Fatalf("expected a created issue, got id=%d created=%v", id1, created1)
	}
	// Idempotent re-create (same key) → LINK to the same row.
	id2, created2, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "PROJ-1", "first issue", "body one", "JIRA", nil, now)
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	if id2 != id1 || created2 {
		t.Errorf("re-create must LINK same issue (id=%d created=%v), want id=%d created=false", id2, created2, id1)
	}

	// Verify provenance: data_source=JIRA, external_key set, negative platform_issue_id.
	var ds, ek string
	var pid int64
	store.pool.QueryRow(ctx, `SELECT data_source, external_key, platform_issue_id FROM aveloxis_data.issues WHERE issue_id=$1`, id1).Scan(&ds, &ek, &pid)
	if ds != "JIRA" || ek != "PROJ-1" || pid >= 0 {
		t.Errorf("projected issue provenance wrong: data_source=%q external_key=%q platform_issue_id=%d", ds, ek, pid)
	}

	// LINK to a pre-existing native GitHub issue carrying an external_key.
	var nativeID int64
	store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, external_key, issue_title)
		VALUES ($1, 9999001, 42, 'PROJ-2', 'native') RETURNING issue_id`, repoID).Scan(&nativeID)
	idLink, createdLink, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "PROJ-2", "ignored title", "b", "JIRA", nil, now)
	if err != nil {
		t.Fatalf("link native: %v", err)
	}
	if idLink != nativeID || createdLink {
		t.Errorf("must LINK the existing native issue (id=%d created=%v), want id=%d created=false", idLink, createdLink, nativeID)
	}

	// Bridge a body to id1 → issue_message_ref + comment_count recompute.
	var msgID int64
	store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_text, data_source)
		VALUES ($1, -55501, 6, 'x', '_av_proj_list') RETURNING msg_id`, repoID).Scan(&msgID)
	if err := store.BridgeEmailToIssue(ctx, id1, repoID, msgID); err != nil {
		t.Fatalf("bridge: %v", err)
	}
	var cc, refs int
	store.pool.QueryRow(ctx, `SELECT comment_count FROM aveloxis_data.issues WHERE issue_id=$1`, id1).Scan(&cc)
	store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.issue_message_ref WHERE issue_id=$1 AND msg_id=$2`, id1, msgID).Scan(&refs)
	if refs != 1 {
		t.Errorf("expected 1 issue_message_ref, got %d", refs)
	}
	if cc != 1 {
		t.Errorf("comment_count must recompute to 1, got %d", cc)
	}
	// Idempotent bridge: re-bridging the same msg doesn't double-count.
	if err := store.BridgeEmailToIssue(ctx, id1, repoID, msgID); err != nil {
		t.Fatalf("re-bridge: %v", err)
	}
	store.pool.QueryRow(ctx, `SELECT comment_count FROM aveloxis_data.issues WHERE issue_id=$1`, id1).Scan(&cc)
	if cc != 1 {
		t.Errorf("re-bridge must not double-count; comment_count=%d", cc)
	}
}

// TestEmailMessageModelHasProjectionFields pins the model fields that back the
// NEW columns (so a rename in code and schema stay paired).
func TestEmailMessageModelHasProjectionFields(t *testing.T) {
	data := readSourceFile(t, "../model/email_message.go")
	for _, needle := range []string{"LinkedReviewID", "ProjectedKind"} {
		if !strings.Contains(data, needle) {
			t.Errorf("model.EmailMessage must declare %s", needle)
		}
	}
}
