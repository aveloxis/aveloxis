// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.26.5 — write-path pins + backfill integration tests for the
// identity-attribution remediation (2026-07-09 audit + plan at
// summary/identity-attribution-audit-2026-07-09.md).

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestAssignmentInsertsWriteCntrbID pins that the three assignment
// INSERTs and the meta upsert carry cntrb_id with a fill-don't-clobber
// COALESCE, so active items self-heal on every incremental cycle.
func TestAssignmentInsertsWriteCntrbID(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, tbl := range []string{
		"INSERT INTO aveloxis_data.issue_assignees",
		"INSERT INTO aveloxis_data.pull_request_assignees",
		"INSERT INTO aveloxis_data.pull_request_reviewers",
	} {
		idx := strings.Index(code, tbl)
		if idx < 0 {
			t.Fatalf("cannot find %s", tbl)
		}
		window := code[idx:min(idx+700, len(code))]
		if !strings.Contains(window, "cntrb_id") {
			t.Errorf("%s must include the cntrb_id column — it was absent since "+
				"inception (0%% fill on 13M production rows)", tbl)
		}
		if !strings.Contains(window, "COALESCE(") || !strings.Contains(window, "DO UPDATE") {
			t.Errorf("%s must upsert cntrb_id via ON CONFLICT DO UPDATE SET cntrb_id = "+
				"COALESCE(existing, EXCLUDED) so re-collection fills without clobbering", tbl)
		}
	}

	idx := strings.Index(code, "func (s *PostgresStore) UpsertPRMeta(")
	window := code[idx:min(idx+900, len(code))]
	if !strings.Contains(window, "cntrb_id") || !strings.Contains(window, "COALESCE(") {
		t.Error("UpsertPRMeta must write cntrb_id with a COALESCE upsert (operator " +
			"decision: pr_meta identity is high-value)")
	}
}

func TestIdentityBackfillFunctionsRegistered(t *testing.T) {
	src, err := os.ReadFile("identity_backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, needle := range []string{
		"func (s *PostgresStore) BackfillAssignmentIdentities(",
		"func (s *PostgresStore) BackfillPRMetaOwners(",
		"func (s *PostgresStore) BackfillClosedByFromEvents(",
		"func (s *PostgresStore) DeriveIssueClosedByFromEvents(",
		// the identity join
		"ci.platform_user_id",
		// closed_by derivation shape
		"DISTINCT ON (issue_id)",
		"action = 'closed'",
		// meta owner derivation: fork-owner login match, case-insensitive
		"split_part(",
		"LOWER(",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("identity_backfill.go must contain %q", needle)
		}
	}
}

// TestIdentityBackfillEndToEnd seeds the exact dark shapes the audit
// found and proves each backfill phase fills them — then proves
// idempotency.
func TestIdentityBackfillEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	pool := store.Pool()

	// --- Seed: repo, contributor (+identity), one dark row per shape.
	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/aveloxis-it/identity-backfill', 'aveloxis-it', 'identity-backfill', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_owner = EXCLUDED.repo_owner
		RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	var cntrbID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, gh_login, gh_user_id)
		VALUES ('avx-it-assignee', 'AVX-IT-Assignee', 987001)
		ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO UPDATE SET gh_user_id = EXCLUDED.gh_user_id
		RETURNING cntrb_id::text`).Scan(&cntrbID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login)
		VALUES ($1::uuid, 1, 987001, 'avx-it-assignee')
		ON CONFLICT (platform_id, platform_user_id) DO NOTHING`, cntrbID); err != nil {
		t.Fatal(err)
	}

	var issueID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, closed_at)
		VALUES ($1, 660001, 5, NOW())
		RETURNING issue_id`, repoID).Scan(&issueID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issue_assignees (issue_id, repo_id, platform_assignee_id)
		VALUES ($1, $2, 987001)`, issueID, repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issue_events (issue_id, repo_id, platform_id, platform_event_id, action, cntrb_id, created_at)
		VALUES ($1, $2, 1, 770001, 'closed', $3::uuid, NOW())`, issueID, repoID, cntrbID); err != nil {
		t.Fatal(err)
	}
	var prID, metaID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number)
		VALUES ($1, 880001, 9) RETURNING pull_request_id`, repoID).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_request_meta (pull_request_id, repo_id, head_or_base, meta_ref)
		VALUES ($1, $2, 'head', 'branch-x') RETURNING pr_meta_id`, prID, repoID).Scan(&metaID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.pull_request_repo (pr_repo_meta_id, pr_repo_head_or_base, pr_repo_full_name)
		VALUES ($1, 'head', 'AVX-IT-Assignee/some-fork')`, metaID); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		c := context.Background()
		for _, q := range []string{
			`DELETE FROM aveloxis_data.pull_request_repo WHERE pr_repo_meta_id = ` +
				"(SELECT pr_meta_id FROM aveloxis_data.pull_request_meta WHERE repo_id = $1)",
			`DELETE FROM aveloxis_data.pull_request_meta WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.issue_events WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.issue_assignees WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.issues WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`,
		} {
			_, _ = pool.Exec(c, q, repoID)
		}
	})

	// --- Phase 1: assignment identity joins.
	n, err := store.BackfillAssignmentIdentities(ctx, 10000, 0, false)
	if err != nil {
		t.Fatalf("BackfillAssignmentIdentities: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 assignment row backfilled, got %d", n)
	}
	var got string
	if err := pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.issue_assignees WHERE issue_id = $1`, issueID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != cntrbID {
		t.Errorf("issue_assignees.cntrb_id: got %s want %s", got, cntrbID)
	}

	// --- Phase 1b: meta owner via fork-owner login (case-insensitive).
	if _, err := store.BackfillPRMetaOwners(ctx, 10000, 0, false); err != nil {
		t.Fatalf("BackfillPRMetaOwners: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(cntrb_id::text,'') FROM aveloxis_data.pull_request_meta WHERE pr_meta_id = $1`, metaID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != cntrbID {
		t.Errorf("pull_request_meta.cntrb_id: got %q want %s (owner login match must be case-insensitive)", got, cntrbID)
	}

	// --- Phase 2: closed_by from events.
	if _, err := store.BackfillClosedByFromEvents(ctx, 10000, 0, false); err != nil {
		t.Fatalf("BackfillClosedByFromEvents: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT COALESCE(closed_by_id::text,'') FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != cntrbID {
		t.Errorf("issues.closed_by_id: got %q want %s", got, cntrbID)
	}

	// --- Idempotency: second pass updates nothing.
	n2, err := store.BackfillAssignmentIdentities(ctx, 10000, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Errorf("second assignment backfill pass must be a no-op, updated %d", n2)
	}

	// --- Per-repo derivation (the forward ProcessRepo step) is also a
	// clean no-op now that the backfill filled it.
	if _, err := store.DeriveIssueClosedByFromEvents(ctx, repoID); err != nil {
		t.Fatalf("DeriveIssueClosedByFromEvents: %v", err)
	}
}
