// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Integration test for the v0.22.3 cntrb_id collision soft-merge.
//
// Validates the end-to-end behavior of MergeCntrbIDCollisionsBatch:
//
//   - Loser's contributor_identities row moves to winner.
//   - Winner picks up loser's non-empty fields via COALESCE (winner-
//     wins-when-present semantics).
//   - contributors_aliases row created mapping loser's email to winner.
//   - Loser flagged cntrb_deleted = 1; row stays in place.
//   - Loser's child FK references (e.g. an issue's reporter_id)
//     remain intact and valid.
//   - Re-running is idempotent: the merged pair drops out of the
//     candidate set because cntrb_deleted = 1 filter applies.
//
// Gated on AVELOXIS_TEST_DB.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestMergeCntrbIDCollisionsBatchEndToEnd(t *testing.T) {
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
	defer store.Close()

	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// Collision setup:
	//   loser  — cntrb_login = "olduser", random cntrb_id, identity
	//            for (platform=1, user_id=88888), cntrb_email +
	//            cntrb_company populated
	//   winner — cntrb_login = "newuser", deterministic cntrb_id =
	//            PlatformUUID(1, 88888), no identity, no email/company
	// Expected after merge:
	//   - identity points at winner
	//   - winner has cntrb_email + cntrb_company from loser
	//   - aliases has a row (loser.email, winner.canonical/email)
	//   - loser.cntrb_deleted = 1
	//   - child issue still points at loser.cntrb_id and still resolves
	const (
		loserLogin     = "_av_merge_olduser"
		winnerLogin    = "_av_merge_newuser"
		loserOldID     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		winnerTargetID = "0100015b-3800-0000-0000-000000000000" // PlatformUUID(1, 88888): bytes 01 00 01 5b 38 …
		loserEmail     = "olduser@example.com"
		loserCompany   = "Acme Corp"
	)

	cleanup := func(sql string, args ...any) {
		if _, err := store.pool.Exec(ctx, sql, args...); err != nil {
			t.Logf("pre-test cleanup (non-fatal): %v", err)
		}
	}
	for _, login := range []string{loserLogin, winnerLogin} {
		cleanup(`DELETE FROM aveloxis_data.issues WHERE reporter_id IN (
			SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1
		)`, login)
		cleanup(`DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id IN (
			SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1
		)`, login)
		cleanup(`DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN (
			SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1
		)`, login)
		// v0.23.0 contributor_login_history has an ON DELETE RESTRICT FK to
		// contributors; clear it before deleting the contributor, or this
		// cleanup fails against a reused scratch DB (CI uses a fresh DB and
		// never hit it; production never hard-deletes contributors).
		cleanup(`DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id IN (
			SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1
		)`, login)
		cleanup(`DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	}
	cleanup(`DELETE FROM aveloxis_data.repos WHERE repo_owner = '_av_merge' AND repo_name = 'test'`)

	// Seed repo for the issue.
	var repoGroupID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repo_groups (rg_name, rg_description)
		VALUES ('_av_merge_grp', 'collision merge test')
		ON CONFLICT DO NOTHING RETURNING repo_group_id`).Scan(&repoGroupID); err != nil {
		if err := store.pool.QueryRow(ctx,
			`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = '_av_merge_grp'`).Scan(&repoGroupID); err != nil {
			t.Fatalf("repo_group setup: %v", err)
		}
	}
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_owner, repo_name, repo_git, platform_id, repo_group_id)
		VALUES ('_av_merge', 'test', 'https://github.com/_av_merge/test', 1, $1)
		RETURNING repo_id`, repoGroupID).Scan(&repoID); err != nil {
		t.Fatalf("repo setup: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// Seed loser (random cntrb_id, with data) + winner (deterministic
	// cntrb_id, no data).
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email, cntrb_company)
		VALUES ($1::uuid, $2, $3, $4)`, loserOldID, loserLogin, loserEmail, loserCompany); err != nil {
		t.Fatalf("seed loser: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login)
		VALUES ($1::uuid, $2)`, winnerTargetID, winnerLogin); err != nil {
		t.Fatalf("seed winner: %v", err)
	}

	// Seed identity row pointing at loser (88888 fits in uint32).
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_identities
			(cntrb_id, platform_id, platform_user_id, login, name, email, avatar_url, profile_url, node_id, user_type, is_admin)
		VALUES ($1::uuid, 1, 88888, $2, '', $3, '', '', '', '', FALSE)`,
		loserOldID, loserLogin, loserEmail); err != nil {
		t.Fatalf("seed identity: %v", err)
	}

	// Seed a child issue pointing at loser to verify FK integrity
	// (R10) is preserved by soft-merge.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, reporter_id)
		VALUES ($1, 88888888, 88888, $2::uuid)`, repoID, loserOldID); err != nil {
		t.Fatalf("seed issue: %v", err)
	}

	// Count phase.
	collisions, err := CountCntrbIDCollisions(ctx, store)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("collisions: %d", collisions)
	if collisions < 1 {
		t.Fatalf("expected at least 1 collision, got %d", collisions)
	}

	// Merge.
	merged, err := MergeCntrbIDCollisionsBatch(ctx, store, 100)
	if err != nil {
		t.Fatalf("MergeCntrbIDCollisionsBatch: %v", err)
	}
	t.Logf("merged %d pairs", merged)
	if merged < 1 {
		t.Fatalf("expected at least 1 merge, got %d", merged)
	}

	// Assertion 1: identity now points at winner.
	var identityCntrbID string
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_id::text FROM aveloxis_data.contributor_identities
		WHERE platform_id = 1 AND platform_user_id = 88888
	`).Scan(&identityCntrbID); err != nil {
		t.Fatalf("read identity back: %v", err)
	}
	if identityCntrbID != winnerTargetID {
		t.Errorf("identity.cntrb_id = %q, want winner %q", identityCntrbID, winnerTargetID)
	}

	// Assertion 2: winner picked up loser's email + company.
	var winnerEmail, winnerCompany string
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(cntrb_email,''), COALESCE(cntrb_company,'')
		FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid
	`, winnerTargetID).Scan(&winnerEmail, &winnerCompany); err != nil {
		t.Fatalf("read winner back: %v", err)
	}
	if winnerEmail != loserEmail {
		t.Errorf("winner.cntrb_email = %q, want loser's %q (COALESCE prefer-existing didn't fall through)",
			winnerEmail, loserEmail)
	}
	if winnerCompany != loserCompany {
		t.Errorf("winner.cntrb_company = %q, want loser's %q", winnerCompany, loserCompany)
	}

	// Assertion 3: alias row created.
	var aliasCount int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.contributors_aliases
		WHERE cntrb_id = $1::uuid AND alias_email = $2
	`, winnerTargetID, loserEmail).Scan(&aliasCount); err != nil {
		t.Fatalf("read alias: %v", err)
	}
	if aliasCount != 1 {
		t.Errorf("contributors_aliases count for (winner, loserEmail) = %d, want 1", aliasCount)
	}

	// Assertion 4: loser flagged cntrb_deleted = 1.
	var loserDeleted int
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(cntrb_deleted, 0) FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid
	`, loserOldID).Scan(&loserDeleted); err != nil {
		t.Fatalf("read loser back: %v", err)
	}
	if loserDeleted != 1 {
		t.Errorf("loser.cntrb_deleted = %d, want 1", loserDeleted)
	}

	// Assertion 5: child issue still points at loser.cntrb_id (R10
	// FK integrity — the loser row stayed in place specifically so
	// this would keep working).
	var issueReporterID string
	if err := store.pool.QueryRow(ctx, `
		SELECT reporter_id::text FROM aveloxis_data.issues
		WHERE repo_id = $1 AND issue_number = 88888
	`, repoID).Scan(&issueReporterID); err != nil {
		t.Fatalf("read issue back: %v", err)
	}
	if issueReporterID != loserOldID {
		t.Errorf("issue.reporter_id = %q, want unchanged %q (FK integrity broken)",
			issueReporterID, loserOldID)
	}

	// Assertion 6: re-running drops the merged pair from the
	// candidate set (loser is now cntrb_deleted=1).
	count2, err := CountCntrbIDCollisions(ctx, store)
	if err != nil {
		t.Fatalf("re-count: %v", err)
	}
	if count2 >= collisions {
		t.Errorf("collision count after merge = %d, want < pre-merge %d "+
			"(merged pair should drop out via cntrb_deleted filter)",
			count2, collisions)
	}
}
