// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Integration test for the v0.22.2 cntrb_id data migration.
//
// Validates the end-to-end behavior of MigrateCntrbIDsBatch:
//
//   - random-UUID contributor with non-zero platform_user_id is
//     correctly rewritten to deterministic PlatformUUID form.
//   - ON UPDATE CASCADE (v0.22.1) propagates the rewrite to a
//     child issue row's reporter_id automatically.
//   - The collision case (target deterministic UUID already used
//     by a different row) is correctly counted but NOT migrated.
//   - Re-running is idempotent — the second call processes zero
//     rows because the original random row is now deterministic
//     and excluded from the candidate set.
//
// Gated on AVELOXIS_TEST_DB. Skips when unset.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestMigrateCntrbIDsBatchEndToEnd(t *testing.T) {
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
		t.Fatalf("RunMigrations: %v", err)
	}

	// Synthetic users:
	//   alice — gh_user_id=12345, random old cntrb_id. Target =
	//     PlatformUUID(1, 12345) = 0x01 + 0x00003039 + zeros.
	//   bob — gh_user_id=67890, random old cntrb_id. NO COLLISION.
	//     Target = PlatformUUID(1, 67890) = 0x01 + 0x00010932 + zeros.
	//   collisionVictim — pre-existing row with cntrb_id =
	//     PlatformUUID(1, 99999) (so an "alice2" candidate trying
	//     to migrate to the same target hits the collision filter).
	//   alice2 — gh_user_id=99999, random old cntrb_id, will hit
	//     collision against collisionVictim.
	//
	// All four go through the candidate query; alice + bob should
	// migrate (safe), alice2 should NOT (collision).
	const (
		aliceLogin              = "_av_mig_alice"
		bobLogin                = "_av_mig_bob"
		alice2Login             = "_av_mig_alice2"
		collisionVictimLogin    = "_av_mig_victim"
		aliceOldID              = "11111111-1111-4111-8111-111111111111"
		aliceTargetID           = "01000030-3900-0000-0000-000000000000" // PlatformUUID(1, 12345)
		bobOldID                = "22222222-2222-4222-8222-222222222222"
		bobTargetID             = "01000109-3200-0000-0000-000000000000" // PlatformUUID(1, 67890)
		alice2OldID             = "33333333-3333-4333-8333-333333333333"
		alice2TargetCollidingID = "01000186-9f00-0000-0000-000000000000" // PlatformUUID(1, 99999)
	)

	// Cleanup any leftover state from a prior run. Order: children
	// first, then identities, then repos, then contributors.
	// v0.27.120 bounded-retry helpers: on 2026-09-01 a full-suite run's
	// concurrent-migrate deadlock storm made THIS pre-clean a 40P01
	// victim (logged non-fatally), and the seed then collided with the
	// prior run's surviving residue (contributors_pkey 23505). The
	// retry helper absorbs the deadlock so the pre-clean actually
	// cleans.
	cleanup := func(sql string, args ...any) {
		cleanupExecRetry(ctx, store, sql, args...)
	}
	for _, login := range []string{aliceLogin, bobLogin, alice2Login, collisionVictimLogin} {
		cleanup(`DELETE FROM aveloxis_data.issues WHERE reporter_id IN (
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
	cleanup(`DELETE FROM aveloxis_data.repos WHERE repo_owner = '_av_mig' AND repo_name = 'test'`)

	// Seed a repo_group + repo so we have a parent for the issue.
	var repoGroupID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repo_groups (rg_name, rg_description)
		VALUES ('_av_mig_grp', 'cntrb_id migration integration test')
		ON CONFLICT DO NOTHING RETURNING repo_group_id`).Scan(&repoGroupID); err != nil {
		if err := store.pool.QueryRow(ctx,
			`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = '_av_mig_grp'`).Scan(&repoGroupID); err != nil {
			t.Fatalf("repo_group setup: %v", err)
		}
	}
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_owner, repo_name, repo_git, platform_id, repo_group_id)
		VALUES ('_av_mig', 'test', 'https://github.com/_av_mig/test', 1, $1)
		RETURNING repo_id`, repoGroupID).Scan(&repoID); err != nil {
		t.Fatalf("repo setup: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// Insert alice + identity + a child issue pointing at alice.
	type seedRow struct {
		login, oldID string
		userID       int64
	}
	seeds := []seedRow{
		{aliceLogin, aliceOldID, 12345},
		{bobLogin, bobOldID, 67890},
		{alice2Login, alice2OldID, 99999},
	}
	for _, s := range seeds {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login)
			VALUES ($1::uuid, $2)`, s.oldID, s.login)
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.contributor_identities
				(cntrb_id, platform_id, platform_user_id, login, name, email, avatar_url, profile_url, node_id, user_type, is_admin)
			VALUES ($1::uuid, 1, $2, $3, '', '', '', '', '', '', FALSE)`,
			s.oldID, s.userID, s.login)
	}

	// Seed the collision victim with the SAME cntrb_id alice2's
	// target would compute to. Different login, so no on-login
	// collision with the candidate seeds above.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login)
		VALUES ($1::uuid, $2)`, alice2TargetCollidingID, collisionVictimLogin); err != nil {
		t.Fatalf("seed collision victim: %v", err)
	}

	// Seed an issue with reporter_id = alice's random cntrb_id, so
	// we can verify cascade after the migration.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues
			(repo_id, platform_issue_id, issue_number, reporter_id)
		VALUES ($1, 99999998, 12345, $2::uuid)`, repoID, aliceOldID); err != nil {
		t.Fatalf("seed issue: %v", err)
	}

	// Count phase.
	counts, err := CountCntrbIDMigrationCandidates(ctx, store)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	t.Logf("counts: total=%d safe=%d collisions=%d", counts.Total, counts.Safe, counts.Collisions)
	// Our 3 seeded rows: 2 safe (alice + bob), 1 collision (alice2
	// targeting collisionVictim's cntrb_id). The DB may have other
	// random-UUID rows from earlier integration tests; we only check
	// the lower bounds here.
	if counts.Safe < 2 {
		t.Errorf("counts.Safe = %d, want >= 2 (alice + bob)", counts.Safe)
	}
	if counts.Collisions < 1 {
		t.Errorf("counts.Collisions = %d, want >= 1 (alice2 vs victim)", counts.Collisions)
	}

	// Live migration.
	migrated, err := MigrateCntrbIDsBatch(ctx, store, 1000)
	if err != nil {
		t.Fatalf("MigrateCntrbIDsBatch: %v", err)
	}
	t.Logf("migrated %d rows in first batch", migrated)

	// Verify alice was migrated to her target deterministic UUID.
	var aliceNewCntrbID string
	if err := store.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
		aliceLogin).Scan(&aliceNewCntrbID); err != nil {
		t.Fatalf("read alice back: %v", err)
	}
	if aliceNewCntrbID != aliceTargetID {
		t.Errorf("alice.cntrb_id after migration = %q, want %q", aliceNewCntrbID, aliceTargetID)
	}

	// Verify CASCADE propagated to alice's issue.
	var issueReporterID string
	if err := store.pool.QueryRow(ctx,
		`SELECT reporter_id::text FROM aveloxis_data.issues WHERE repo_id = $1 AND issue_number = 12345`,
		repoID).Scan(&issueReporterID); err != nil {
		t.Fatalf("read issue back: %v", err)
	}
	if issueReporterID != aliceTargetID {
		t.Errorf("issue.reporter_id after migration = %q, want %q — CASCADE did not propagate",
			issueReporterID, aliceTargetID)
	}

	// Verify bob migrated too.
	var bobNewCntrbID string
	_ = store.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
		bobLogin).Scan(&bobNewCntrbID)
	if bobNewCntrbID != bobTargetID {
		t.Errorf("bob.cntrb_id after migration = %q, want %q", bobNewCntrbID, bobTargetID)
	}

	// Verify alice2 was NOT migrated (collision case).
	var alice2NowCntrbID string
	_ = store.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
		alice2Login).Scan(&alice2NowCntrbID)
	if alice2NowCntrbID != alice2OldID {
		t.Errorf("alice2.cntrb_id after migration = %q, want %q (unchanged — collision case skipped)",
			alice2NowCntrbID, alice2OldID)
	}

	// Idempotency: re-run. alice + bob are now deterministic so they
	// fall out of the candidate set. alice2 still collides. So the
	// second batch should migrate nothing related to our seeded set
	// (it might migrate other unrelated leftover rows from earlier
	// test runs, hence why we don't assert == 0; we assert the
	// state of OUR rows is preserved).
	_, err = MigrateCntrbIDsBatch(ctx, store, 1000)
	if err != nil {
		t.Fatalf("idempotency MigrateCntrbIDsBatch: %v", err)
	}
	_ = store.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
		aliceLogin).Scan(&aliceNewCntrbID)
	if aliceNewCntrbID != aliceTargetID {
		t.Errorf("idempotency: alice.cntrb_id changed on second run = %q, want unchanged %q",
			aliceNewCntrbID, aliceTargetID)
	}
}

func TestPrecheckCntrbIDCascadeReturnsEmptyOnHealthySchema(t *testing.T) {
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
		t.Fatalf("RunMigrations: %v", err)
	}

	missing, err := PrecheckCntrbIDCascade(ctx, store, logger)
	if err != nil {
		t.Fatalf("PrecheckCntrbIDCascade: %v", err)
	}
	if len(missing) > 0 {
		t.Errorf("PrecheckCntrbIDCascade returned %d missing constraints after RunMigrations, want 0: %v",
			len(missing), missing)
	}
}
