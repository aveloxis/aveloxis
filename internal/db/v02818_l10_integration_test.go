// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

// TestListDedupHandlesStagingCollisions (AVELOXIS_TEST_DB) — the L10
// re-runs' findings on the v0.28.18 list dedup. Three registrations of
// one (group, list): A < B < C, winner A. Staging headers: H1 under B AND
// C (two losers colliding with EACH OTHER — the winner never had it), H2
// under A and B (winner's copy must survive), H3 under C only
// (repointed). email_message under B is repointed; the losers'
// checkpoints merge into A; a SECOND partition whose loser holds a live
// worker lock is left untouched. All or nothing, idempotent.
func TestListDedupHandlesStagingCollisions(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	groupID, err := store.UpsertRepoGroup(ctx, "_avdedup-list-"+t.Name(), "test", "")
	if err != nil {
		t.Fatalf("UpsertRepoGroup: %v", err)
	}
	lockedGroupID, err := store.UpsertRepoGroup(ctx, "_avdedup-locked-"+t.Name(), "test", "")
	if err != nil {
		t.Fatalf("UpsertRepoGroup (locked): %v", err)
	}
	// Registered FIRST so it runs LAST (after the row cleanups): nothing
	// else rebuilds the index — every connect helper fast-paths on the
	// schema stamp — and RegisterMailingList's ON CONFLICT needs it (42P10
	// for every sibling test otherwise).
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_rgls_group_email ON aveloxis_data.repo_groups_list_serve (repo_group_id, rgls_email)`)
	})
	// The UNIQUE (repo_group_id, rgls_email) index blocks seeding duplicate
	// registrations on a migrated DB — drop it for the fixture (the dedup
	// is what makes it buildable on fleets that lack it).
	mustExecRetry(ctx, t, store, `DROP INDEX CONCURRENTLY IF EXISTS aveloxis_data.idx_rgls_group_email`)
	seedList := func(group int64, lastMonth string, lastRun, lockedAt string, complete bool) int64 {
		t.Helper()
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repo_groups_list_serve
				(repo_group_id, rgls_email, rgls_name, mlls_system, mlls_last_month, mlls_last_run, mlls_locked_at, mlls_scan_complete)
			VALUES ($1, 'dev@_avdedup.example', 'dev', 'apache_ponymail', $2, NULLIF($3, '')::timestamptz, NULLIF($4, '')::timestamptz, $5)
			RETURNING rgls_id`, group, lastMonth, lastRun, lockedAt, complete).Scan(&id); err != nil {
			t.Fatalf("seed list row: %v", err)
		}
		return id
	}
	a := seedList(groupID, "", "", "", true)
	b := seedList(groupID, "2026-05", "2026-05-01T00:00:00Z", "", false)
	c := seedList(groupID, "2026-03", "", "", true)
	d := seedList(lockedGroupID, "", "", "", false)
	e := seedList(lockedGroupID, "2026-06", "", "now()", false) // live worker lock
	all := []int64{a, b, c, d, e}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = ANY($1)`, all)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.email_message WHERE rgls_id = ANY($1)`, all)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = ANY($1)`, all)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_groups WHERE repo_group_id IN ($1, $2)`, groupID, lockedGroupID)
	})
	stage := func(rgls int64, header string) int64 {
		t.Helper()
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.mailing_list_staging (rgls_id, repo_group_id, message_id_header, envelope)
			VALUES ($1, $2, $3, '{}'::jsonb) RETURNING mls_id`, rgls, groupID, header).Scan(&id); err != nil {
			t.Fatalf("seed staging: %v", err)
		}
		return id
	}
	stage(b, "<h1@_avdedup>")
	stage(c, "<h1@_avdedup>")
	h2Winner := stage(a, "<h2@_avdedup>")
	stage(b, "<h2@_avdedup>")
	stage(c, "<h3@_avdedup>")
	stage(e, "<locked@_avdedup>")
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.email_message (repo_group_id, rgls_id, platform_id, message_id_header, list_address, msg_class)
		VALUES ($1, $2, 6, '<em-loser@_avdedup>', 'dev@_avdedup.example', 'mailing_list_only')`, groupID, b)

	var errs []error
	dedupRepoGroupsListServe(ctx, store, slog.Default(), &errs)
	if len(errs) != 0 {
		t.Fatalf("dedup errors: %v", errs)
	}
	var survivors, winnerStaging, loserStaging, emWinner, lockedRows int
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id IN ($1, $2, $3)`, &survivors, a, b, c)
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1`, &winnerStaging, a)
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id IN ($1, $2)`, &loserStaging, b, c)
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_data.email_message WHERE rgls_id = $1`, &emWinner, a)
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id IN ($1, $2)`, &lockedRows, d, e)
	if survivors != 1 {
		t.Errorf("partition A/B/C: %d list rows survived, want 1 (the winner)", survivors)
	}
	if winnerStaging != 3 || loserStaging != 0 {
		t.Errorf("staging after dedup: winner=%d losers=%d, want 3/0 (h1 once, h2 once, h3 repointed)", winnerStaging, loserStaging)
	}
	var h2Survivor int64
	mustQueryRowRetry(ctx, t, store, `SELECT mls_id FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1 AND message_id_header = '<h2@_avdedup>'`, &h2Survivor, a)
	if h2Survivor != h2Winner {
		t.Errorf("h2: the winner's own staging row must survive (mls_id %d), got %d", h2Winner, h2Survivor)
	}
	if emWinner != 1 {
		t.Errorf("email_message repointed to winner: %d rows, want 1", emWinner)
	}
	var lastMonth string
	var lastRunSet, complete bool
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(mlls_last_month, ''), mlls_last_run IS NOT NULL, COALESCE(mlls_scan_complete, FALSE)
		FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, a).Scan(&lastMonth, &lastRunSet, &complete); err != nil {
		t.Fatalf("read winner checkpoints: %v", err)
	}
	if lastMonth != "2026-05" || !lastRunSet || complete {
		t.Errorf("winner checkpoints = month %q run-set %v complete %v, want 2026-05 / true / false (GREATEST of the losers; incomplete if any copy was)", lastMonth, lastRunSet, complete)
	}
	if lockedRows != 2 {
		t.Errorf("the live-locked partition must be left untouched: %d rows, want 2", lockedRows)
	}
	// Idempotent: a second pass finds no unlocked duplicates and touches nothing.
	dedupRepoGroupsListServe(ctx, store, slog.Default(), &errs)
	if len(errs) != 0 {
		t.Fatalf("second pass errors: %v", errs)
	}
}

// TestRefreshAllRepoAggregatesRefusesWhenLockHeld (AVELOXIS_TEST_DB): a
// second holder of DMAggregatesAdvisoryLockID gets the typed sentinel
// instead of interleaving with the first pass.
func TestRefreshAllRepoAggregatesRefusesWhenLockHeld(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	holder, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(holder.Release)
	var got bool
	if err := holder.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, DMAggregatesAdvisoryLockID).Scan(&got); err != nil || !got {
		t.Fatalf("could not take the aggregate lock for the fixture: ok=%v err=%v", got, err)
	}
	t.Cleanup(func() {
		var unlocked bool
		_ = holder.QueryRow(context.Background(), `SELECT pg_advisory_unlock($1)`, DMAggregatesAdvisoryLockID).Scan(&unlocked)
	})
	err = store.RefreshAllRepoAggregates(ctx, slog.Default())
	if !errors.Is(err, ErrAggregateRebuildRunning) {
		t.Fatalf("RefreshAllRepoAggregates with the lock held = %v, want ErrAggregateRebuildRunning", err)
	}
}

// TestRunOnceSeedRecordsWhenStampProvesApplied (AVELOXIS_TEST_DB): on a
// database whose stamp is at/above the step's introducing version, the
// seed records the label without running anything; a second seed is a
// no-op; a label below the stamp's proof is left alone.
func TestRunOnceSeedRecordsWhenStampProvesApplied(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	stamp := store.GetSchemaVersion(ctx)
	if stamp == "" {
		t.Fatal("scratch DB carries no schema stamp after Migrate")
	}
	label := "_avseed test label " + t.Name()
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
	})
	runOnceSeedIfApplied(ctx, store, slog.Default(), label, "0.27.37")
	var n int
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, &n, label)
	if n != 1 {
		t.Fatalf("seed with stamp %s >= 0.27.37 recorded %d rows, want 1", stamp, n)
	}
	runOnceSeedIfApplied(ctx, store, slog.Default(), label, "0.27.37")
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, &n, label)
	if n != 1 {
		t.Fatalf("second seed changed the ledger: %d rows", n)
	}
	other := label + " future"
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, other)
	})
	runOnceSeedIfApplied(ctx, store, slog.Default(), other, "999.0.0")
	mustQueryRowRetry(ctx, t, store, `SELECT count(*) FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, &n, other)
	if n != 0 {
		t.Fatalf("a stamp below the step's introducing version must not seed, got %d rows", n)
	}
}
