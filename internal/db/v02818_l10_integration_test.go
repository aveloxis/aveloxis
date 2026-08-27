// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestListDedupHandlesStagingCollisions (AVELOXIS_TEST_DB) — the L10
// re-runs' findings on the v0.28.18 list dedup, driven INSIDE ONE
// TRANSACTION that is rolled back: the fixture needs the two UNIQUE
// indexes gone to seed duplicates, and a transactional DROP INDEX holds
// the tables' ACCESS EXCLUSIVE lock until the rollback restores them —
// a concurrent package's RegisterMailingList WAITS instead of hitting
// 42P10 (a CONCURRENTLY drop would open that window), and nothing needs
// cleaning up. Shapes: three registrations of one (group, list) A < B <
// C (winner A) with staging H1 under B AND C (two losers colliding with
// each other), H2 under A and B (the winner's copy survives), H3 under
// C only (repointed); email_message under B repointed; checkpoints
// merged into A; a same-NAMED second group's registration folds into
// the canonical partition; a partition whose loser holds a live worker
// lock is left untouched.
func TestListDedupHandlesStagingCollisions(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	// A "running serve": a second session tagged aveloxis-serve makes
	// worker locks LIVE; closing it turns the same locks into ghosts.
	cfg, err := pgx.ParseConfig(os.Getenv("AVELOXIS_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["application_name"] = "aveloxis-serve"
	fakeServe, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("fake serve connection: %v", err)
	}
	fakeServeOpen := true
	t.Cleanup(func() {
		if fakeServeOpen {
			_ = fakeServe.Close(context.Background())
		}
	})
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	for _, sql := range []string{
		`DROP INDEX IF EXISTS aveloxis_data.idx_rgls_group_email`,
		`DROP INDEX IF EXISTS aveloxis_data.uq_repo_groups_rg_name`,
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	newGroup := func(name string) int64 {
		t.Helper()
		var id int64
		if err := tx.QueryRow(ctx, `INSERT INTO aveloxis_data.repo_groups (rg_name, rg_type) VALUES ($1, 'test') RETURNING repo_group_id`, name).Scan(&id); err != nil {
			t.Fatalf("seed group %s: %v", name, err)
		}
		return id
	}
	groupID := newGroup("_avdedup-list-" + t.Name())
	sameName := newGroup("_avdedup-list-" + t.Name()) // the pre-v0.27.17 duplicate-group shape
	lockedGroupID := newGroup("_avdedup-locked-" + t.Name())
	seedList := func(group int64, lastMonth string, lastRun, lockedAt string, complete bool) int64 {
		t.Helper()
		var id int64
		if err := tx.QueryRow(ctx, `
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
	x := seedList(sameName, "2026-07", "", "", true) // same list under the same-named group
	d := seedList(lockedGroupID, "", "", "", false)
	e := seedList(lockedGroupID, "2026-06", "", "now", false) // live worker lock
	stage := func(rgls, group int64, header string) int64 {
		t.Helper()
		var id int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.mailing_list_staging (rgls_id, repo_group_id, message_id_header, envelope)
			VALUES ($1, $2, $3, '{}'::jsonb) RETURNING mls_id`, rgls, group, header).Scan(&id); err != nil {
			t.Fatalf("seed staging: %v", err)
		}
		return id
	}
	stage(b, groupID, "<h1@_avdedup>")
	stage(c, groupID, "<h1@_avdedup>")
	h2Winner := stage(a, groupID, "<h2@_avdedup>")
	stage(b, groupID, "<h2@_avdedup>")
	stage(c, groupID, "<h3@_avdedup>")
	stage(x, sameName, "<h1@_avdedup>") // a third copy of h1, stamped with the same-named group
	stage(x, sameName, "<h4@_avdedup>") // repointed: rgls AND repo_group_id must follow the winner
	stage(e, lockedGroupID, "<locked@_avdedup>")
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.email_message (repo_group_id, rgls_id, platform_id, message_id_header, list_address, msg_class)
		VALUES ($1, $2, 6, '<em-loser@_avdedup>', 'dev@_avdedup.example', 'mailing_list_only')`, groupID, b); err != nil {
		t.Fatalf("seed email_message: %v", err)
	}

	touched, err := dedupRepoGroupsListServeTx(ctx, tx, slog.Default(), MailingListStaleLock.String())
	if err != nil {
		t.Fatalf("dedup: %v", err)
	}
	if touched == nil {
		t.Fatal("dedup reported nothing to do")
	}
	count := func(sql string, args ...any) int {
		t.Helper()
		var n int
		if err := tx.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return n
	}
	if n := count(`SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id IN ($1, $2, $3, $4)`, a, b, c, x); n != 1 {
		t.Errorf("partition A/B/C/X: %d list rows survived, want 1 (the winner)", n)
	}
	if n := count(`SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, a); n != 1 {
		t.Error("the lowest rgls_id must be the survivor")
	}
	if w, l := count(`SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1`, a),
		count(`SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id IN ($1, $2, $3)`, b, c, x); w != 4 || l != 0 {
		t.Errorf("staging after dedup: winner=%d losers=%d, want 4/0 (h1 once, h2 once, h3 + h4 repointed)", w, l)
	}
	if n := count(`SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1 AND repo_group_id <> $2`, a, groupID); n != 0 {
		t.Errorf("%d repointed staging rows still carry the loser's repo_group_id — DrainList resolves the list's repo from it", n)
	}
	var h2Survivor int64
	if err := tx.QueryRow(ctx, `SELECT mls_id FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1 AND message_id_header = '<h2@_avdedup>'`, a).Scan(&h2Survivor); err != nil {
		t.Fatalf("h2 survivor: %v", err)
	}
	if h2Survivor != h2Winner {
		t.Errorf("h2: the winner's own staging row must survive (mls_id %d), got %d", h2Winner, h2Survivor)
	}
	if n := count(`SELECT count(*) FROM aveloxis_data.email_message WHERE rgls_id = $1`, a); n != 1 {
		t.Errorf("email_message repointed to winner: %d rows, want 1", n)
	}
	var lastMonth string
	var lastRunSet, complete bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(mlls_last_month, ''), mlls_last_run IS NOT NULL, COALESCE(mlls_scan_complete, FALSE)
		FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, a).Scan(&lastMonth, &lastRunSet, &complete); err != nil {
		t.Fatalf("read winner checkpoints: %v", err)
	}
	if lastMonth != "2026-07" || !lastRunSet || complete {
		t.Errorf("winner checkpoints = month %q run-set %v complete %v, want 2026-07 / true / false (GREATEST of every copy incl. the same-named group's; incomplete if any copy was)", lastMonth, lastRunSet, complete)
	}
	if n := count(`SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id IN ($1, $2)`, d, e); n != 2 {
		t.Errorf("the live-locked partition must be left untouched while a serve is connected: %d rows, want 2", n)
	}
	// Idempotent while the lock stays live: a second pass finds no
	// unlocked duplicates, and the stage-10 gate sees one pending partition.
	again, err := dedupRepoGroupsListServeTx(ctx, tx, slog.Default(), MailingListStaleLock.String())
	if err != nil || again != nil {
		t.Fatalf("second pass: touched=%v err=%v, want nothing to do", again, err)
	}
	if n := count(`SELECT count(*) FROM (
			SELECT 1 FROM aveloxis_data.repo_groups_list_serve l
			JOIN (SELECT repo_group_id, CASE WHEN rg_name IS NULL THEN repo_group_id ELSE MIN(repo_group_id) OVER (PARTITION BY rg_name) END AS canonical_group FROM aveloxis_data.repo_groups) g ON g.repo_group_id = l.repo_group_id
			WHERE l.rgls_email IS NOT NULL GROUP BY g.canonical_group, l.rgls_email HAVING count(*) > 1) p`); n != 1 {
		t.Errorf("one duplicate partition must remain pending while its lock is live, got %d", n)
	}
	// `stop serve` → `migrate`: with no aveloxis-serve backend connected,
	// the same lock is a ghost and the partition consolidates.
	if err := fakeServe.Close(ctx); err != nil {
		t.Fatalf("closing fake serve: %v", err)
	}
	fakeServeOpen = false
	// Backend teardown is asynchronous: pg_stat_activity lists the closed
	// session for a few milliseconds more.
	for i := 0; ; i++ {
		var still int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'aveloxis-serve'`).Scan(&still); err != nil {
			t.Fatal(err)
		}
		if still == 0 {
			break
		}
		if i > 100 {
			t.Fatalf("fake serve backend still listed in pg_stat_activity after 10s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	third, err := dedupRepoGroupsListServeTx(ctx, tx, slog.Default(), MailingListStaleLock.String())
	if err != nil || third == nil {
		t.Fatalf("third pass with serve stopped: touched=%v err=%v, want the ghost-locked partition consolidated", third, err)
	}
	if n := count(`SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id IN ($1, $2)`, d, e); n != 1 {
		t.Errorf("ghost-locked partition after serve stopped: %d rows, want 1 (the winner)", n)
	}
	if n := count(`SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1 AND repo_group_id = $2`, d, lockedGroupID); n != 1 {
		t.Errorf("the ghost loser's staging must be repointed to the winner: %d rows", n)
	}
	var dMonth string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(mlls_last_month, '') FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, d).Scan(&dMonth); err != nil {
		t.Fatal(err)
	}
	if dMonth != "2026-06" {
		t.Errorf("ghost loser's checkpoint must merge into the winner: got %q, want 2026-06", dMonth)
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
