// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestListDedupHandlesStagingCollisions (AVELOXIS_TEST_DB) — the L10
// re-runs' findings on the v0.28.18 list dedup, driven INSIDE ONE
// TRANSACTION that is rolled back: the fixture needs the two UNIQUE
// indexes gone to seed duplicates, and a transactional DROP INDEX holds
// the tables' ACCESS EXCLUSIVE lock until the rollback restores them —
// a concurrent package's RegisterMailingList WAITS instead of hitting
// 42P10 (a CONCURRENTLY drop would open that window), and nothing needs
// cleaning up. Partitions: P1 = A < B < C (+ X under a same-NAMED
// group) with staging H1 under B AND C (two losers colliding with each
// other), H2 under A and B (the winner's copy survives), H3 under C, H4
// under X (repointed incl. repo_group_id); email_message under B;
// checkpoints merged into A. P2 = D/E with E carrying a young worker
// lock (a ghost once no serve runs). THE rule: while a fake
// aveloxis-serve session is connected NOTHING is consolidated (the drain
// holds no lock, so no row is provably idle); handing that session's
// own PID to the probe (this process's pool) hides it; closing it lets
// every partition consolidate, ghost lock included.
func TestListDedupHandlesStagingCollisions(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	cfg, err := pgx.ParseConfig(os.Getenv("AVELOXIS_TEST_DB"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["application_name"] = "aveloxis-serve"
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })

	// The seventh pass's red proof: a plain pg_stat_activity read FIXES the
	// transaction's activity snapshot; a serve connecting afterwards must
	// still be seen by the probe (a same-statement clear ran AFTER the
	// read and never saw it).
	var before int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = 'aveloxis-serve'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Skipf("an aveloxis-serve backend is already connected to the scratch DB (%d) — cannot run the liveness arms", before)
	}
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
	if seen, err := serveBackendsBeyondOwnPool(ctx, tx, nil); err != nil || !seen {
		t.Fatalf("probe after a serve connected mid-transaction: seen=%v err=%v — the activity snapshot must be cleared BEFORE the read", seen, err)
	}
	// Own-backend exclusion is by server PID: the fake session counted as
	// OURS must not read as a running serve.
	if seen, err := serveBackendsBeyondOwnPool(ctx, tx, func() []int32 { return []int32{int32(fakeServe.PgConn().PID())} }); err != nil || seen {
		t.Fatalf("probe with the serve session's PID excluded as our own: seen=%v err=%v, want false", seen, err)
	}

	// Lock parent before child in ONE statement (the order an explicit
	// "insert a group, then its list rows" transaction takes), then the
	// transactional DROPs. Neither order is deadlock-free against every
	// concurrent shape — an autocommit RegisterMailingList INSERT takes
	// its deferred-FK share lock on repo_groups at commit, after its rgls
	// row lock — so the acquisition retries the bounded 40P01 way
	// (v0.27.120); the window is one INSERT's commit interval.
	lockSQL := `LOCK TABLE aveloxis_data.repo_groups, aveloxis_data.repo_groups_list_serve IN ACCESS EXCLUSIVE MODE`
	for attempt := 1; ; attempt++ {
		_, err := tx.Exec(ctx, lockSQL)
		if err == nil {
			break
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "40P01" || attempt >= 5 {
			t.Fatalf("%s (attempt %d): %v", lockSQL, attempt, err)
		}
		_ = tx.Rollback(ctx)
		time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		if tx, err = store.pool.Begin(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		`DROP INDEX IF EXISTS aveloxis_data.uq_repo_groups_rg_name`,
		`DROP INDEX IF EXISTS aveloxis_data.idx_rgls_group_email`,
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
	lockedGroup := newGroup("_avdedup-locked-" + t.Name())
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
	x := seedList(sameName, "2026-07", "", "", true)
	d := seedList(lockedGroup, "", "", "", false)
	e := seedList(lockedGroup, "2026-06", "", "now", false) // young worker lock
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
	stage(e, lockedGroup, "<locked@_avdedup>")
	if _, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.email_message (repo_group_id, rgls_id, platform_id, message_id_header, list_address, msg_class)
		VALUES ($1, $2, 6, '<em-loser@_avdedup>', 'dev@_avdedup.example', 'mailing_list_only')`, groupID, b); err != nil {
		t.Fatalf("seed email_message: %v", err)
	}
	count := func(sql string, args ...any) int {
		t.Helper()
		var n int
		if err := tx.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return n
	}
	survivors := func(ids ...int64) int {
		t.Helper()
		return count(`SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = ANY($1::bigint[])`, ids)
	}

	pending := func() int {
		t.Helper()
		n, err := listDedupPending(ctx, tx)
		if err != nil {
			t.Fatalf("listDedupPending: %v", err)
		}
		return n
	}
	// Pass 1: another serve is "running" (the fake session) → nothing.
	touched, err := dedupRepoGroupsListServeTx(ctx, tx, slog.Default(), nil)
	if err != nil || touched != nil {
		t.Fatalf("pass 1 with a serve connected: touched=%v err=%v, want nothing consolidated", touched, err)
	}
	if n := survivors(a, b, c, x, d, e); n != 6 {
		t.Errorf("with a serve connected every list row must survive: %d, want 6", n)
	}
	if n := pending(); n != 2 {
		t.Errorf("pending partitions after pass 1 = %d, want 2", n)
	}

	// Pass 2: `stop serve` → `migrate`. Backend teardown is asynchronous —
	// wait until the probe no longer sees the closed session.
	if err := fakeServe.Close(ctx); err != nil {
		t.Fatalf("closing fake serve: %v", err)
	}
	fakeServeOpen = false
	for i := 0; ; i++ {
		still, err := serveBackendsBeyondOwnPool(ctx, tx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !still {
			break
		}
		if i > 100 {
			t.Fatalf("fake serve backend still listed in pg_stat_activity after 10s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	second, err := dedupRepoGroupsListServeTx(ctx, tx, slog.Default(), nil)
	if err != nil || second == nil {
		t.Fatalf("pass 2 with serve stopped: touched=%v err=%v, want every partition consolidated", second, err)
	}
	if n := survivors(a, b, c, x); n != 1 || survivors(a) != 1 {
		t.Errorf("P1: %d list rows survived, want 1 (the lowest rgls_id)", n)
	}
	if w, l := count(`SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1`, a),
		count(`SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = ANY($1::bigint[])`, []int64{b, c, x}); w != 4 || l != 0 {
		t.Errorf("P1 staging: winner=%d losers=%d, want 4/0 (h1 once, h2 once, h3 + h4 repointed)", w, l)
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
		t.Errorf("P1 checkpoints = month %q run-set %v complete %v, want 2026-07 / true / false", lastMonth, lastRunSet, complete)
	}
	if n := survivors(d, e); n != 1 {
		t.Errorf("P2 (ghost lock) after serve stopped: %d rows, want 1", n)
	}
	// The deferred FK email_message.rgls_id → repo_groups_list_serve is
	// checked at COMMIT, which this rolled-back fixture never reaches —
	// fire the queued checks now. This proves every loser's email_message
	// rows landed on a surviving winner (a missing or mis-joined repoint
	// fails here); it does NOT prove statement order — a deferred NO
	// ACTION check re-reads current state, so delete-then-repoint would
	// also pass (verified on PG 18). The five-step order pin is a
	// convention, not an FK guarantee.
	if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
		t.Fatalf("deferred FK checks after the dedup: %v (every loser's email_message rows must be repointed to a surviving winner)", err)
	}
	if n := count(`SELECT count(*) FROM aveloxis_ops.mailing_list_staging WHERE rgls_id = $1 AND repo_group_id = $2`, d, lockedGroup); n != 1 {
		t.Errorf("P2's loser staging must be repointed to the winner: %d rows", n)
	}
	var dMonth string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(mlls_last_month, '') FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = $1`, d).Scan(&dMonth); err != nil {
		t.Fatal(err)
	}
	if dMonth != "2026-06" {
		t.Errorf("P2's loser checkpoint must merge into the winner: got %q, want 2026-06", dMonth)
	}
	if n := pending(); n != 0 {
		t.Errorf("pending partitions after pass 2 = %d, want 0", n)
	}
	// Idempotent: nothing unlocked remains.
	third, err := dedupRepoGroupsListServeTx(ctx, tx, slog.Default(), nil)
	if err != nil || third != nil {
		t.Fatalf("third pass: touched=%v err=%v, want nothing to do", third, err)
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

// TestEmailMessageIndexGateRefusesWithoutIndex (AVELOXIS_TEST_DB) — the
// refusal arm, red-first (v0.27.145): a transactional DROP INDEX of one
// repos-side email_message index makes the gate refuse, naming that
// index and the migrate command, while the list-side parent still reads
// ready in the same transaction; the rollback restores the index and
// the pool reads ready again. The DROP holds email_message's ACCESS
// EXCLUSIVE lock for the few statements until the rollback (a
// concurrent writer waits), acquired the bounded 40P01/55P03 way.
func TestEmailMessageIndexGateRefusesWithoutIndex(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	for _, parent := range []string{"repos", "repo_groups_list_serve"} {
		if err := emailMessageFKIndexesReadyFor(ctx, store.pool, parent); err != nil {
			t.Fatalf("after Migrate the gate must read ready for %s: %v", parent, err)
		}
	}
	for attempt := 1; ; attempt++ {
		tx, err := store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; DROP INDEX aveloxis_data.idx_email_message_signaled_repo_id`)
		if err != nil {
			_ = tx.Rollback(ctx)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || (pgErr.Code != "40P01" && pgErr.Code != "55P03") || attempt >= 5 {
				t.Fatalf("transactional DROP INDEX (attempt %d): %v", attempt, err)
			}
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
			continue
		}
		err = emailMessageFKIndexesReadyFor(ctx, tx, "repos")
		if err == nil || !strings.Contains(err.Error(), "idx_email_message_signaled_repo_id") || !strings.Contains(err.Error(), "aveloxis migrate --skip-views") {
			t.Errorf("gate with the signaled_repo_id index dropped = %v; want a refusal naming the index and the migrate", err)
		}
		// Copilot round 6: a VALID index led by the column but partial on an
		// UNRELATED predicate must not read as ready — the FK check and the
		// repoint cannot use it for values outside its predicate.
		if _, err := tx.Exec(ctx, `CREATE INDEX idx_email_message_decoy_signaled ON aveloxis_data.email_message (signaled_repo_id) WHERE signaled_repo_id > 5`); err != nil {
			t.Fatalf("decoy partial index: %v", err)
		}
		if err := emailMessageFKIndexesReadyFor(ctx, tx, "repos"); err == nil || !strings.Contains(err.Error(), "idx_email_message_signaled_repo_id") {
			t.Errorf("a partial index on an unrelated predicate must not satisfy the gate, got %v", err)
		}
		// …while the real shape — partial on exactly (col IS NOT NULL) — does.
		if _, err := tx.Exec(ctx, `CREATE INDEX idx_email_message_decoy_notnull ON aveloxis_data.email_message (signaled_repo_id) WHERE signaled_repo_id IS NOT NULL`); err != nil {
			t.Fatalf("IS NOT NULL partial index: %v", err)
		}
		if err := emailMessageFKIndexesReadyFor(ctx, tx, "repos"); err != nil {
			t.Errorf("a partial index on exactly (col IS NOT NULL) must satisfy the gate: %v", err)
		}
		if !errors.Is(err, ErrEmailMessageIndexesNotReady) {
			t.Errorf("the not-ready refusal must wrap ErrEmailMessageIndexesNotReady (callers report it as an unmet precondition), got %v", err)
		}
		if err := emailMessageFKIndexesReadyFor(ctx, tx, "repo_groups_list_serve"); err != nil {
			t.Errorf("the list-side parent must still read ready (only a repos-side index is gone): %v", err)
		}
		_ = tx.Rollback(ctx)
		break
	}
	if err := emailMessageFKIndexesReadyFor(ctx, store.pool, "repos"); err != nil {
		t.Fatalf("after the rollback the pool must read ready again: %v", err)
	}
}

// TestDedupBatchPagesPastCollectingHead (AVELOXIS_TEST_DB) — the SR-19
// stall, red-first: pair A sorts first in lower_git order and has a side
// mid-collection; pair B is mergeable. Pre-fix a batch window of the
// first N groups was filled by A's skip every round and merged == 0
// read as "done" with B never reached. Now the batch's window excludes
// collecting pairs (B merges, A is untouched) while the dry-run read
// still lists A, flagged, for the operator.
func TestDedupBatchPagesPastCollectingHead(t *testing.T) {
	ctx, store := caseConnect(t)
	const slugA, slugB = "_avdedup_pga", "_avdedup_pgb"
	cleanupDedupRepos(ctx, t, store, slugA)
	cleanupDedupRepos(ctx, t, store, slugB)
	t.Cleanup(func() {
		cleanupDedupRepos(ctx, t, store, slugA)
		cleanupDedupRepos(ctx, t, store, slugB)
	})
	dropCaseUniqueIndex(ctx, t, store, func() {
		cleanupDedupRepos(ctx, t, store, slugA)
		cleanupDedupRepos(ctx, t, store, slugB)
	})
	gid := defaultRepoGroup(ctx, t, store)
	seedPair := func(slug string) (winnerID, loserID int64) {
		t.Helper()
		winnerURL := "https://github.com/" + slug + "_Org/Repo"
		for i, url := range []string{winnerURL, strings.ToLower(winnerURL)} {
			var id int64
			if err := store.pool.QueryRow(ctx, `
				INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
				VALUES ($1, 1, $2, 'Repo', $3) RETURNING repo_id`, gid, url, slug+"_Org").Scan(&id); err != nil {
				t.Fatalf("seed %s: %v", url, err)
			}
			if _, err := store.pool.Exec(ctx, `
				INSERT INTO aveloxis_ops.collection_queue (repo_id, status, due_at, last_collected)
				VALUES ($1, 'queued', NOW(), NOW()) ON CONFLICT (repo_id) DO NOTHING`, id); err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				winnerID = id
			} else {
				loserID = id
			}
		}
		return winnerID, loserID
	}
	aWinner, aLoser := seedPair(slugA)
	bWinner, bLoser := seedPair(slugB)
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET status = 'collecting' WHERE repo_id = $1`, aLoser); err != nil {
		t.Fatal(err)
	}
	lowerA := strings.ToLower("https://github.com/" + slugA + "_Org/Repo")
	lowerB := strings.ToLower("https://github.com/" + slugB + "_Org/Repo")

	// The dry-run read keeps A in view, flagged, ahead of B.
	shown, err := sampleCaseVariantRepoDups(ctx, store, 10000, false)
	if err != nil {
		t.Fatal(err)
	}
	var sawA, sawB bool
	for _, p := range shown {
		switch p.LowerGit {
		case lowerA:
			sawA = true
			if !p.Collecting {
				t.Error("dry-run read must flag pair A as collecting")
			}
		case lowerB:
			sawB = true
		}
	}
	if !sawA || !sawB {
		t.Fatalf("dry-run read must list both pairs: A=%v B=%v", sawA, sawB)
	}
	// The batch window drops A entirely.
	window, err := sampleCaseVariantRepoDups(ctx, store, 10000, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range window {
		if p.LowerGit == lowerA {
			t.Fatal("the batch window must not contain the collecting pair")
		}
	}
	// A window sized to every mergeable pair merges B and never touches
	// A (the fixture shares the scratch DB, so residue pairs may ride
	// along — they are other fixtures' leftovers and merge harmlessly).
	merged, skipped, err := DedupCaseVariantReposBatch(ctx, store, max(len(window), 1))
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if merged < 1 {
		t.Fatalf("merged=%d skipped=%d — pair B beyond the collecting head must merge", merged, skipped)
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, bWinner, bLoser).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("pair B must be consolidated to its winner, found %d of 2 rows", n)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, aWinner, aLoser).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("the collecting pair A must be left intact, found %d of 2 rows", n)
	}
	// It is still there for the rerun, flagged.
	if p := findPairByLowerGit(ctx, t, store, lowerA); p == nil || !p.Collecting {
		t.Errorf("pair A must remain a flagged candidate for the rerun, got %+v", p)
	}

	// Convergence (SR-19): once A's job finishes, the documented rerun
	// merges it and A leaves the candidate set — asserted for A alone,
	// since the shared scratch DB may carry other fixtures' residue.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET status = 'queued' WHERE repo_id = $1`, aLoser); err != nil {
		t.Fatal(err)
	}
	window, err = sampleCaseVariantRepoDups(ctx, store, 10000, true)
	if err != nil {
		t.Fatal(err)
	}
	if merged, _, err := DedupCaseVariantReposBatch(ctx, store, max(len(window), 1)); err != nil || merged < 1 {
		t.Fatalf("rerun after A's job finished: merged=%d err=%v — the rerun must merge A", merged, err)
	}
	if p := findPairByLowerGit(ctx, t, store, lowerA); p != nil {
		t.Errorf("after the rerun pair A must have left the candidate set, got %+v", p)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, aWinner, aLoser).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("pair A must be consolidated to its winner on the rerun, found %d of 2 rows", n)
	}
}
