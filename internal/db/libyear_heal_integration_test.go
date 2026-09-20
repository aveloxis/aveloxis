// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 — the behavioural tier for HealUnknownLibyear. The predicate and
// the keyset walk only exist against a real database, so this is where they
// are actually exercised: the dry run must change nothing, --apply must NULL
// exactly the uncomputable rows, and a second run must be a no-op.
//
// The rows that matter are the ones no registry answer can ever fix: a
// dependency declared with NO pinned version has no release date to measure
// age from. Rows WITH a pinned version are left alone even when their date
// is missing, because v0.29.56 fixed the resolvers behind most of them and
// re-analysis fills real dates in — NULLing them here would erase rows the
// next collection pass is about to answer properly.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestHealUnknownLibyearIntegration(t *testing.T) {
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
	testMigrate(ctx, t, store)

	// A repo of our own, cleaned up regardless of outcome.
	const repoGit = "https://example.invalid/_avheal/libyear-heal"
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'libyear-heal', '_avheal', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_name = EXCLUDED.repo_name
		RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	if _, err := store.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID); err != nil {
		t.Fatalf("clear fixtures: %v", err)
	}

	// name, current_version, current_release_date, latest_release_date, libyear
	type seed struct {
		name    string
		version string
		curDate string
		latDate string
		libyear *float64
	}
	num := func(f float64) *float64 { return &f }
	seeds := []seed{
		// UNCOMPUTABLE: no pinned version. These are the target.
		{"unpinned-zero", "", "", "2024-01-01T00:00:00Z", num(0)},
		{"unpinned-nonzero", "", "", "", num(1.5)},
		// COMPUTABLE, and correct: must survive untouched.
		{"pinned-real", "1.0.0", "2022-01-01T00:00:00Z", "2024-01-01T00:00:00Z", num(2)},
		{"pinned-genuine-zero", "1.0.0", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", num(0)},
		// PINNED but dateless: deliberately NOT healed — re-analysis owns it.
		{"pinned-no-date", "1.0.0", "", "2024-01-01T00:00:00Z", num(0)},
		// Already NULL: nothing to do, and it must not be counted as work.
		{"already-null", "", "", "", nil},
	}
	for _, s := range seeds {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_libyear
			  (repo_id, name, package_manager, current_version, current_release_date, latest_release_date, libyear)
			VALUES ($1, $2, 'pypi', $3, $4, $5, $6)`,
			repoID, s.name, s.version, s.curDate, s.latDate, s.libyear); err != nil {
			t.Fatalf("seed %s: %v", s.name, err)
		}
	}

	libyearOf := func(name string) (val *float64) {
		if err := store.pool.QueryRow(ctx,
			`SELECT libyear FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 AND name = $2`,
			repoID, name).Scan(&val); err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return val
	}

	// A DRY RUN must change nothing at all.
	candidates, updated, err := store.HealUnknownLibyear(ctx, false)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if updated != 0 {
		t.Errorf("dry run reported %d rows updated, want 0 — the preview must not write", updated)
	}
	if candidates < 2 {
		t.Errorf("dry run found %d candidates, want at least the 2 unpinned rows", candidates)
	}
	for _, name := range []string{"unpinned-zero", "unpinned-nonzero"} {
		if libyearOf(name) == nil {
			t.Errorf("%s was NULLed by a DRY RUN", name)
		}
	}

	// --apply NULLs exactly the uncomputable rows.
	_, updated, err = store.HealUnknownLibyear(ctx, true)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if updated < 2 {
		t.Errorf("apply updated %d rows, want at least the 2 unpinned ones", updated)
	}
	for _, name := range []string{"unpinned-zero", "unpinned-nonzero"} {
		if libyearOf(name) != nil {
			t.Errorf("%s still carries a libyear after --apply — it names no version, so it can never be computed", name)
		}
	}
	// Everything computable is untouched, including a GENUINE zero: that is
	// the whole distinction this release exists to preserve.
	for name, want := range map[string]float64{"pinned-real": 2, "pinned-genuine-zero": 0, "pinned-no-date": 0} {
		got := libyearOf(name)
		if got == nil {
			t.Errorf("%s was NULLed — only rows with no pinned version may be", name)
			continue
		}
		if *got != want {
			t.Errorf("%s libyear = %v, want %v", name, *got, want)
		}
	}

	// Idempotent: a second apply finds nothing left to do for this repo.
	before := candidates
	candidates2, updated2, err := store.HealUnknownLibyear(ctx, true)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if candidates2 >= before {
		t.Errorf("second run still sees %d candidates (first saw %d) — the heal did not converge", candidates2, before)
	}
	_ = updated2
}

// TestHealUnknownLibyearInterruptKeepsFinishedWindows (AVELOXIS_TEST_DB) is
// the behavioural half of what `aveloxis heal-libyear` now tells an operator
// when a Ctrl-C or `aveloxis stop` lands mid-pass: "the windows already
// written stay written ... rerun to finish". Both halves of that sentence are
// claims about this function, so both are driven here — the counts come back
// with the cancellation rather than zeroed, the rows of a finished window are
// still NULL afterwards, and a re-run completes the rest.
//
// The walk is parked mid-flight deterministically, not by timing: a second
// connection holds a row lock on the row in the LATER window, so the healer
// applies the earlier window, commits it (each window is its own statement),
// and then blocks on the UPDATE it cannot take until the test cancels. The
// only clock in here is a ceiling that keeps a broken run from hanging the
// suite; no assertion depends on its value.
func TestHealUnknownLibyearInterruptKeepsFinishedWindows(t *testing.T) {
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
	testMigrate(ctx, t, store)

	const repoGit = "https://example.invalid/_avheal/libyear-interrupt"
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'libyear-interrupt', '_avheal', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_name = EXCLUDED.repo_name
		RETURNING repo_id`, repoGit).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// Two uncomputable rows, one per keyset window. Explicit primary keys
	// above everything present, on a window boundary, so which window each
	// row lands in is a fact rather than a hope: the walk steps lo by
	// LibyearHealWindowSize from 0, so `first` opens a window and `second`
	// opens the next one.
	var maxPK int64
	if err := store.pool.QueryRow(ctx,
		`SELECT coalesce(max(repo_deps_libyear_id), 0) FROM aveloxis_data.repo_deps_libyear`).Scan(&maxPK); err != nil {
		t.Fatalf("bounds: %v", err)
	}
	base := (maxPK/LibyearHealWindowSize + 1) * LibyearHealWindowSize
	first, second := base+1, base+LibyearHealWindowSize+1
	for name, id := range map[string]int64{"early-window": first, "late-window": second} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_libyear
			  (repo_deps_libyear_id, repo_id, name, package_manager, current_version, current_release_date, latest_release_date, libyear)
			VALUES ($1, $2, $3, 'pypi', '', '', '', 3.5)`, id, repoID, name); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	libyearOf := func(id int64) (val *float64) {
		if err := store.pool.QueryRow(ctx,
			`SELECT libyear FROM aveloxis_data.repo_deps_libyear WHERE repo_deps_libyear_id = $1`, id).Scan(&val); err != nil {
			t.Fatalf("read %d: %v", id, err)
		}
		return val
	}

	// The park: hold a row lock on the LATE row from another connection.
	blocker, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(blocker.Release)
	tx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack := false
	t.Cleanup(func() {
		if !rolledBack {
			_ = tx.Rollback(context.Background())
		}
	})
	var locked int64
	if err := tx.QueryRow(ctx,
		`SELECT repo_deps_libyear_id FROM aveloxis_data.repo_deps_libyear
		  WHERE repo_deps_libyear_id = $1 FOR UPDATE`, second).Scan(&locked); err != nil {
		t.Fatalf("take the fixture row lock: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	type outcome struct {
		candidates, updated int64
		err                 error
	}
	done := make(chan outcome, 1)
	go func() {
		c, u, err := store.HealUnknownLibyear(runCtx, true)
		done <- outcome{c, u, err}
	}()
	t.Cleanup(cancel)

	// Wait for the early window to be committed. The healer cannot get past
	// the late row, so this either happens or the run is broken; the ceiling
	// only bounds the failure.
	deadline := time.Now().Add(30 * time.Second)
	for libyearOf(first) != nil {
		if time.Now().After(deadline) {
			t.Fatal("the early window was never applied — the walk never reached it, or it is not one statement per window")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	got := <-done
	if got.err == nil {
		t.Fatal("a cancelled walk reported success — heal-libyear would print a completion line for a pass that stopped early")
	}
	if !errors.Is(got.err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled: healLibyearReport classifies on that to tell a stop from a fault", got.err)
	}
	if got.updated < 1 {
		t.Errorf("updated = %d after a window was committed, want the finished work reported — heal-libyear prints this number as rows already healed", got.updated)
	}
	if got.candidates < 2 {
		t.Errorf("candidates = %d, want at least the 2 seeded rows counted before the stop", got.candidates)
	}

	// Still holding the lock: the parked window must be exactly where the
	// walk stopped, or the run ended somewhere else and proves nothing.
	if libyearOf(second) == nil {
		t.Fatal("the locked row was healed anyway — the fixture did not park the walk")
	}

	// Cancelling the client does not reliably stop the statement on the
	// SERVER, and this test must not depend on which way it goes. Observed
	// both ways on PostgreSQL 18.4 with pgx v5.11.0: in one run of this very
	// fixture the window UPDATE ran to completion once the lock was released,
	// after the client had already returned context.Canceled — the orphaned
	// backend the troubleshooting runbook describes — and in others the
	// backend was gone within milliseconds of the cancel. Either is harmless
	// for this healer (the window is idempotent and re-running is what the
	// operator is told to do), but "what is true after the stop" would be a
	// race, so any statement still running is ended before the lock is
	// released.
	//
	// The scan is narrowed to ACTIVE backends on purpose: pg_stat_activity
	// keeps the last query text of an IDLE connection too, so matching on the
	// text alone would terminate a healthy pooled connection that merely ran
	// this UPDATE earlier.
	orphans := func() []int32 {
		rows, err := store.pool.Query(context.Background(), `
			SELECT pid FROM pg_stat_activity
			 WHERE datname = current_database()
			   AND pid <> pg_backend_pid()
			   AND state = 'active'
			   AND query LIKE '%repo_deps_libyear SET libyear = NULL%'`)
		if err != nil {
			t.Fatalf("look for a statement still running: %v", err)
		}
		defer rows.Close()
		var pids []int32
		for rows.Next() {
			var pid int32
			if err := rows.Scan(&pid); err != nil {
				t.Fatalf("scan pid: %v", err)
			}
			pids = append(pids, pid)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read pids: %v", err)
		}
		return pids
	}
	deadline = time.Now().Add(30 * time.Second)
	var lastTerminateErr error
	for {
		pids := orphans()
		if len(pids) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the abandoned UPDATE (%v) never went away; last terminate error: %v", pids, lastTerminateErr)
		}
		for _, pid := range pids {
			var killed bool
			if err := store.pool.QueryRow(context.Background(),
				`SELECT pg_terminate_backend($1)`, pid).Scan(&killed); err != nil {
				// Expected when the pool hands this query to the very
				// backend being terminated, or when it already exited: the
				// loop re-scans and only a persistent failure fails the test.
				lastTerminateErr = err
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = tx.Rollback(context.Background())
	rolledBack = true

	// The claim the operator is given: what was written stays written.
	if libyearOf(first) != nil {
		t.Error("the finished window did not survive the stop — a re-run would redo it, and the message promising otherwise would be a lie")
	}
	if libyearOf(second) == nil {
		t.Fatal("the unfinished window applied after all — nothing is left for the re-run to prove")
	}

	// ... and the other half: a re-run finishes what the stop left behind.
	if _, _, err := store.HealUnknownLibyear(ctx, true); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if libyearOf(second) != nil {
		t.Error("the row left behind by the interrupted pass is still unhealed after a re-run — the walk does not resume")
	}
}
