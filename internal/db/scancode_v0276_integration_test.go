// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// v0.27.6 — live-Postgres integration tests for the new scancode
// store behavior: the locked-host round trip, the generated-content
// skip bookkeeping, and the at-cap timeout sideline. Gated on
// AVELOXIS_TEST_DB (same pattern as v0251Connect); skipped otherwise.

func v0276Connect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// t.Cleanup runs LIFO and AFTER the test's deferred calls would —
	// registering the pool close here (before any per-row cleanup is
	// registered) guarantees the row deletes still have a live pool.
	// An explicit `t.Cleanup(store.pool.Close)` in the test body would
	// close the pool BEFORE t.Cleanup's deletes, silently leaking
	// seed rows into the shared scratch DB (observed on the first
	// suite re-run as 23505 on repos_repo_git_key).
	t.Cleanup(func() { store.pool.Close() })
	return store, ctx
}

func v0276SeedRepo(t *testing.T, store *PostgresStore, ctx context.Context, slug string) int64 {
	t.Helper()
	repoGit := "https://github.com/_avsc276/" + slug
	// Self-heal residue from a prior aborted run.
	if _, err := store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, repoGit); err != nil {
		t.Fatalf("pre-clean: %v", err)
	}
	var repoID int64
	err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name, languages)
		VALUES (1, $1, '_avsc276', $2, '{"HTML": 6442450944}'::jsonb)
		RETURNING repo_id`,
		repoGit, slug).Scan(&repoID)
	if err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	return repoID
}

func TestV0276LockStateHostRoundTrip(t *testing.T) {
	store, ctx := v0276Connect(t)
	repoID := v0276SeedRepo(t, store, ctx, "hostrt")

	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos SET scancode_locked_at = NOW() WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordScancodeLockState(ctx, repoID, 4242, "boot-abc", "/tmp/out.json", "scanhost-1"); err != nil {
		t.Fatalf("RecordScancodeLockState: %v", err)
	}

	rows, err := store.ListLockedScancodeRows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range rows {
		if r.RepoID == repoID {
			found = true
			if r.LockedHost != "scanhost-1" {
				t.Errorf("LockedHost round trip failed: got %q", r.LockedHost)
			}
			if r.LockedPID != 4242 || r.LockedBootID != "boot-abc" {
				t.Errorf("pid/boot round trip failed: %+v", r)
			}
		}
	}
	if !found {
		t.Fatal("locked row not returned by ListLockedScancodeRows")
	}

	// Clearing the lock must clear the host too.
	if err := store.ClearScancodeLock(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	var host *string
	if err := store.pool.QueryRow(ctx,
		`SELECT scancode_locked_host FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&host); err != nil {
		t.Fatal(err)
	}
	if host != nil {
		t.Errorf("ClearScancodeLock must NULL scancode_locked_host, got %v", *host)
	}
}

func TestV0276SkipBookkeeping(t *testing.T) {
	store, ctx := v0276Connect(t)
	repoID := v0276SeedRepo(t, store, ctx, "skipbk")

	// Give the row a diagnostic trail the skip must NOT erase.
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos SET scancode_timeout_attempts = 27 WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}

	if err := store.MarkScancodeSkipped(ctx, repoID, "generated-content"); err != nil {
		t.Fatalf("MarkScancodeSkipped: %v", err)
	}
	var reason string
	var lastRun *time.Time
	var attempts int
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(scancode_skip_reason,''), scancode_last_run, COALESCE(scancode_timeout_attempts,0)
		FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&reason, &lastRun, &attempts); err != nil {
		t.Fatal(err)
	}
	if reason != "generated-content" {
		t.Errorf("skip reason not recorded, got %q", reason)
	}
	if lastRun == nil || time.Since(*lastRun) > time.Minute {
		t.Error("skip must stamp scancode_last_run so the cadence gate applies")
	}
	if attempts != 27 {
		t.Errorf("skip must preserve the diagnostic trail (scancode_timeout_attempts), got %d", attempts)
	}

	// A later REAL successful scan clears the stale skip reason.
	if err := store.MarkScancodeComplete(ctx, repoID, "v99-test"); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT COALESCE(scancode_skip_reason,'') FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason != "" {
		t.Errorf("MarkScancodeComplete must clear scancode_skip_reason, got %q", reason)
	}
}

func TestV0276TimeoutSideline(t *testing.T) {
	store, ctx := v0276Connect(t)
	repoID := v0276SeedRepo(t, store, ctx, "capstrike")

	// Below-threshold timeout: attempts++ but last_run stays NULL
	// (the row remains claimable after backoff — v0.23.8 semantics).
	if err := store.RecordScancodeTimeout(ctx, repoID, false); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var lastRun *time.Time
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(scancode_timeout_attempts,0), scancode_last_run
		FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&attempts, &lastRun); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Errorf("timeout must increment the attempts counter, got %d", attempts)
	}
	if lastRun != nil {
		t.Error("sideline=false must NOT stamp scancode_last_run — that would silently cadence-out every big repo on its first timeout")
	}

	// At-cap strike threshold reached: sideline=true stamps last_run
	// (the v0.21.4 cadence-gate mechanism), ending the June 2026
	// re-claim loop, while the attempts trail keeps growing.
	if err := store.RecordScancodeTimeout(ctx, repoID, true); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(scancode_timeout_attempts,0), scancode_last_run
		FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&attempts, &lastRun); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Errorf("sideline must still increment the diagnostic counter, got %d", attempts)
	}
	if lastRun == nil || time.Since(*lastRun) > time.Minute {
		t.Error("sideline=true must stamp scancode_last_run = NOW() so the cadence gate excludes the row")
	}
}
