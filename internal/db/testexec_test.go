// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Bounded 40P01 retry for integration-test seeds and cleanups — the
// combined-run deadlock class (v0.27.114): a concurrent package's
// migration DDL holds locks across many tables in one implicit
// transaction and Postgres can pick ANY test statement as the deadlock
// victim. Migration-side statements retry via execMigrationStep and
// withRetry; test-side seeds are the residual victim set (observed:
// the dedup fixture cleanups locally, then
// TestBatchSingleStatsAgreementSeeded's vuln seed in CI 2026-08-21 —
// third instance, so the pattern is factored here once). Every seed
// statement is single-row and safe to re-run: a deadlock victim
// commits nothing.
//
// mustExecRetry fatals on persistent/non-deadlock failure (seed path);
// cleanupExecRetry is best-effort (cleanup path — but WITH the retry,
// because a deadlock-killed cleanup strands fixture rows that poison
// the NEXT run's seeds and data-verify).

const testExecRetries = 4

func retryLoop40P01(exec func() error) error {
	var err error
	for attempt := 0; attempt < testExecRetries; attempt++ {
		if err = exec(); err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "40P01" {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	return err
}

func mustExecRetry(ctx context.Context, t testing.TB, pg *PostgresStore, sql string, args ...any) {
	t.Helper()
	if err := retryLoop40P01(func() error {
		_, err := pg.pool.Exec(ctx, sql, args...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// mustQueryRowRetry runs an INSERT ... RETURNING (or similar
// single-row query) with the same bounded deadlock retry, scanning
// into dest.
func mustQueryRowRetry(ctx context.Context, t testing.TB, pg *PostgresStore, sql string, dest any, args ...any) {
	t.Helper()
	if err := retryLoop40P01(func() error {
		return pg.pool.QueryRow(ctx, sql, args...).Scan(dest)
	}); err != nil {
		t.Fatal(err)
	}
}

func cleanupExecRetry(ctx context.Context, pg *PostgresStore, sql string, args ...any) {
	_ = retryLoop40P01(func() error {
		_, err := pg.pool.Exec(ctx, sql, args...)
		return err
	})
}

// testMigrate — the test-infrastructure half of the deadlock-class fix
// (the other half is the migration-side retry, v0.27.114): every
// integration test migrating the SHARED scratch DB re-executed the
// full base schema DDL — one giant implicit transaction per call, ~30+
// calls per combined run — and each was a fresh chance for Postgres to
// pick a concurrent package's statement as the 40P01 victim. The stamp
// in schema_meta is written ONLY when every migration step succeeded
// (migrate.go), so "stamped == ToolVersion" proves this binary's
// schema is fully applied and the migrate can be skipped. Fresh DBs
// (no stamp) and version changes still migrate; the PRODUCTION migrate
// contract is untouched — this helper is test-only.
func testMigrate(ctx context.Context, t testing.TB, store *PostgresStore) {
	t.Helper()
	if store.GetSchemaVersion(ctx) == ToolVersion {
		return
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}
