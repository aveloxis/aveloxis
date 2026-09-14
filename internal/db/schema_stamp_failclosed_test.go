// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot round 1 on PR #197, finding 2 — verified real and a genuine
// L10 hit on v0.29.4's OWN new code.
//
// Pre-v0.29.4 a failed schema-version stamp was benign: the stamp's only
// consumer was CheckSchemaVersion's advisory warning and the v0.27.131
// fast path, so a stale stamp cost one extra full (idempotent) migration
// run. WARN-and-continue was the right call.
//
// v0.29.4 promoted the stamp to EVIDENCE. deployStepsProvablyUnrun reads
// it as proof that a migration of this binary COMPLETED here, and
// refuses `start serve` when the stamp is behind — "no migration of this
// binary has completed here". So a transient stamp-write failure now
// produces a FALSE refusal on a fleet whose migration genuinely
// completed, and the operator's only visible remedy is a full re-run
// (hours on aveloxis_large). The gate's premise ("the stamp moves only
// when a migration completes") is false exactly when the stamp write
// fails.
//
// The fix is the release's own principle applied to its own writer:
// fail closed. Every migration step is idempotent, so re-running
// `aveloxis migrate` re-runs them as no-ops and stamps.

// TestStampSchemaVersionFailsClosed pins that the writer surfaces its
// failure instead of swallowing it into a WARN.
func TestStampSchemaVersionFailsClosed(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")

	decl := "func stampSchemaVersion(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {"
	if !strings.Contains(src, decl) {
		t.Errorf("stampSchemaVersion must RETURN an error so RunMigrations can fail closed.\n"+
			"v0.29.4's deployStepsProvablyUnrun reads schema_meta.schema_version as PROOF that a\n"+
			"migration of this binary completed here; a swallowed stamp failure makes that proof\n"+
			"false and produces a refusal on a fleet that DID migrate.\n"+
			"want declaration: %s", decl)
	}

	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func stampSchemaVersion("))

	if strings.Contains(body, `logger.Warn(`) {
		t.Error("stampSchemaVersion must not WARN-and-continue on a failed stamp — the stamp is\n" +
			"load-bearing evidence for the v0.29.4 deploy gate. Log at ERROR and return the error.")
	}
	if !strings.Contains(body, "RowsAffected()") {
		t.Error("stampSchemaVersion must check the command tag's RowsAffected(): an UPDATE ... WHERE\n" +
			"id = TRUE against a missing aveloxis_ops.schema_meta row SUCCEEDS with zero rows and\n" +
			"stamps nothing — the same silent-failure shape as a write error. The base schema DDL\n" +
			"seeds the row (schema.sql: INSERT ... ON CONFLICT DO NOTHING) and runs before this, so\n" +
			"zero rows is genuinely impossible on a healthy path — which is exactly why it should\n" +
			"be an error rather than a silent no-op.")
	}
	if !strings.Contains(body, "return") {
		t.Error("stampSchemaVersion body must return its error")
	}
}

// TestRunMigrationsFailsOnStampFailure pins the CALLER half. A writer
// that returns an error nobody checks is the v0.27.107 decorative-gate
// class.
func TestRunMigrationsFailsOnStampFailure(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func RunMigrations("))

	idx := strings.Index(body, "stampSchemaVersion(ctx, pg, logger)")
	if idx < 0 {
		t.Fatal("RunMigrations must still call stampSchemaVersion(ctx, pg, logger)")
	}

	// The call must be error-checked. Accept either the if-statement form
	// or an explicit assignment followed by a check — but the bare
	// statement form (v0.29.4 and earlier) must be gone.
	window := srctest.NormalizeWS(body[maxInt(0, idx-40):minInt(len(body), idx+220)])
	if !strings.Contains(window, "err := stampSchemaVersion") && !strings.Contains(window, "err = stampSchemaVersion") {
		t.Errorf("RunMigrations must capture and check stampSchemaVersion's error and RETURN it.\n"+
			"A bare call discards the failure and lets RunMigrations report success over a\n"+
			"schema_meta row that still names the PREVIOUS version — which the v0.29.4 deploy\n"+
			"gate then reads as \"no migration of this binary has completed here\".\n"+
			"window: %s", window)
	}
	if !strings.Contains(window, "return") {
		t.Errorf("RunMigrations must RETURN on a stamp failure, not log and fall through.\nwindow: %s", window)
	}
}

// TestStampSchemaVersionSurfacesWriteError is the behavioral half: a
// genuinely failing write must produce an error, not a nil return. A
// closed pool is the cheapest way to make the Exec fail for real without
// perturbing the shared scratch database.
func TestStampSchemaVersionSurfacesWriteError(t *testing.T) {
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
	// Deliberately close BEFORE stamping so the Exec fails at the pool.
	store.pool.Close()

	if err := stampSchemaVersion(ctx, store, logger); err == nil {
		t.Fatal("stampSchemaVersion returned nil against a closed pool — a failed stamp must\n" +
			"surface so RunMigrations can fail closed (Copilot round 1 finding 2).")
	}
}

// TestStampSchemaVersionSurfacesMissingRow drives the zero-rows arm the
// source pin above requires. It removes the single schema_meta row under
// the test-migrate advisory lock (so no concurrent testMigrate reads the
// missing stamp mid-window) and restores it unconditionally.
func TestStampSchemaVersionSurfacesMissingRow(t *testing.T) {
	store, ctx := v0251Connect(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	conn, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	defer conn.Release()
	var got bool
	// Paced: a spin would exhaust its attempts inside a microsecond and
	// skip whenever another package's testMigrate holds the lock for even
	// a moment.
	for i := 0; i < 40; i++ {
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", testMigrateLockID).Scan(&got); err != nil {
			t.Fatalf("try advisory lock: %v", err)
		}
		if got {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !got {
		t.Skip("could not acquire the test-migrate advisory lock — another package is migrating")
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testMigrateLockID)
	}()

	var savedVersion string
	if err := store.pool.QueryRow(ctx, `SELECT schema_version FROM aveloxis_ops.schema_meta WHERE id = TRUE`).Scan(&savedVersion); err != nil {
		t.Fatalf("read stamp: %v", err)
	}
	t.Cleanup(func() { restoreSchemaMetaRow(context.Background(), t, store, savedVersion) })

	if _, err := store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.schema_meta WHERE id = TRUE`); err != nil {
		t.Fatalf("delete stamp row: %v", err)
	}

	stampErr := stampSchemaVersion(ctx, store, logger)

	// Restore BEFORE the advisory lock is released: another package's
	// testMigrate grabbing the lock in the gap would read no stamp and
	// full-migrate. t.Cleanup stays as the backstop.
	restoreSchemaMetaRow(ctx, t, store, savedVersion)

	if stampErr == nil {
		t.Fatal("stampSchemaVersion returned nil with no aveloxis_ops.schema_meta row — an\n" +
			"UPDATE ... WHERE id = TRUE against a missing row succeeds with zero rows and stamps\n" +
			"NOTHING, which the v0.29.4 deploy gate then reads as \"no migration of this binary\n" +
			"has completed here\". Zero rows must be an error.")
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// restoreSchemaMetaRow puts the single stamp row back. Idempotent, so
// the explicit call and the t.Cleanup backstop compose.
func restoreSchemaMetaRow(ctx context.Context, t *testing.T, store *PostgresStore, version string) {
	t.Helper()
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.schema_meta (id, schema_version, migrated_at)
		VALUES (TRUE, $1, NOW())
		ON CONFLICT (id) DO UPDATE SET schema_version = EXCLUDED.schema_version`, version); err != nil {
		t.Errorf("restoring aveloxis_ops.schema_meta: %v", err)
	}
}
