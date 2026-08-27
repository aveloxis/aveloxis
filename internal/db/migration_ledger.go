// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// migration_ledger.go — the v0.28.4 completed-backfill ledger (the F13
// full fix flagged in v0.27.96 and measured on the 2026-08-23
// production migrate: ~1.5-2.5h of no-op re-walking per version bump —
// timestamp cleanup ~1h for 38 rows, the v0.27.104 keyset windows,
// repo_labor's 2,829 rotation windows, the v0.27.15 msg_ref walks).
//
// The v0.27.131 stamp fast-path only short-circuits when
// schema_meta.schema_version matches ToolVersion EXACTLY, so every
// version bump still re-ran the full walk. The ledger records each
// completed one-shot DATA step by a stable label; runOnce skips
// recorded labels on every later migrate.
//
// SCOPE IS EXPLICIT OPT-IN, never blanket:
//   - Ledgered: the expensive one-shot DATA walkers — keyset backfills
//     (runKeysetWindows), one-shot UPDATE/INSERT/DELETE backfills,
//     rotations, dedups, timestamp cleanup.
//   - NOT ledgered: all DDL (CREATE TABLE/INDEX IF NOT EXISTS,
//     addColumnIfMissing — cheap, and the explicit `aveloxis migrate`
//     must keep healing hand-dropped objects), views.sql (runs every
//     migrate by v0.27.115 design), setToolVersionDefaults,
//     dedup-gated unique creation (must re-evaluate its guard), and
//     CONCURRENTLY index builds (INVALID-recovery must re-check).
//
// Label stability matters: a renamed label re-runs its step (harmless
// — every ledgered step stays idempotent by contract — but wasteful).
// The registry pin test (migration_ledger_test.go) freezes the label
// set so renames are deliberate.
//
// Operator replay of a single step:
//
//	DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = '<label>';
//	aveloxis migrate --skip-views

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ensureMigrationLedgerTable creates the ledger. Runs immediately
// after the base schema DDL (which also declares the table for fresh
// installs) as a belt: a partially-failed base DDL must not leave the
// ledgered steps probing a missing table.
func ensureMigrationLedgerTable(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	execMigrationStep(ctx, pg, logger, errs, "create aveloxis_ops.migration_ledger", `
		CREATE TABLE IF NOT EXISTS aveloxis_ops.migration_ledger (
		    step_label   TEXT PRIMARY KEY,
		    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		    tool_version TEXT NOT NULL DEFAULT ''
		)`)
}

// runOnce gates a one-shot data migration step through the ledger:
// a recorded label skips instantly; an unrecorded label runs the step
// and records it ONLY when the step contributed zero errors (the
// v0.19.4 fail-closed contract preserved — failed steps re-run on the
// next migrate). The step receives its OWN error collector so runOnce
// can tell "this step failed" apart from earlier steps' errors; the
// local errors are folded into the shared collector either way.
func runOnce(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error,
	label string, step func(errs *[]error)) {
	var one int
	err := pg.pool.QueryRow(ctx,
		`SELECT 1 FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label).Scan(&one)
	switch {
	case err == nil:
		logger.Debug("migration step already completed (ledger) — skipping", "label", label)
		return
	case errors.Is(err, pgx.ErrNoRows):
		// Never completed — run it below.
	default:
		// A ledger READ failure must not SKIP the step: running an
		// idempotent step redundantly is safe; skipping it on bad
		// information is the SR-5 class. Run it.
		logger.Warn("migration ledger probe failed — running the step anyway", "label", label, "error", err)
	}

	var local []error
	step(&local)
	if len(local) > 0 {
		*errs = append(*errs, local...)
		return // unrecorded — the step re-runs on the next migrate
	}
	if _, err := pg.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.migration_ledger (step_label, tool_version)
		VALUES ($1, $2)
		ON CONFLICT (step_label) DO NOTHING`, label, ToolVersion); err != nil {
		// Harmless (the idempotent step just re-runs next migrate)
		// but worth surfacing — a persistent record failure means the
		// ledger never converges.
		logger.Warn("migration ledger record failed — step will re-run next migrate", "label", label, "error", err)
	}
}

// runOnceStep is runOnce over a plain-SQL execMigrationStep — the
// drop-in conversion for ledgered one-shot backfill statements. Same
// signature as execMigrationStep so call sites change one identifier.
func runOnceStep(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error,
	label, sql string) {
	runOnce(ctx, pg, logger, errs, label, func(errs *[]error) {
		execMigrationStep(ctx, pg, logger, errs, label, sql)
	})
}

// runOnceSeedIfApplied records label in the ledger WITHOUT running the
// step when the database's schema stamp proves a migrate at or above
// appliedSince already completed (v0.28.18). The stamp is written only
// after every step of that binary succeeded, and a step that existed as
// a plain execMigrationStep ran on every one of those migrates — so the
// work is provably done. Any stamp below appliedSince, an absent stamp
// (fresh or pre-v0.14.5 database) or an unparseable one leaves the step
// to run: running an idempotent step once more is the safe direction.
func runOnceSeedIfApplied(ctx context.Context, pg *PostgresStore, logger *slog.Logger, label, appliedSince string) {
	prior := pg.GetSchemaVersion(ctx)
	if prior == "" || !schemaVersionAtLeast(prior, appliedSince) {
		return
	}
	tag, err := pg.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.migration_ledger (step_label, tool_version)
		VALUES ($1, $2)
		ON CONFLICT (step_label) DO NOTHING`, label, ToolVersion)
	if err != nil {
		logger.Warn("migration ledger seed failed — the step will run once more instead", "label", label, "error", err)
		return
	}
	if tag.RowsAffected() > 0 {
		logger.Info("migration ledger seeded — step already applied by every migrate since the stamp proves", "label", label, "prior_schema_version", prior, "applied_since", appliedSince)
	}
}

// schemaVersionAtLeast compares dotted numeric versions ("0.27.37").
// Malformed input compares false (the caller then runs the step).
func schemaVersionAtLeast(have, want string) bool {
	parse := func(v string) ([]int, bool) {
		parts := strings.Split(strings.TrimSpace(v), ".")
		out := make([]int, 0, len(parts))
		for _, p := range parts {
			n, err := strconv.Atoi(p)
			if err != nil || n < 0 {
				return nil, false
			}
			out = append(out, n)
		}
		return out, len(out) > 0
	}
	h, ok1 := parse(have)
	w, ok2 := parse(want)
	if !ok1 || !ok2 {
		return false
	}
	for i := 0; i < len(h) || i < len(w); i++ {
		var a, b int
		if i < len(h) {
			a = h[i]
		}
		if i < len(w) {
			b = w[i]
		}
		if a != b {
			return a > b
		}
	}
	return true
}
