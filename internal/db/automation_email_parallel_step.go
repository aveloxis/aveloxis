// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
)

// ensureAutomationEmailParallelSafe marks is_automation_email PARALLEL SAFE
// on every migrate, as its own step (operator request 2026-09-29, for
// analytics). schema.sql's CREATE OR REPLACE already declares it (v0.29.8),
// but where that did not apply — a function owned by another role, a
// database migrated before v0.29.8 — Postgres keeps the default PARALLEL
// UNSAFE and every query calling the function runs serially. The effective
// marker is read back: a change is logged, and a marker that is still not
// SAFE is a WARN naming why (usually ownership). Never fails the migrate: it
// is a plan-quality setting, not an integrity rule.
func ensureAutomationEmailParallelSafe(ctx context.Context, pg *PostgresStore, logger *slog.Logger) {
	const fn = "aveloxis_data.is_automation_email(text)"
	var before string
	if err := pg.pool.QueryRow(ctx, `SELECT proparallel::text FROM pg_proc WHERE oid = $1::regprocedure`, fn).Scan(&before); err != nil {
		logger.Warn("is_automation_email: could not read its parallel marker — skipping the PARALLEL SAFE step", "error", err)
		return
	}
	if before == "s" {
		return
	}
	if _, err := pg.pool.Exec(ctx, `ALTER FUNCTION aveloxis_data.is_automation_email(text) PARALLEL SAFE`); err != nil {
		logger.Warn("is_automation_email is still not PARALLEL SAFE — queries calling it run serially; ALTER FUNCTION failed (it needs the function's owner)",
			"was", before, "error", err)
		return
	}
	logger.Info("is_automation_email marked PARALLEL SAFE", "was", before)
}
