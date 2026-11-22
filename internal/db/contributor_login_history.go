// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Login-observation source constants for contributor_login_history.source.
// Kept as a closed set so operators can SQL-filter and dashboards can group
// by them without arbitrary string drift.
const (
	LoginSourceObservation    = "observation"
	LoginSourceRenameRecovery = "rename_recovery"
	LoginSourceRenameBreadth  = "rename_breadth"
	LoginSourceBackfill       = "backfill"
)

// recordLoginObservation upserts a (cntrb_id, platform_id, login) row
// into contributor_login_history. first_seen is preserved on conflict;
// last_seen advances. Caller passes the active pgx.Tx (so the observation
// commits with the contributor write it belongs to).
//
// Empty cntrbID, empty login, or platformID == 0 silently no-op — the
// helper is called from many sites including ones where the data may
// legitimately be missing (e.g. email-only commit-author contributors
// with no platform identity). Callers don't need to pre-validate.
//
// Source values are the LoginSource* constants above. Drift-prevent
// these by always passing a named constant; arbitrary string literals
// in call sites diverge over time.
//
// v0.23.0 — see docs/architecture/contributor-resolution.md "Rename
// handling" section for the full table contract.
func recordLoginObservation(ctx context.Context, tx pgx.Tx, cntrbID string, platformID int16, login, source string) error {
	if cntrbID == "" || login == "" || platformID == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_login_history
			(cntrb_id, platform_id, login, source, tool_version)
		VALUES ($1::uuid, $2, $3, $4, $5)
		ON CONFLICT (cntrb_id, platform_id, login) DO UPDATE SET
			last_seen = NOW()`,
		cntrbID, platformID, login, source, ToolVersion)
	return err
}

// The table is written by recordLoginObservation and read by operator SQL
// (docs/architecture/contributor-resolution.md, "Rename handling"); the
// exported per-contributor reader was deleted in 0.29.109 (N28 of
// summary/59: no page or command read it).
