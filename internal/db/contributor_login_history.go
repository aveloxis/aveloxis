// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
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
// last_seen advances. A caller that writes the login in a transaction
// passes that transaction (so the observation commits with the write it
// belongs to); the two writers that have none (UpsertContributorFull, the
// resolver's reuse of a row found by login) pass the pool, and their
// history row is re-recorded at the next observation when it fails.
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
func recordLoginObservation(ctx context.Context, tx rotationExecer, cntrbID string, platformID int16, login, source string) error {
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

// replacedLoginsToHistorySQL keeps the logins a write is about to replace:
// for each (cntrb_id, platform, platform user id, incoming login) it is
// given, the login stored on that platform user's identity row, and (for
// GitHub) the gh_login stored on that contributor's row, when either is
// set and is not the incoming one. One statement for any number of rows,
// run BEFORE the writes that overwrite them, so the batch upsert pays one
// round trip a batch and writes nothing when no login changes.
//
// Why it exists (operator rule, 2026-11-21: a login changes when a new one
// is observed, and the old one is in contributor_login_history): every
// writer of a login records the login it WRITES, so a replaced login is
// already here whenever a recording writer wrote it. It was not when the
// row came from a writer that recorded nothing before 0.29.216
// (UpsertContributorFull, ContributorResolver.Resolve, the search link),
// and those rows are not back-filled; this statement closes the case at
// the moment such a login is replaced.
//
// The row is the shape of the v0.23.0 seeding from identities
// (loginHistoryFromIdentitiesSQL) and carries its source, 'backfill': a
// login taken from what is stored, not one a forge just answered with.
// DO NOTHING: a login already recorded keeps its first_seen and last_seen
// (it was not observed now). A platform user id of 0 names no identity
// (v0.27.108) and is never matched.
const replacedLoginsToHistorySQL = `
	INSERT INTO aveloxis_data.contributor_login_history
		(cntrb_id, platform_id, login, source, tool_version)
	SELECT i.cntrb_id, i.platform_id, i.login, $5, $6
	FROM unnest($1::text[], $2::smallint[], $3::bigint[], $4::text[]) AS n(cntrb_id, platform_id, user_id, login)
	JOIN aveloxis_data.contributor_identities i
	  ON i.platform_id = n.platform_id AND i.platform_user_id = n.user_id
	WHERE n.user_id > 0 AND COALESCE(i.login, '') <> '' AND i.login <> n.login
	UNION
	SELECT c.cntrb_id, n.platform_id, c.gh_login, $5, $6
	FROM unnest($1::text[], $2::smallint[], $3::bigint[], $4::text[]) AS n(cntrb_id, platform_id, user_id, login)
	JOIN aveloxis_data.contributors c ON c.cntrb_id = NULLIF(n.cntrb_id, '')::uuid
	WHERE n.platform_id = 1 AND COALESCE(c.gh_login, '') <> '' AND c.gh_login <> n.login
	ON CONFLICT (cntrb_id, platform_id, login) DO NOTHING`

// replacedLogins is the argument of replacedLoginsToHistorySQL: one entry
// per login about to be written. cntrbID may be "" when the caller knows no
// contributor row whose gh_login it will overwrite.
type replacedLogins struct {
	cntrbIDs  []string
	platforms []int16
	userIDs   []int64
	logins    []string
}

func (r *replacedLogins) add(cntrbID string, platformID int16, userID int64, login string) {
	if login == "" || platformID == 0 {
		return
	}
	r.cntrbIDs = append(r.cntrbIDs, cntrbID)
	r.platforms = append(r.platforms, platformID)
	r.userIDs = append(r.userIDs, userID)
	r.logins = append(r.logins, login)
}

// recordReplacedLogins runs replacedLoginsToHistorySQL; nothing is sent for
// an empty set.
func recordReplacedLogins(ctx context.Context, q rotationExecer, r *replacedLogins) error {
	if len(r.logins) == 0 {
		return nil
	}
	_, err := q.Exec(ctx, replacedLoginsToHistorySQL,
		r.cntrbIDs, r.platforms, r.userIDs, r.logins, LoginSourceBackfill, ToolVersion)
	return err
}
