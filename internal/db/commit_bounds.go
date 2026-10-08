// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"time"
)

// RecordCommitBounds maintains repos.first_commit_at and last_commit_at, the
// MIN and MAX of the repository's PLAUSIBLE commits.cmt_author_timestamp
// (O11 option 2, operator decision 2026-09-29; "plausible" = within
// [EarliestPlausibleCommitTime, LatestPlausibleCommitTime), 2026-10-07). Production has no (repo_id,
// cmt_author_timestamp) index, so computing either live reads every one of a
// repository's per-file commit rows — what ran /stats past nginx's 60 s on
// the giant repositories.
//
// first and last are the bounds of the rows the caller PROVED written (zero
// when none: a refused clone, the gone recheck). A NULL column means "never
// filled" and is computed from the table in full, once: the table can hold
// commits no longer on the default branch (a force-push, a default-branch
// change — commit rows are never deleted), which a walk of the branch does
// not see. The callers run inside a collection job or the recheck ticker,
// where no request deadline applies. A filled, plausible column only widens
// (GREATEST/LEAST); a filled column outside the plausible range (a bogus
// date stored before the rule — 2080, or the epoch an unset clock stamps)
// is treated as unfilled and refilled from the table's plausible rows —
// the one way a bound moves against its direction. The uncorrelated
// subqueries are InitPlans, evaluated only on that
// branch. A filled row is rewritten only when a bound widens or a bogus
// bound is repaired (the gone recheck calls this for every still-gone
// repository every cadence); a row that stays NULL — a repository with no
// plausibly dated commits — is rewritten on each call: once per collection
// job and once per recheck cadence (review round 5).
func (s *PostgresStore) RecordCommitBounds(ctx context.Context, repoID int64, first, last time.Time) error {
	// Bogus author dates (2026-10-07: 2080, or the epoch an unset clock
	// stamps) never become a bound: a run's bound outside [floor, ceiling)
	// is dropped (GREATEST/LEAST ignore NULL), the table fill reads only
	// plausible rows, and a stored bound outside it — written before this
	// rule — reads as unfilled and is refilled, first and last alike. A commit dated just beyond the
	// bound by a fast clock is simply not a bound YET: every walk notes
	// every row of the default branch again, so the first walk after the
	// bound passes it folds it in (review round 2 corrected round 1's F3).
	bound := LatestPlausibleCommitTime(time.Now())
	floor := EarliestPlausibleCommitTime()
	if !last.Before(bound) || last.Before(floor) {
		last = time.Time{}
	}
	if !first.Before(bound) || first.Before(floor) {
		first = time.Time{}
	}
	// The CASE expressions sit in SET and the guard, reading the target row
	// r: a statement that waited on another writer's row lock re-evaluates
	// them against the row it finally updates (review round 4 F1 — computed
	// in a CTE joined to the UPDATE, they kept the pre-wait row, and a
	// concurrent fill shrank a filled column).
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos r SET
		    first_commit_at = CASE WHEN r.first_commit_at IS NULL OR r.first_commit_at >= $4::timestamptz OR r.first_commit_at < $5::timestamptz
		        THEN (SELECT MIN(cmt_author_timestamp) FROM aveloxis_data.commits WHERE repo_id = $1 AND cmt_author_timestamp >= $5::timestamptz AND cmt_author_timestamp < $4::timestamptz)
		        ELSE LEAST(r.first_commit_at, $2::timestamptz) END,
		    last_commit_at = CASE WHEN r.last_commit_at IS NULL OR r.last_commit_at >= $4::timestamptz OR r.last_commit_at < $5::timestamptz
		        THEN (SELECT MAX(cmt_author_timestamp) FROM aveloxis_data.commits WHERE repo_id = $1 AND cmt_author_timestamp >= $5::timestamptz AND cmt_author_timestamp < $4::timestamptz)
		        ELSE GREATEST(r.last_commit_at, $3::timestamptz) END
		WHERE r.repo_id = $1
		  AND (r.first_commit_at IS NULL OR r.last_commit_at IS NULL
		       OR r.first_commit_at >= $4::timestamptz OR r.last_commit_at >= $4::timestamptz
		       OR r.first_commit_at < $5::timestamptz OR r.last_commit_at < $5::timestamptz
		       OR r.first_commit_at > $2::timestamptz OR r.last_commit_at < $3::timestamptz)`,
		repoID, NullTime(first), NullTime(last), bound, floor)
	return err
}
