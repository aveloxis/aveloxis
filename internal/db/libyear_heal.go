// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 — the one-shot healer for libyear rows that can NEVER be
// computed.
//
// calcLibyear returns 0 when a release date is missing, and until v0.29.57
// that 0 was stored as if it meant "up to date". The resolver now stores
// NULL instead (markUnknownLibyear), but only as each repository is
// re-analysed — on chaoss.tv that is roughly a fortnight, and two of the ten
// days before 2026-09-17 did no analysis at all.
//
// This command closes the half that re-analysis can never fix. A dependency
// with no PINNED VERSION has no release date to measure age from, and no
// registry fix can change that: measured on chaoss.tv on 2026-09-17,
// 471,344 rows carried a libyear number with an empty current_version, and
// ZERO of them had a current_release_date. Those are NULLed here in minutes
// instead of never.
//
// The other class — a pinned version whose registry would not give a date —
// is deliberately NOT touched: v0.29.56 fixed the Go and Maven resolvers
// that caused 88% of it, so re-analysis will fill in real dates. NULLing
// them here would erase rows the next cycle is about to answer properly.

import (
	"context"
	"fmt"
)

// LibyearHealWindowSize is the keyset window over repo_deps_libyear_id.
// Sized against the production range (19,310 → 13,331,892 on 2026-09-17,
// holding 3.6M rows): ~27 windows, each a bounded UPDATE that takes its
// locks and releases them rather than one statement over the whole table.
const LibyearHealWindowSize = 500_000

// libyearUnknownPredicate selects rows whose libyear is unknowable: a
// number is stored, but the dependency names no version to date.
// coalesce covers both the empty string (the column default) and NULL.
const libyearUnknownPredicate = `libyear IS NOT NULL AND coalesce(current_version, '') = ''`

// HealUnknownLibyear reports how many rows carry an uncomputable libyear
// and, when apply is true, replaces those numbers with NULL.
//
// Walks keyset windows over the primary key rather than a `LIMIT n` loop:
// a LIMIT loop re-pays the scan per batch and never advances past rows it
// cannot change.
func (s *PostgresStore) HealUnknownLibyear(ctx context.Context, apply bool) (candidates, updated int64, err error) {
	var maxPK int64
	if err := s.pool.QueryRow(ctx,
		`SELECT coalesce(max(repo_deps_libyear_id), 0) FROM aveloxis_data.repo_deps_libyear`).Scan(&maxPK); err != nil {
		return 0, 0, fmt.Errorf("heal libyear: bounds: %w", err)
	}

	countSQL := `SELECT count(*) FROM aveloxis_data.repo_deps_libyear
	              WHERE repo_deps_libyear_id > $1 AND repo_deps_libyear_id <= $2
	                AND ` + libyearUnknownPredicate
	updateSQL := `UPDATE aveloxis_data.repo_deps_libyear SET libyear = NULL
	               WHERE repo_deps_libyear_id > $1 AND repo_deps_libyear_id <= $2
	                 AND ` + libyearUnknownPredicate

	for lo := int64(0); lo < maxPK; lo += LibyearHealWindowSize {
		if err := ctx.Err(); err != nil {
			// A cancelled run is a stop, not a failure. Windows already
			// applied stay applied: each is its own statement and the
			// predicate no longer matches them, so a re-run resumes.
			return candidates, updated, err
		}
		hi := lo + LibyearHealWindowSize

		var n int64
		if err := s.pool.QueryRow(ctx, countSQL, lo, hi).Scan(&n); err != nil {
			return candidates, updated, fmt.Errorf("heal libyear: count window (%d,%d]: %w", lo, hi, err)
		}
		candidates += n
		if !apply || n == 0 {
			continue
		}
		tag, err := s.pool.Exec(ctx, updateSQL, lo, hi)
		if err != nil {
			return candidates, updated, fmt.Errorf("heal libyear: update window (%d,%d]: %w", lo, hi, err)
		}
		updated += tag.RowsAffected()
	}
	return candidates, updated, nil
}
