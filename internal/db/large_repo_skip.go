// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — large_repo_skip.go (v0.27.35): the TEMPORARY throughput
// lever behind collection.skip_largest_percent. Very large repos
// (kernel/pytorch class) periodically occupy every worker slot for
// hours and hold back the rest of the fleet; with the knob set, the
// scheduler excludes the fleet's top-N% repos (by forge-reported
// commit count OR pull-request count — either measure qualifies) from
// claim eligibility. Skipped repos stay 'queued' and simply become
// immediately due the moment the knob is removed — nothing is
// archived, dequeued, or mutated.
package db

import (
	"context"
	"fmt"
)

// LargestRepoIDs returns the repo_ids in the fleet's top `fraction`
// (e.g. 0.005 = top 0.5%) by forge-reported commit count OR PR count,
// using the latest repo_info snapshot (one row per repo — the history
// pattern rotates older snapshots out). Repos with no repo_info yet
// (never collected) are NEVER classified as large: an unknown repo
// must collect so it can be measured. Also returns the two thresholds
// for effective-value logging.
func (s *PostgresStore) LargestRepoIDs(ctx context.Context, fraction float64) (ids []int64, commitTh, prTh float64, err error) {
	if fraction <= 0 || fraction >= 1 {
		return nil, 0, 0, fmt.Errorf("fraction must be in (0,1), got %v", fraction)
	}
	// percentile_cont ignores NULLs; a NULL count then compares NULL
	// against the threshold and drops out of the OR — never skipped.
	rows, qerr := s.pool.Query(ctx, `
		WITH th AS (
			SELECT percentile_cont(1 - $1::float8) WITHIN GROUP (ORDER BY commit_count) AS c,
			       percentile_cont(1 - $1::float8) WITHIN GROUP (ORDER BY pr_count)     AS p
			FROM aveloxis_data.repo_info
		)
		SELECT DISTINCT ri.repo_id, th.c, th.p
		FROM aveloxis_data.repo_info ri, th
		WHERE ri.commit_count >= th.c OR ri.pr_count >= th.p`,
		fraction)
	if qerr != nil {
		return nil, 0, 0, qerr
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id, &commitTh, &prTh); err != nil {
			return nil, 0, 0, err
		}
		ids = append(ids, id)
	}
	return ids, commitTh, prTh, rows.Err()
}
