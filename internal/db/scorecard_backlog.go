// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"time"
)

// ScorecardBacklogRepo is one candidate row for the `aveloxis
// run-scorecard` bulk pass (v0.27.5).
type ScorecardBacklogRepo struct {
	RepoID   int64
	Owner    string
	Name     string
	GitURL   string
	Platform int16
	// LastScorecard is the latest repo_deps_scorecard
	// data_collection_date for the repo; nil = never scanned.
	LastScorecard *time.Time
}

// ListScorecardBacklog returns non-archived, at-least-once-collected
// GitHub repos ordered oldest-scorecard-first (never-scanned repos
// first). Used by `aveloxis run-scorecard`.
//
//   - olderThanDays > 0 filters to repos whose latest scorecard run is
//     older than that many days (never-scanned repos always qualify);
//     0 = every repo.
//   - limit > 0 caps the result (canary runs); 0 = no cap.
//
// GitHub-only (platform_id = 1) by design: the bulk pass exists to
// upgrade local-mode 11-check data to remote-mode 18-check data, and
// scorecard's remote mode is GitHub-only (its GitLab support is
// immature). GitLab/generic repos keep getting local-mode scans from
// the per-cycle collection phase.
//
// `q.last_collected IS NOT NULL` gates on "collection has produced
// data at least once" — running scorecard against a repo we have never
// collected would attach 18 checks to an otherwise-empty catalog row.
func (s *PostgresStore) ListScorecardBacklog(ctx context.Context, olderThanDays int, limit int) ([]ScorecardBacklogRepo, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_owner, r.repo_name, r.repo_git,
		       r.platform_id, sc.last_run
		FROM aveloxis_data.repos r
		JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		LEFT JOIN LATERAL (
			SELECT MAX(data_collection_date) AS last_run
			FROM aveloxis_data.repo_deps_scorecard s
			WHERE s.repo_id = r.repo_id
		) sc ON TRUE
		WHERE COALESCE(r.repo_archived, FALSE) = FALSE
		  AND r.platform_id = 1
		  AND q.last_collected IS NOT NULL
		  AND ($1 <= 0
		       OR sc.last_run IS NULL
		       OR sc.last_run < NOW() - make_interval(days => $1))
		ORDER BY sc.last_run ASC NULLS FIRST, r.repo_id
		LIMIT NULLIF($2, 0)`,
		olderThanDays, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ScorecardBacklogRepo
	for rows.Next() {
		var r ScorecardBacklogRepo
		if err := rows.Scan(&r.RepoID, &r.Owner, &r.Name, &r.GitURL, &r.Platform, &r.LastScorecard); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
