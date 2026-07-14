// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.4 — GUI home-tab support: per-user starred repos plus a
// "most active in the last 90 days" list drawn from the user's own
// groups. Operator (2026-07-14): "the default be activity based, and
// allowing that to be overridden by stars".

import (
	"context"
)

// HomeRepo is one row of the home-tab repo list.
type HomeRepo struct {
	RepoID     int64  `json:"repo_id"`
	Owner      string `json:"owner"`
	Name       string `json:"name"`
	Starred    bool   `json:"starred"`
	Activity90 int    `json:"activity_90d"` // issues + PRs opened + distinct commits, last 90 days
}

// StarRepo stars repoID for userID. Idempotent.
func (s *PostgresStore) StarRepo(ctx context.Context, userID int, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id)
		VALUES ($1, $2) ON CONFLICT DO NOTHING`, userID, repoID)
	return err
}

// UnstarRepo removes a star. Idempotent.
func (s *PostgresStore) UnstarRepo(ctx context.Context, userID int, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM aveloxis_ops.user_repo_stars WHERE user_id = $1 AND repo_id = $2`,
		userID, repoID)
	return err
}

// GetHomeRepos returns the home-tab list for userID: every starred
// repo (always included, however idle), then the most active repos
// from the user's own groups over the trailing 90 days, deduplicated,
// capped at limit. Activity = issues opened + change requests opened
// + distinct commits authored in the window. Both admins and regular
// users see THEIR OWN groups' repos here — the home tab is personal;
// fleet-wide views live on the monitor.
func (s *PostgresStore) GetHomeRepos(ctx context.Context, userID int, limit int) ([]HomeRepo, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		WITH mine AS (
			SELECT DISTINCT ur.repo_id,
			       EXISTS (SELECT 1 FROM aveloxis_ops.user_repo_stars st
			               WHERE st.user_id = $1 AND st.repo_id = ur.repo_id) AS starred
			FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_ops.user_groups g USING (group_id)
			WHERE g.user_id = $1
			UNION
			SELECT st.repo_id, TRUE
			FROM aveloxis_ops.user_repo_stars st
			WHERE st.user_id = $1
		),
		act AS (
			SELECT m.repo_id, m.starred,
				COALESCE((SELECT COUNT(*) FROM aveloxis_data.issues i
				          WHERE i.repo_id = m.repo_id
				            AND i.created_at >= NOW() - INTERVAL '90 days'), 0)
				+ COALESCE((SELECT COUNT(*) FROM aveloxis_data.pull_requests p
				            WHERE p.repo_id = m.repo_id
				              AND p.created_at >= NOW() - INTERVAL '90 days'), 0)
				+ COALESCE((SELECT COUNT(DISTINCT c.cmt_commit_hash) FROM aveloxis_data.commits c
				            WHERE c.repo_id = m.repo_id
				              AND c.cmt_author_timestamp >= NOW() - INTERVAL '90 days'), 0)
				AS activity
			FROM mine m
		)
		SELECT a.repo_id, r.repo_owner, r.repo_name, a.starred, a.activity
		FROM act a
		JOIN aveloxis_data.repos r USING (repo_id)
		ORDER BY a.starred DESC, a.activity DESC, r.repo_owner, r.repo_name
		LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []HomeRepo{}
	for rows.Next() {
		var h HomeRepo
		if err := rows.Scan(&h.RepoID, &h.Owner, &h.Name, &h.Starred, &h.Activity90); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// HasDependencyData reports whether the analysis phase has ever
// recorded dependency rows for repoID — lets the GUI distinguish
// "not collected yet" from "this repository declares no dependencies"
// on the licenses panel (operator request, 2026-07-14).
func (s *PostgresStore) HasDependencyData(ctx context.Context, repoID int64) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1)
		    OR EXISTS (SELECT 1 FROM aveloxis_data.repo_dependencies WHERE repo_id = $1)`,
		repoID).Scan(&exists)
	return exists, err
}
