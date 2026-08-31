// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.4 — GUI home-tab support: per-user starred repos plus a
// "most active in the last 90 days" list drawn from the user's own
// groups. Operator (2026-07-14): "the default be activity based, and
// allowing that to be overridden by stars".

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// HomeRepo is one row of the home-tab repo list.
type HomeRepo struct {
	RepoID     int64  `json:"repo_id"`
	Owner      string `json:"owner"`
	Name       string `json:"name"`
	Starred    bool   `json:"starred"`
	Activity90 int    `json:"activity_90d"` // issues + PRs opened (the queue-cached last_activity_90d), last 90 days
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

// IsRepoStarred reports whether userID has starred repoID — the
// targeted read behind GET /repos/{id}/star (v0.27.85, the repo page's
// star toggle). Deliberately a single EXISTS rather than
// GetUserStarredRepoIDs, which loads the caller's entire star set.
func (s *PostgresStore) IsRepoStarred(ctx context.Context, userID int, repoID int64) (bool, error) {
	var starred bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM aveloxis_ops.user_repo_stars
		              WHERE user_id = $1 AND repo_id = $2)`, userID, repoID).Scan(&starred)
	return starred, err
}

// GetHomeRepos returns the home-tab list for userID: every starred
// repo (always included, however idle), then the most active repos
// from the user's own groups over the trailing 90 days, deduplicated,
// capped at limit. Activity = issues opened + change requests opened
// in the window (commits are deliberately excluded — a 90-day count
// against the 474M-row commits table per candidate repo is what made
// the first version time out for an 86,909-repo admin).
//
// Shape matters at fleet scale: the candidate set is joined via
// unnest() and each activity count is ONE set-based GROUP BY backed
// by the (repo_id, created_at) composite indexes — a tight index
// probe per candidate repo, most of which return instantly.
func (s *PostgresStore) GetHomeRepos(ctx context.Context, userID int, limit int) ([]HomeRepo, error) {
	if limit <= 0 {
		limit = 50 // v0.27.14: raised from 20 (operator ask — the home list capacity)
	}
	// v0.29.0: the 90-day activity is READ from the queue's cached
	// column (stamped by CompleteJob, backfilled once at migrate) —
	// the per-render fleet-wide aggregation this replaced measured
	// mean 8.1s / max 48.2s on the production fleet for a 143K-repo
	// admin scope (pg_stat_statements, 2026-08-31). LEFT JOIN so a
	// queueless repo (the gone cohort) still renders, ranked as 0.
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
		)
		SELECT m.repo_id, r.repo_owner, r.repo_name, m.starred,
		       COALESCE(q.last_activity_90d, 0) AS activity
		FROM mine m
		JOIN aveloxis_data.repos r USING (repo_id)
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = m.repo_id
		ORDER BY m.starred DESC, activity DESC, r.repo_owner, r.repo_name
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

// GetUserStarredRepoIDs returns the set of repo ids userID has
// starred — used to annotate search results with star state.
func (s *PostgresStore) GetUserStarredRepoIDs(ctx context.Context, userID int) (map[int64]bool, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT repo_id FROM aveloxis_ops.user_repo_stars WHERE user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// StarredGroupName is the implicit per-user group that stars ride
// into scope on. Auto-created on the first star of a repo outside the
// user's groups; follows the normal group rules otherwise (a user can
// see and prune it like any group).
const StarredGroupName = "Starred"

// ComparisonsGroupName is the implicit per-user group that
// out-of-scope compare selections ride into scope on (v0.27.14 —
// same pattern as Starred). It only ever receives ALREADY-COLLECTED
// repos (the compare picker surfaces nothing else), so it can never
// trigger new collection; org entities add their collected repo set
// here and NEVER register org tracking.
const ComparisonsGroupName = "Comparisons"

// findOrCreateNamedGroup is the shared find-or-create for the
// implicit per-user groups (Starred, Comparisons). Creation goes
// through CreateUserGroup so the v0.19.0 status rules apply
// uniformly — the status only matters for future COLLECTION
// enqueues, never for scope (see GetUserRepoScope).
func (s *PostgresStore) findOrCreateNamedGroup(ctx context.Context, userID int, name string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		SELECT group_id FROM aveloxis_ops.user_groups
		WHERE user_id = $1 AND name = $2
		ORDER BY group_id LIMIT 1`, userID, name).Scan(&id)
	if err == nil {
		return id, nil
	}
	return s.CreateUserGroup(ctx, userID, name)
}

// FindOrCreateUserGroupByName is the exported find-or-create for a
// user's group by name (v0.27.63 — the collections copy flow lets the
// caller name a fresh destination group). Same v0.19.0 status rules
// as every other creation path.
func (s *PostgresStore) FindOrCreateUserGroupByName(ctx context.Context, userID int, name string) (int64, error) {
	return s.findOrCreateNamedGroup(ctx, userID, name)
}

// FindOrCreateStarredGroup returns the user's Starred group id,
// creating the group on first use.
func (s *PostgresStore) FindOrCreateStarredGroup(ctx context.Context, userID int) (int64, error) {
	return s.findOrCreateNamedGroup(ctx, userID, StarredGroupName)
}

// FindOrCreateComparisonsGroup returns the user's Comparisons group
// id, creating the group on first use (v0.27.14).
func (s *PostgresStore) FindOrCreateComparisonsGroup(ctx context.Context, userID int) (int64, error) {
	return s.findOrCreateNamedGroup(ctx, userID, ComparisonsGroupName)
}

// RecordComparisonRepos links repoIDs into userID's Comparisons group
// and returns the number of NEW links. v0.27.86: Comparisons is the
// persistent record of every repo the user has compared — before
// this, only the v0.27.14 out-of-scope auto-add wrote to it, so
// admins (unscoped) and in-scope selections left no trace at all
// (operator report 2026-08-05; production showed the admin account
// had no Comparisons group despite heavy compare use).
//
// The insert joins aveloxis_data.repos so nonexistent ids (admin repo
// ids come straight from the URL, unverified) are silently skipped,
// and the existence probe runs FIRST so a garbage-only call can never
// leave an empty group behind (the shared-with-me rule). Links are
// idempotent — compare fires one request per metric section, all
// carrying the same entity set. Pure user_repos linkage: NEVER
// touches collection machinery (approval gates collection, v0.27.20).
func (s *PostgresStore) RecordComparisonRepos(ctx context.Context, userID int, repoIDs []int64) (int, error) {
	if userID <= 0 || len(repoIDs) == 0 {
		return 0, nil
	}
	var anyReal bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM aveloxis_data.repos WHERE repo_id = ANY($1))`,
		repoIDs).Scan(&anyReal); err != nil {
		return 0, fmt.Errorf("comparison record repo check: %w", err)
	}
	if !anyReal {
		return 0, nil
	}
	gid, err := s.findOrCreateNamedGroup(ctx, userID, ComparisonsGroupName)
	if err != nil {
		return 0, fmt.Errorf("comparison record group: %w", err)
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id)
		SELECT $1, r.repo_id FROM aveloxis_data.repos r WHERE r.repo_id = ANY($2)
		ON CONFLICT DO NOTHING`, gid, repoIDs)
	if err != nil {
		return 0, fmt.Errorf("comparison record link: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ScorecardOverallName is the reserved row name under which the
// collector stores scorecard's aggregate score (v0.27.4 — previously
// it was logged and dropped). Double-underscored so it can never
// collide with a real check name (those are Hyphenated-Caps).
const ScorecardOverallName = "__overall__"

// ScorecardCheck is one OpenSSF Scorecard check result for a repo
// (latest snapshot; history lives in repo_deps_scorecard_history).
// Score is 0–10; -1 means the check did not apply or was inconclusive.
// float64 because the aggregate carries one decimal (e.g. 5.6);
// individual checks are whole numbers.
type ScorecardCheck struct {
	Name  string  `json:"name"`
	Score float64 `json:"score"`
}

// GetRepoScorecard returns the current scorecard checks for repoID,
// the aggregate ("headline") score when the collector has stored one
// (nil for scans that predate v0.27.4 — heals on the next scorecard
// run), and the scan timestamp. Empty checks + zero time = never
// scanned.
func (s *PostgresStore) GetRepoScorecard(ctx context.Context, repoID int64) ([]ScorecardCheck, *float64, time.Time, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, score, data_collection_date
		FROM aveloxis_data.repo_deps_scorecard
		WHERE repo_id = $1
		ORDER BY name`, repoID)
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	defer rows.Close()
	var (
		checks  []ScorecardCheck
		overall *float64
		asOf    time.Time
	)
	for rows.Next() {
		var (
			c        ScorecardCheck
			scoreTxt string
			ts       time.Time
		)
		if err := rows.Scan(&c.Name, &scoreTxt, &ts); err != nil {
			return nil, nil, time.Time{}, err
		}
		if f, perr := strconv.ParseFloat(strings.TrimSpace(scoreTxt), 64); perr == nil {
			c.Score = f
		} else {
			c.Score = -1 // unparseable legacy value → N/A, never fabricate
		}
		if ts.After(asOf) {
			asOf = ts
		}
		if c.Name == ScorecardOverallName {
			v := c.Score
			overall = &v
			continue // the aggregate is not a check row
		}
		checks = append(checks, c)
	}
	return checks, overall, asOf, rows.Err()
}
