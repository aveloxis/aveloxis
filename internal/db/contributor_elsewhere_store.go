// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"
)

// v0.27.64 — the read surface over the v0.27.58 contributor daily
// history (contributor_activity_days / _day_totals, fed by GitHub's
// contributionsCollection):
//
//   - ContributorsElsewhere: "where else are this repo's top
//     contributors active?" — the repo page's cross-repo matrix.
//   - ContributorActivity: one person's monthly per-repo view + the
//     disclosed-total day series.
//
// THE HONESTY RULE (v0.27.58): every contributor entry carries
// contributors.gh_history_backfilled_at. An un-backfilled contributor
// has NO history rows, and rendering that absence as "active nowhere
// else" would be a lie — the frontend shows "history pending" when
// the stamp is nil.

// ElsewhereRepo is one other-repo aggregate for a contributor.
// RepoID is non-nil only when the repo is tracked on THIS instance
// (GitHub side — the history source is GitHub-only, so the link join
// is restricted to platform_id = 1).
type ElsewhereRepo struct {
	RepoFullName string `json:"repo_full_name"`
	RepoID       *int64 `json:"repo_id,omitempty"`
	ActiveDays   int    `json:"active_days"`
	Commits      int    `json:"commits"`
	Issues       int    `json:"issues"`
	PRs          int    `json:"prs"`
	Reviews      int    `json:"reviews"`
}

// ElsewhereContributor is one top contributor with their other-repo
// activity.
type ElsewhereContributor struct {
	CntrbID      string          `json:"cntrb_id"`
	Login        string          `json:"login"`
	BackfilledAt *time.Time      `json:"backfilled_at"`
	Elsewhere    []ElsewhereRepo `json:"elsewhere"`
}

// ContributorsElsewhere returns the repo's top-N contributors (by the
// same ranking as TopContributors) with each one's top other repos
// from the daily-history tables, aggregated over [since, now).
//
// The CURRENT repo is excluded case-insensitively: repo_full_name is
// GitHub-canonical TEXT and our stored owner/name casing can drift
// from it (the v0.25.32 case lesson).
func (s *PostgresStore) ContributorsElsewhere(ctx context.Context, repoID int64, since time.Time, topN, reposPer int) ([]ElsewhereContributor, error) {
	if topN <= 0 {
		topN = 10
	}
	if reposPer <= 0 {
		reposPer = 10
	}

	top, err := s.TopContributors(ctx, repoID, since, time.Time{}, topN)
	if err != nil {
		return nil, err
	}
	if len(top) == 0 {
		return []ElsewhereContributor{}, nil
	}

	ids := make([]string, len(top))
	out := make([]ElsewhereContributor, len(top))
	index := make(map[string]int, len(top))
	for i, tc := range top {
		ids[i] = tc.CntrbID
		out[i] = ElsewhereContributor{CntrbID: tc.CntrbID, Login: tc.Login, Elsewhere: []ElsewhereRepo{}}
		index[tc.CntrbID] = i
	}

	// Backfill stamps for the whole cohort (nil = history pending).
	stampRows, err := s.pool.Query(ctx, `
		SELECT cntrb_id::text, gh_history_backfilled_at
		FROM aveloxis_data.contributors
		WHERE cntrb_id = ANY($1::uuid[])`, ids)
	if err != nil {
		return nil, fmt.Errorf("ContributorsElsewhere stamps: %w", err)
	}
	for stampRows.Next() {
		var id string
		var at *time.Time
		if err := stampRows.Scan(&id, &at); err != nil {
			stampRows.Close()
			return nil, fmt.Errorf("ContributorsElsewhere stamp scan: %w", err)
		}
		if i, ok := index[id]; ok {
			out[i].BackfilledAt = at
		}
	}
	stampRows.Close()
	if err := stampRows.Err(); err != nil {
		return nil, fmt.Errorf("ContributorsElsewhere stamps rows: %w", err)
	}

	// Per-contributor other-repo aggregates, ranked by active days.
	// The repo_id link joins platform 1 only — the history source is
	// GitHub's contributionsCollection, and the platform filter keeps
	// the join at most 1:1 under uq_repos_repo_git_ci.
	rows, err := s.pool.Query(ctx, `
		WITH agg AS (
		    SELECT ad.cntrb_id, ad.repo_full_name,
		           COUNT(*)::int              AS active_days,
		           SUM(ad.commit_count)::int  AS commits,
		           SUM(ad.issue_count)::int   AS issues,
		           SUM(ad.pr_count)::int      AS prs,
		           SUM(ad.review_count)::int  AS reviews
		    FROM aveloxis_data.contributor_activity_days ad
		    WHERE ad.cntrb_id = ANY($1::uuid[])
		      AND ad.day >= $2::date
		      AND LOWER(ad.repo_full_name) <> LOWER(
		            (SELECT r.repo_owner || '/' || r.repo_name
		             FROM aveloxis_data.repos r WHERE r.repo_id = $3))
		    GROUP BY ad.cntrb_id, ad.repo_full_name
		),
		ranked AS (
		    SELECT agg.*,
		           ROW_NUMBER() OVER (
		               PARTITION BY agg.cntrb_id
		               ORDER BY agg.active_days DESC,
		                        (agg.commits + agg.issues + agg.prs + agg.reviews) DESC,
		                        agg.repo_full_name) AS rn
		    FROM agg
		)
		SELECT rk.cntrb_id::text, rk.repo_full_name, tr.repo_id,
		       rk.active_days, rk.commits, rk.issues, rk.prs, rk.reviews
		FROM ranked rk
		LEFT JOIN aveloxis_data.repos tr
		    ON LOWER(tr.repo_owner || '/' || tr.repo_name) = LOWER(rk.repo_full_name)
		    AND tr.platform_id = 1
		WHERE rk.rn <= $4
		ORDER BY rk.cntrb_id, rk.rn`, ids, since, repoID, reposPer)
	if err != nil {
		return nil, fmt.Errorf("ContributorsElsewhere query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var er ElsewhereRepo
		if err := rows.Scan(&id, &er.RepoFullName, &er.RepoID,
			&er.ActiveDays, &er.Commits, &er.Issues, &er.PRs, &er.Reviews); err != nil {
			return nil, fmt.Errorf("ContributorsElsewhere scan: %w", err)
		}
		if i, ok := index[id]; ok {
			out[i].Elsewhere = append(out[i].Elsewhere, er)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ContributorsElsewhere rows: %w", err)
	}
	return out, nil
}

// ActivityMonth is one month bucket of one repo's activity.
type ActivityMonth struct {
	Month      string `json:"month"` // YYYY-MM
	ActiveDays int    `json:"active_days"`
	Commits    int    `json:"commits"`
	Issues     int    `json:"issues"`
	PRs        int    `json:"prs"`
	Reviews    int    `json:"reviews"`
}

// ActivityRepo is one repo in a contributor's monthly view.
type ActivityRepo struct {
	RepoFullName string          `json:"repo_full_name"`
	RepoID       *int64          `json:"repo_id,omitempty"`
	Months       []ActivityMonth `json:"months"`
}

// DayTotal is one day of the contribution calendar's disclosed total
// (includes private contributions when the user enabled disclosure).
type DayTotal struct {
	Day   string `json:"day"` // YYYY-MM-DD
	Total int    `json:"total"`
}

// ContributorActivityView is the person-level cross-repo response.
type ContributorActivityView struct {
	CntrbID      string         `json:"cntrb_id"`
	Login        string         `json:"login"`
	BackfilledAt *time.Time     `json:"backfilled_at"`
	Months       int            `json:"months"`
	Repos        []ActivityRepo `json:"repos"`
	DayTotals    []DayTotal     `json:"day_totals"`
}

// ContributorActivity returns one contributor's monthly per-repo
// activity (top 10 repos by total activity in the window) plus the
// disclosed-total day series, over the trailing `months` months.
func (s *PostgresStore) ContributorActivity(ctx context.Context, cntrbID string, months int) (*ContributorActivityView, error) {
	if months <= 0 {
		months = 24
	}
	since := time.Now().AddDate(0, -months, 0)

	view := &ContributorActivityView{
		CntrbID: cntrbID,
		Months:  months,
		Repos:   []ActivityRepo{},
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(NULLIF(cntrb_login, ''), gh_login, gl_username, ''), gh_history_backfilled_at
		FROM aveloxis_data.contributors
		WHERE cntrb_id = $1::uuid AND COALESCE(cntrb_deleted, 0) = 0`,
		cntrbID).Scan(&view.Login, &view.BackfilledAt); err != nil {
		return nil, fmt.Errorf("ContributorActivity contributor lookup: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		WITH windowed AS (
		    SELECT ad.repo_full_name,
		           to_char(date_trunc('month', ad.day), 'YYYY-MM') AS month,
		           COUNT(*)::int             AS active_days,
		           SUM(ad.commit_count)::int AS commits,
		           SUM(ad.issue_count)::int  AS issues,
		           SUM(ad.pr_count)::int     AS prs,
		           SUM(ad.review_count)::int AS reviews
		    FROM aveloxis_data.contributor_activity_days ad
		    WHERE ad.cntrb_id = $1::uuid AND ad.day >= $2::date
		    GROUP BY ad.repo_full_name, date_trunc('month', ad.day)
		),
		repo_rank AS (
		    SELECT repo_full_name,
		           SUM(commits + issues + prs + reviews) AS total
		    FROM windowed
		    GROUP BY repo_full_name
		    ORDER BY total DESC, repo_full_name
		    LIMIT 10
		)
		SELECT w.repo_full_name, tr.repo_id, w.month,
		       w.active_days, w.commits, w.issues, w.prs, w.reviews
		FROM windowed w
		JOIN repo_rank rr USING (repo_full_name)
		LEFT JOIN aveloxis_data.repos tr
		    ON LOWER(tr.repo_owner || '/' || tr.repo_name) = LOWER(w.repo_full_name)
		    AND tr.platform_id = 1
		ORDER BY rr.total DESC, w.repo_full_name, w.month`, cntrbID, since)
	if err != nil {
		return nil, fmt.Errorf("ContributorActivity months query: %w", err)
	}
	defer rows.Close()

	byRepo := map[string]int{}
	for rows.Next() {
		var name, month string
		var repoLink *int64
		var m ActivityMonth
		if err := rows.Scan(&name, &repoLink, &month, &m.ActiveDays, &m.Commits, &m.Issues, &m.PRs, &m.Reviews); err != nil {
			return nil, fmt.Errorf("ContributorActivity months scan: %w", err)
		}
		m.Month = month
		i, ok := byRepo[name]
		if !ok {
			view.Repos = append(view.Repos, ActivityRepo{RepoFullName: name, RepoID: repoLink, Months: []ActivityMonth{}})
			i = len(view.Repos) - 1
			byRepo[name] = i
		}
		view.Repos[i].Months = append(view.Repos[i].Months, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ContributorActivity months rows: %w", err)
	}

	dtRows, err := s.pool.Query(ctx, `
		SELECT to_char(day, 'YYYY-MM-DD'), total_contributions
		FROM aveloxis_data.contributor_activity_day_totals
		WHERE cntrb_id = $1::uuid AND day >= $2::date
		ORDER BY day`, cntrbID, since)
	if err != nil {
		return nil, fmt.Errorf("ContributorActivity day totals: %w", err)
	}
	defer dtRows.Close()

	view.DayTotals = []DayTotal{}
	for dtRows.Next() {
		var dt DayTotal
		if err := dtRows.Scan(&dt.Day, &dt.Total); err != nil {
			return nil, fmt.Errorf("ContributorActivity day-total scan: %w", err)
		}
		view.DayTotals = append(view.DayTotals, dt)
	}
	return view, dtRows.Err()
}
