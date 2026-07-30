// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/jackc/pgx/v5"
)

// contributor_history_store.go — store layer for the v0.27.58 daily
// contributor activity history. One claim mechanism drives both the
// bootstrap (gh_history_backfilled_at IS NULL — newly discovered
// contributors are born NULL and stage themselves) and the quarterly
// re-audit (stamp older than the cooldown; heals disclosure-toggle
// flips and other retroactive changes).

// GetContributorsForHistoryBackfill claims contributors for the daily
// history sweep: never-backfilled first, then oldest — with
// contributors whose trailing-year class shows activity
// ('public-active' / 'private-active') prioritized ahead of the quiet
// pool, because they're the rows the frontend renders. Same jittered
// cooldown as breadth (the quarterly re-audit has the same cohort-echo
// dynamics). Served by idx_contributors_history_backfilled.
func (s *PostgresStore) GetContributorsForHistoryBackfill(ctx context.Context, limit int, cooldown time.Duration) ([]ActivityCheckContributor, error) {
	if limit <= 0 {
		return nil, nil
	}
	if cooldown <= 0 {
		cooldown = 90 * 24 * time.Hour
	}
	claim := func(classFilter string, n int) ([]ActivityCheckContributor, error) {
		rows, err := s.pool.Query(ctx, `
			SELECT cntrb_id::text, gh_login
			FROM aveloxis_data.contributors
			WHERE gh_login IS NOT NULL
			  AND gh_login != ''
			  AND COALESCE(cntrb_deleted, 0) = 0
			  `+classFilter+`
			  AND (gh_history_backfilled_at IS NULL
			       OR gh_history_backfilled_at < NOW() - ($2::interval * (1.0 - $3::float8 + random() * 2.0 * $3::float8)))
			ORDER BY gh_history_backfilled_at ASC NULLS FIRST
			LIMIT $1`, n, cooldown.String(), BreadthCooldownJitterFrac)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []ActivityCheckContributor
		for rows.Next() {
			var c ActivityCheckContributor
			if err := rows.Scan(&c.ID, &c.Login); err != nil {
				return nil, err
			}
			out = append(out, c)
		}
		return out, rows.Err()
	}
	// Priority pass: active-classified contributors first.
	out, err := claim(`AND gh_activity_class IN ('public-active', 'private-active')`, limit)
	if err != nil {
		return nil, fmt.Errorf("history backfill claim (priority): %w", err)
	}
	if len(out) < limit {
		seen := make(map[string]bool, len(out))
		for _, c := range out {
			seen[c.ID] = true
		}
		rest, err := claim(`AND gh_activity_class NOT IN ('public-active', 'private-active')`, limit-len(out))
		if err != nil {
			return nil, fmt.Errorf("history backfill claim (fallback): %w", err)
		}
		for _, c := range rest {
			if !seen[c.ID] {
				out = append(out, c)
			}
		}
	}
	return out, nil
}

// StoreContributorActivityHistory upserts a contributor's daily
// history rows and calendar totals AND stamps gh_history_backfilled_at
// in ONE transaction — data and its freshness stamp can never drift.
// Upserts overwrite on the natural keys so the quarterly re-audit
// heals in place. Rows for days that disappeared upstream (deleted
// repos, disclosure changes) are left as historical record.
func (s *PostgresStore) StoreContributorActivityHistory(ctx context.Context, cntrbID string, days []model.ContributorDayActivity, totals []model.ContributorDayTotal) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store activity history begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	b := &pgx.Batch{}
	for _, d := range days {
		b.Queue(`
			INSERT INTO aveloxis_data.contributor_activity_days
				(cntrb_id, day, repo_full_name, commit_count, issue_count, pr_count, review_count, fetched_at)
			VALUES ($1::uuid, $2::date, $3, $4, $5, $6, $7, NOW())
			ON CONFLICT (cntrb_id, day, repo_full_name) DO UPDATE SET
				commit_count = EXCLUDED.commit_count,
				issue_count  = EXCLUDED.issue_count,
				pr_count     = EXCLUDED.pr_count,
				review_count = EXCLUDED.review_count,
				fetched_at   = NOW()`,
			cntrbID, d.Day, d.RepoFullName, d.Commits, d.Issues, d.PRs, d.Reviews)
	}
	for _, dt := range totals {
		b.Queue(`
			INSERT INTO aveloxis_data.contributor_activity_day_totals
				(cntrb_id, day, total_contributions, fetched_at)
			VALUES ($1::uuid, $2::date, $3, NOW())
			ON CONFLICT (cntrb_id, day) DO UPDATE SET
				total_contributions = EXCLUDED.total_contributions,
				fetched_at          = NOW()`,
			cntrbID, dt.Day, dt.Total)
	}
	b.Queue(`UPDATE aveloxis_data.contributors SET gh_history_backfilled_at = NOW() WHERE cntrb_id = $1::uuid`, cntrbID)

	br := tx.SendBatch(ctx, b)
	for i := 0; i < len(days)+len(totals)+1; i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("store activity history exec: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("store activity history close: %w", err)
	}
	return tx.Commit(ctx)
}

// MarkHistoryBackfilled stamps the claim column WITHOUT writing data —
// the path for contributors whose meta/history fetch found no account
// (deleted/renamed). Marking unconditionally keeps dead-enders off the
// NULLS-FIRST claim head (the v0.20.17 lesson).
func (s *PostgresStore) MarkHistoryBackfilled(ctx context.Context, cntrbID string) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors SET gh_history_backfilled_at = NOW() WHERE cntrb_id = $1::uuid`, cntrbID); err != nil {
		return fmt.Errorf("mark history backfilled: %w", err)
	}
	return nil
}
