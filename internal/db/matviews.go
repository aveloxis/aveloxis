// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — matviews.go manages materialized views used by 8Knot and other
// analytics tools. Views are created during migration and refreshed periodically.
package db

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"time"
)

//go:embed matviews.sql
var matviewsSQL string

// viewsSQL holds the base-table plain views executed on EVERY migrate
// (v0.27.115 drift-audit finding 1 — see views.sql's header for why
// matviews.sql is structurally unreachable on populated fleets).
//
//go:embed views.sql
var viewsSQL string

// CreateMaterializedViews creates or replaces all materialized views.
// Safe to run repeatedly (uses DROP IF EXISTS + CREATE).
func CreateMaterializedViews(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	logger.Info("creating materialized views")
	_, err := pg.pool.Exec(ctx, matviewsSQL)
	if err != nil {
		return fmt.Errorf("creating materialized views: %w", err)
	}
	logger.Info("materialized views created")
	return nil
}

// CreateMaterializedViewsIfNotExist creates views only on first run.
// If the first view already exists, this is a no-op. Much faster than
// CreateMaterializedViews which drops and recreates every time.
func CreateMaterializedViewsIfNotExist(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	var exists bool
	if err := pg.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_matviews
			WHERE schemaname = 'aveloxis_data' AND matviewname = 'api_get_all_repo_prs'
		)`).Scan(&exists); err != nil {
		return fmt.Errorf("probing for existing matviews: %w", err)
	}
	if exists {
		logger.Info("materialized views already exist, skipping creation on startup (use 'aveloxis refresh-views' or wait for scheduled rebuild)")
		return nil
	}
	return CreateMaterializedViews(ctx, pg, logger)
}

// matviewNames lists all materialized views to refresh, in order.
var matviewNames = []string{
	"aveloxis_data.api_get_all_repo_prs",
	"aveloxis_data.api_get_all_repos_commits",
	"aveloxis_data.api_get_all_repos_issues",
	"aveloxis_data.explorer_entry_list",
	"aveloxis_data.explorer_commits_and_committers_daily_count",
	"aveloxis_data.explorer_contributor_actions",
	// augur_new_contributors is a VIEW alias for
	// explorer_contributor_actions as of v0.25.6 (was a MATERIALIZED
	// VIEW pre-v0.25.5, dropped outright in v0.25.5, restored as a
	// plain VIEW in v0.25.6 because operators query it to identify
	// new contributors). VIEWs don't need refresh — they read from
	// the underlying matview live.
	"aveloxis_data.explorer_new_contributors",
	"aveloxis_data.explorer_user_repos",
	"aveloxis_data.explorer_pr_response_times",
	"aveloxis_data.explorer_pr_assignments",
	"aveloxis_data.explorer_issue_assignments",
	"aveloxis_data.explorer_pr_response",
	"aveloxis_data.explorer_repo_languages",
	// explorer_libyear_all converted to a regular VIEW alias in v0.25.5
	// (was byte-for-byte identical SQL to _summary). The alias name stays
	// queryable for downstream tooling but doesn't need a refresh pass.
	"aveloxis_data.explorer_libyear_summary",
	"aveloxis_data.explorer_libyear_detail",
	"aveloxis_data.issue_reporter_created_at",
	"aveloxis_data.explorer_contributor_recent_actions",
	"aveloxis_data.explorer_pr_files",
	"aveloxis_data.explorer_cntrb_per_file",
	"aveloxis_data.explorer_repo_files",
}

// RefreshMaterializedViews refreshes all materialized views concurrently.
// Uses CONCURRENTLY where a unique index exists (doesn't lock reads during refresh).
// Falls back to non-concurrent refresh if the view has never been populated.
func RefreshMaterializedViews(ctx context.Context, pg *PostgresStore, logger *slog.Logger) error {
	start := time.Now()
	logger.Info("refreshing materialized views", "count", len(matviewNames))

	for _, name := range matviewNames {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		viewStart := time.Now()

		// Try CONCURRENTLY first (requires a unique index and at least one row).
		_, err := pg.pool.Exec(ctx, fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", name))
		if err != nil {
			// Fall back to non-concurrent refresh (blocks reads but always works).
			_, err = pg.pool.Exec(ctx, fmt.Sprintf("REFRESH MATERIALIZED VIEW %s", name))
			if err != nil {
				logger.Warn("failed to refresh materialized view",
					"view", name, "error", err, "duration", time.Since(viewStart))
				continue // Don't abort all views if one fails.
			}
		}

		logger.Info("refreshed materialized view",
			"view", name, "duration", time.Since(viewStart).Truncate(time.Millisecond))
	}

	logger.Info("materialized view refresh complete",
		"total_duration", time.Since(start).Truncate(time.Second))
	return nil
}
