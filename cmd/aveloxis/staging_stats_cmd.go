// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// stagingStatsCmd surfaces per-repo / per-entity_type staging table
// state for operator diagnosis. v0.22.4 item 6.
//
// Two views, controlled by --repo:
//   - default ("top" mode): top N rows ordered by row count
//   - --repo OWNER/REPO: every entity_type for that repo
//
// Read-only — never writes to the staging table. Safe to run alongside
// `aveloxis serve` or `aveloxis migrate`; the query is a single
// GROUP BY scan with no locks beyond what Postgres takes for the read.
func stagingStatsCmd(cfgPath *string) *cobra.Command {
	var (
		top     int
		repoArg string
	)
	cmd := &cobra.Command{
		Use:   "staging-stats",
		Short: "Show staging-table row counts, age, and approximate disk size per repo / entity_type",
		Long: `Surfaces the state of the aveloxis_ops.staging table for operator
diagnosis. Two views:

  aveloxis staging-stats              # top 10 (repo, entity_type) by row count
  aveloxis staging-stats --top 50     # top 50
  aveloxis staging-stats --repo microsoft/vscode   # every entity_type for one repo

The output includes:
  - rows total / processed / unprocessed
  - oldest and newest created_at (helps verify the v0.22.4
    staging_retention_hours cleanup is working)
  - approximate JSONB byte size (pg_column_size aggregated)

Useful for:
  - confirming the hourly PurgeStagedProcessed sweep is doing its job
    (oldest age should not exceed staging_retention_hours by much)
  - spotting outliers: a single repo with millions of unprocessed
    rows usually means its Processor path quietly broke
  - validating that v0.22.4's retention reduction (7d → 1h) cut the
    table size as expected after deployment.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
			// staging-stats is read-only and trusts that the operator has
			// already run `aveloxis migrate` once.

			rows, err := store.StagingStats(ctx, repoArg, top)
			if err != nil {
				return fmt.Errorf("staging stats: %w", err)
			}
			if len(rows) == 0 {
				fmt.Println("no staging rows found")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			fmt.Fprintln(tw, "REPO\tENTITY_TYPE\tROWS\tPROC\tUNPROC\tOLDEST\tNEWEST\tBYTES")
			for _, r := range rows {
				slug := r.RepoOwner + "/" + r.RepoName
				if slug == "/" {
					slug = fmt.Sprintf("repo_id=%d", r.RepoID)
				}
				fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\n",
					slug,
					r.EntityType,
					r.Rows,
					r.Processed,
					r.Unprocessed,
					r.Oldest.Format("2006-01-02 15:04"),
					r.Newest.Format("2006-01-02 15:04"),
					humanBytes(r.Bytes),
				)
			}
			return tw.Flush()
		},
	}
	cmd.Flags().IntVar(&top, "top", 10, "show the top N (repo, entity_type) pairs by row count (ignored when --repo is set)")
	cmd.Flags().StringVar(&repoArg, "repo", "", "drill into one repo by OWNER/REPO (omits the top-N view)")
	return cmd
}

// humanBytes formats a byte count using SI-style suffixes so the
// staging-stats column stays narrow even for multi-GB cohorts.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}
