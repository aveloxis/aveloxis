// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// healCommitDailyCmd fills aveloxis_data.repo_commit_daily (summary/49) for
// repositories whose daily picture is not complete (never filled, or filled
// only by a walk that swallowed writes — repos.commit_daily_complete_at
// unset): one scan of each repository's commit rows, largest first, each in
// its own transaction, stamped complete at the end.
func healCommitDailyCmd(cfgPath *string) *cobra.Command {
	var apply bool
	var limit int
	cmd := &cobra.Command{
		Use:   "heal-commit-daily",
		Short: "Fill the daily commit counts for repositories whose picture is not complete (dry run unless --apply)",
		Long: `The repository page's weekly commit series and the commits column of its
top contributors read aveloxis_data.repo_commit_daily — distinct commits per
repository, UTC day and author email — which the facade writes after every
completed walk of a repository's default branch — and use it only once the
repository's picture is COMPLETE (repos.commit_daily_complete_at, set by a
walk whose every commit was proven written, and by this command). A
repository collected before 0.29.73 has no rows there until its next
collection, and one whose only walk since swallowed commit writes has rows
but no stamp; either way its page reads the commits table instead: one row
per file per commit, scattered across the table, minutes for a kernel fork.

This command fills those repositories now, largest first, by one scan of
each repository's commit rows (the cost its first page view pays today, paid
once here). Each repository is its own transaction, so an interrupt loses at
most the one in flight; rerun to finish. It can run while serve runs.
Without --apply it only counts the repositories that need it.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return err
			}
			defer store.Close()

			// A dry run reports the whole count; --limit bounds only what
			// --apply fills (review round 1 F7).
			listLimit := 1 << 30
			if apply && limit > 0 {
				listLimit = limit
			}
			ids, err := store.ListReposNeedingCommitDaily(ctx, listLimit)
			pending := int64(len(ids))
			var filled int64
			if err == nil && apply {
				for _, id := range ids {
					started := time.Now()
					rows, ferr := store.FillRepoCommitDailyFromCommits(ctx, id)
					if ferr != nil {
						err = fmt.Errorf("repository %d: %w", id, ferr)
						break
					}
					filled++
					logger.Info("daily commit counts filled", "repo_id", id, "rows", rows,
						"elapsed", time.Since(started).Truncate(time.Millisecond), "done", filled, "of", pending)
				}
			}
			line, err := healCommitDailyReport(apply, pending, filled, err)
			if err != nil {
				return err
			}
			fmt.Println(line)
			return nil
		},
	}
	cmd.Flags().BoolVar(&apply, "apply", false, "actually fill the rows (without this the command only counts)")
	cmd.Flags().IntVar(&limit, "limit", 0, "fill at most this many repositories, largest first (0 = all)")
	return cmd
}

// healCommitDailyReport is the command's closing line or error.
func healCommitDailyReport(apply bool, pending, filled int64, err error) (string, error) {
	switch {
	case err != nil && errors.Is(err, context.Canceled):
		if !apply {
			return "", fmt.Errorf("heal-commit-daily (dry run) interrupted — nothing was written: %w", err)
		}
		return "", fmt.Errorf("heal-commit-daily interrupted after %d of %d repositories — rerun to finish; filled repositories stay filled: %w", filled, pending, err)
	case err != nil:
		return "", fmt.Errorf("heal-commit-daily after %d of %d repositories: %w", filled, pending, err)
	case !apply:
		return fmt.Sprintf("heal-commit-daily (dry run): %d repositories have commits but no complete daily counts; re-run with --apply to fill them, largest first", pending), nil
	}
	return fmt.Sprintf("heal-commit-daily: %d of %d repositories filled", filled, pending), nil
}
