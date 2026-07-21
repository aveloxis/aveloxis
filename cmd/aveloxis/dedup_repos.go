// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// `aveloxis dedup-repos` — v0.25.32 one-shot merge of case-variant
// duplicate repositories. GitHub/GitLab treat owner/repo paths
// case-insensitively, but repos.repo_git was byte-exact-unique, so
// bulk-pasted case variants (github.com/azure/x vs github.com/Azure/x)
// created full duplicate rows that each collected the same repository
// twice.
//
// Per pair (winner = oldest repo_id): user_repos links repoint to the
// winner, shared-copy rows (messages, email_message, commit_comment_ref,
// contributor_repo) repoint, and the loser's duplicated child data plus
// the loser row itself are deleted — the winner holds the same data.
//
// Operator workflow:
//
//	aveloxis dedup-repos --dry-run      # show the plan
//	aveloxis dedup-repos --limit 50     # canary batch
//	aveloxis dedup-repos                # full run (re-run until 0 pairs)
//	aveloxis migrate --skip-views       # then: builds uq_repos_repo_git_ci
//	aveloxis refresh-views              # analytics stop double-counting
//
// See CLAUDE.md v0.25.32 for the full rationale.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func dedupReposCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun    bool
		batchSize int
		limit     int
	)

	cmd := &cobra.Command{
		Use:   "dedup-repos",
		Short: "Merge case-variant duplicate repositories (GitHub/GitLab case-insensitive URLs)",
		Long: `Opt-in one-shot data consolidation (v0.25.32).

For each group of repos whose repo_git differs only by case on a forge
platform (GitHub/GitLab — their owner/repo paths are case-insensitive,
so the variants ARE the same repository):

  1. Repoint aveloxis_ops.user_repos to the winner (oldest repo_id) so
     every user group that referenced either variant keeps the repo and
     immediately sees the winner's collected data.
  2. Repoint shared-copy rows (messages, email_message,
     commit_comment_ref, contributor_repo, foundation_membership) —
     these are globally unique, the pair shares one copy.
  3. Delete the loser's duplicated child data (issues, PRs, commits,
     ...) leaves-first, then the loser repos row. Nothing is lost: both
     sides collected the same repository.
  4. Enqueue the winner if it has never been collected.

Pairs with either side mid-collection (status='collecting') are skipped
and reported; re-run after those jobs finish. The command is idempotent
— resolved pairs drop out of the candidate set.

After the fleet reports 0 pairs, run 'aveloxis migrate --skip-views' to
build the uq_repos_repo_git_ci unique index (the permanent DB-level
backstop), then 'aveloxis refresh-views' so matviews stop
double-counting.

Use --dry-run first to see the plan.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDedupRepos(*cfgPath, dryRun, batchSize, limit)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show the merge plan without writing")
	cmd.Flags().IntVar(&batchSize, "batch-size", 50, "pairs merged per batch (each pair is one heavy transaction)")
	cmd.Flags().IntVar(&limit, "limit", 0, "cap total pairs merged this run (0 = no cap)")

	return cmd
}

func runDedupRepos(cfgPath string, dryRun bool, batchSize, limit int) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// v0.21.5: store.Migrate(ctx) intentionally NOT called here — only
	// serve and migrate run schema migrations.
	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	count, err := db.CountCaseVariantRepoDups(ctx, store)
	if err != nil {
		return fmt.Errorf("count duplicate groups: %w", err)
	}
	logger.Info("v0.25.32 case-variant repo dedup plan",
		"duplicate_groups", count,
		"dry_run", dryRun,
		"batch_size", batchSize,
		"limit_cap", limit)

	if count == 0 {
		logger.Info("no case-variant duplicate repos — nothing to merge. " +
			"Run `aveloxis migrate --skip-views` to build the uq_repos_repo_git_ci backstop if it doesn't exist yet.")
		return nil
	}

	if dryRun {
		sample, err := db.SampleCaseVariantRepoDups(ctx, store, 20)
		if err != nil {
			return fmt.Errorf("sample duplicate pairs: %w", err)
		}
		logger.Info("=== sample plan (first 20 pairs) ===")
		for _, p := range sample {
			logger.Info("  duplicate pair",
				"winner_id", p.WinnerID, "winner_git", p.WinnerGit,
				"loser_id", p.LoserID, "loser_git", p.LoserGit,
				"group_size", p.GroupSize,
				"winner_last_collected", fmtNullableTime(p.WinnerLastCollected),
				"loser_last_collected", fmtNullableTime(p.LoserLastCollected),
				"collecting_now", p.Collecting)
		}
		logger.Info("dry-run complete — no changes written. Re-run without --dry-run to apply.")
		return nil
	}

	merged := 0
	skipped := 0
	startedAt := time.Now()
	for {
		if limit > 0 && merged >= limit {
			logger.Info("hit --limit cap, stopping", "merged", merged, "limit", limit)
			break
		}
		thisBatch := batchSize
		if limit > 0 && merged+thisBatch > limit {
			thisBatch = limit - merged
		}

		batchMerged, batchSkipped, err := db.DedupCaseVariantReposBatch(ctx, store, thisBatch)
		merged += batchMerged
		skipped = batchSkipped // per-round snapshot; skipped pairs recur each round
		if err != nil {
			logger.Error("batch dedup errored mid-way — completed pairs are preserved",
				"batch_merged", batchMerged, "total_merged", merged, "error", err)
			return err
		}
		logger.Info("batch complete",
			"batch_merged", batchMerged,
			"skipped_collecting", batchSkipped,
			"total_merged", merged,
			"elapsed", time.Since(startedAt).Round(time.Second))

		if batchMerged == 0 {
			break
		}
	}

	logger.Info("v0.25.32 case-variant repo dedup complete",
		"merged", merged,
		"skipped_collecting", skipped,
		"duration", time.Since(startedAt).Round(time.Second))
	if skipped > 0 {
		logger.Info("some pairs were mid-collection and skipped — re-run once those jobs finish",
			"skipped_collecting", skipped)
	}
	if merged > 0 {
		logger.Info("next steps: `aveloxis migrate --skip-views` builds the unique-index backstop; " +
			"`aveloxis refresh-views` rebuilds matviews so analytics stop double-counting")
	}
	return nil
}

// fmtNullableTime renders a nullable timestamp for dry-run output.
func fmtNullableTime(t *time.Time) string {
	if t == nil {
		return "never"
	}
	return t.Format("2006-01-02")
}
