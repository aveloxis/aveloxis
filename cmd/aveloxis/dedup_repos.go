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
// winner, shared-copy rows (messages, email_message, commit_comment_ref)
// repoint, and the loser's duplicated child data plus the loser row
// itself are deleted — the winner holds the same data. contributor_repo
// is deliberately untouched (v0.25.34).
//
// Precondition (v0.28.18): the migrate on this binary must have built
// the email_message FK indexes (idx_email_message_repo_id /
// _signaled_repo_id) — the store refuses to merge without them, since
// every pair would otherwise sequential-scan that table (12 GB on the
// mailing-list deployment).
//
// Operator workflow:
//
//	aveloxis migrate --skip-views       # first: the v0.28.18 email_message indexes
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
     commit_comment_ref, foundation_membership) — these are globally
     unique, the pair shares one copy. contributor_repo is deliberately
     untouched: the breadth worker's observational record, keyed by the
     numeric gh_repo_id.
  3. Delete the loser's duplicated child data (issues, PRs, commits,
     ...) leaves-first, then the loser repos row. Nothing is lost: both
     sides collected the same repository.
  4. Enqueue the winner if it has never been collected.

Pairs with either side mid-collection (status='collecting') are left
out of each batch's window so they cannot stall the run, and the final
summary reports how many groups remain; re-run after those jobs finish.
The command is idempotent — resolved pairs drop out of the candidate
set.

Requires the v0.28.18 migrate to have built the email_message FK
indexes (idx_email_message_repo_id / _signaled_repo_id): the merge
refuses to start without them rather than sequential-scan the table
per pair — run 'aveloxis migrate --skip-views' on this binary first.

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
	skipped := 0 // run total of in-transaction races (collecting pairs sit outside the window since v0.28.18)
	capped := false
	startedAt := time.Now()
	for {
		if limit > 0 && merged >= limit {
			logger.Info("hit --limit cap, stopping", "merged", merged, "limit", limit)
			capped = true
			break
		}
		thisBatch := batchSize
		if limit > 0 && merged+thisBatch > limit {
			thisBatch = limit - merged
		}

		batchMerged, batchSkipped, err := db.DedupCaseVariantReposBatch(ctx, store, thisBatch)
		merged += batchMerged
		skipped += batchSkipped
		if err != nil {
			if errors.Is(err, db.ErrEmailMessageIndexesNotReady) {
				// The v0.28.18 precondition: nothing in this batch was
				// touched — the store refuses before any write.
				logger.Error("precondition unmet — stopping: run `aveloxis migrate --skip-views` on this binary first",
					"total_merged", merged, "error", err)
				return err
			}
			logger.Error("batch dedup errored mid-way — completed pairs are preserved",
				"batch_merged", batchMerged, "total_merged", merged, "error", err)
			return err
		}
		logger.Info("batch complete",
			"batch_merged", batchMerged,
			"skipped_collecting_races", batchSkipped,
			"total_merged", merged,
			"elapsed", time.Since(startedAt).Round(time.Second))

		if batchMerged == 0 {
			break
		}
	}

	logger.Info("v0.25.32 case-variant repo dedup complete",
		"merged", merged,
		"skipped_collecting_races", skipped,
		"duration", time.Since(startedAt).Round(time.Second))
	// v0.28.18: mid-collection pairs sit outside the batch window, so
	// "merged == 0" no longer means "nothing left" — report what remains
	// (SR-19: the rerun-until-0 contract needs the remaining count, not
	// the per-round skip count, to be honest).
	remaining, rerr := db.CountCaseVariantRepoDups(ctx, store)
	switch {
	case rerr != nil:
		// The remaining count IS the rerun-until-0 signal; losing it must
		// not read as success to a script (v0.27.106: nonzero on
		// incomplete work). Every merge is already committed.
		logger.Error("could not count remaining duplicate groups — re-run to verify", "error", rerr)
		return fmt.Errorf("count remaining duplicate groups: %w", rerr)
	case remaining == 0:
		logger.Info("no duplicate groups remain")
	case capped:
		logger.Info("hit --limit; duplicate groups remain — re-run to continue",
			"remaining_groups", remaining, "limit", limit)
	default:
		// The loop exited on merged == 0 with no cap: every remaining
		// group sat outside the window (mid-collection) or arrived since.
		logger.Info("duplicate groups remain (mid-collection during this run, or added since) — re-run once those jobs finish",
			"remaining_groups", remaining, "skipped_collecting_races", skipped)
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
