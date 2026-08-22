// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// heal_collection_gaps.go — v0.27.140: the ISOLATED healer for the
// v0.27.139 blind-window losses. Deliberately separate from routine
// collection (operator requirement): routine collection keeps its 5%
// gap threshold; this command visits ONLY the count-gap candidates
// (~6,815 repos on aveloxis_large — 4.8% of the fleet, never a 100%
// rescan) and runs the existing GapFiller at threshold 0: full number
// listing → set-diff vs stored → per-item fetch incl. children and
// comments (v0.16.12) → stage → process.
//
// Concurrency safety vs a running serve: each repo is wrapped in the
// v0.18.29 drain lock (status 'queued' → 'collecting'), so
// fillWorkerSlots can't claim it mid-heal and a routine CollectRepo
// can't purge the healer's staging. Repos already 'collecting' are
// SKIPPED and counted — a rerun picks them up. The candidate query is
// itself the resume state: a healed repo's stored counts catch up and
// it drops out on the next run. Neither the drain lock nor the heal
// touches last_collected (the pinned invariant) — only a clean
// end-to-end collection sets it.
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/spf13/cobra"
)

const gapHealPageSize = 200

func healCollectionGapsCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun      bool
		limit       int
		workers     int
		repoID      int64
		afterRepoID int64
		sweepAll    bool
	)

	cmd := &cobra.Command{
		Use:   "heal-collection-gaps",
		Short: "Heal issues/PRs lost to the pre-v0.27.139 blind-window bug (targeted, not a fleet rescan)",
		Long: `Finds repos whose metadata issue/PR counts exceed the stored counts and
runs a threshold-0 gap fill on each: list all numbers from the API,
diff against the database, fetch only the missing items (with their
children and comments), stage, and process.

Only count-gap candidates are visited (~5% of a typical fleet). The
candidate query is the resume state — healed repos drop out on the
next run, so re-running until "0 candidates" is the workflow. Safe
beside a running serve: repos are drain-locked per heal; repos
mid-collection are skipped and picked up by a rerun.

--all sweeps every collected repo instead (completeness mode for the
count-netting corner where stored-but-deleted rows hide a gap). Not
recommended for routine use; prefer --repo-id for a specific suspect.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, logger)

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-heal-gaps"), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			ghKeys, glKeys, err := loadKeys(ctx, cfg, store, false, logger)
			if err != nil {
				return fmt.Errorf("loading API keys: %w", err)
			}
			ghClient := github.New(cfg.GitHub.BaseURL, ghKeys, logger)
			glClient := gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)

			workerID := fmt.Sprintf("gap-heal-%d", os.Getpid())
			if workers < 1 {
				workers = 1
			}

			var totalFilled, totalFailed, totalSkipped, totalVisited int64

			// Round-23: --all and --repo-id run in FORCE-LIST mode —
			// threshold 0 still requires metadata > gathered, which
			// cannot see the count-netting case (retained deleted rows
			// offsetting missing ones); the completeness modes make the
			// listing itself the truth source.
			threshold := 0.0
			if sweepAll || repoID > 0 {
				threshold = collector.GapForceList
			}

			healOne := func(c db.GapHealCandidate) {
				var client platform.Client
				switch c.Platform {
				case model.PlatformGitHub:
					client = ghClient
				case model.PlatformGitLab:
					client = glClient
				default:
					return // generic git — nothing to list
				}
				// Drain-lock: only 'queued' rows lock; a repo
				// mid-collection is skipped (rerun catches it).
				locked, lerr := store.LockReposForDrain(ctx, []int64{c.RepoID}, workerID)
				if lerr != nil {
					logger.Warn("drain lock failed — skipping repo", "repo_id", c.RepoID, "error", lerr)
					atomic.AddInt64(&totalFailed, 1)
					return
				}
				if len(locked) == 0 {
					logger.Info("repo is mid-collection — skipped (rerun picks it up)", "repo_id", c.RepoID)
					atomic.AddInt64(&totalSkipped, 1)
					return
				}
				defer func() {
					if rerr := store.ReleaseDrainLock(ctx, c.RepoID, workerID); rerr != nil {
						logger.Warn("drain unlock failed", "repo_id", c.RepoID, "error", rerr)
					}
				}()

				gf := collector.NewGapFillerWithMode(store, client, logger, cfg.Collection.PRChildMode)
				filled, ferr := gf.AssessAndFillGapsWithThreshold(ctx, c.RepoID, c.Owner, c.Name, c.MetaIssues, c.MetaPRs, threshold)
				atomic.AddInt64(&totalVisited, 1)
				atomic.AddInt64(&totalFilled, int64(filled))
				if ferr != nil {
					logger.Warn("gap heal error", "repo_id", c.RepoID, "owner", c.Owner, "repo", c.Name, "error", ferr)
					atomic.AddInt64(&totalFailed, 1)
					return
				}
				// Round-23: refresh the queue's cached counts so a healed
				// repo DROPS OUT of the candidate set — without this,
				// "rerun until 0 candidates" never converged (the healer
				// fills rows without a CompleteJob pass). A failed
				// refresh only means the rerun revisits this repo's
				// (cheap) listing — warn, don't fail the heal.
				if rerr := store.RefreshQueueGatheredCounts(ctx, c.RepoID); rerr != nil {
					logger.Warn("gathered-count refresh failed — repo stays a candidate until rerun", "repo_id", c.RepoID, "error", rerr)
				}
				logger.Info("repo healed", "repo_id", c.RepoID, "owner", c.Owner, "repo", c.Name,
					"count_gap", c.Gap, "filled", filled)
			}

			// Single-repo mode.
			if repoID > 0 {
				repo, rerr := store.GetRepoByID(ctx, repoID)
				if rerr != nil {
					return fmt.Errorf("repo %d: %w", repoID, rerr)
				}
				metaIssues, metaPRs, merr := store.GetRepoMetaCounts(ctx, repoID)
				if merr != nil {
					return fmt.Errorf("meta counts for repo %d: %w", repoID, merr)
				}
				healOne(db.GapHealCandidate{RepoID: repoID, Owner: repo.Owner, Name: repo.Name,
					Platform: repo.Platform, MetaIssues: metaIssues, MetaPRs: metaPRs})
				if totalFailed > 0 {
					return fmt.Errorf("gap heal finished with %d failure(s)", totalFailed)
				}
				return nil
			}

			// Fleet mode: keyset pages of candidates through a worker pool.
			after := afterRepoID
			processed := 0
			for {
				pageSize := gapHealPageSize
				if limit > 0 && limit-processed < pageSize {
					pageSize = limit - processed
				}
				if pageSize <= 0 {
					break
				}
				candidates, cerr := store.GetGapHealCandidates(ctx, after, pageSize, sweepAll)
				if cerr != nil {
					return fmt.Errorf("candidate query: %w", cerr)
				}
				if len(candidates) == 0 {
					break
				}
				if dryRun {
					for _, c := range candidates {
						fmt.Printf("repo %d  %s/%s  count_gap=%d  (meta issues=%d prs=%d)\n",
							c.RepoID, c.Owner, c.Name, c.Gap, c.MetaIssues, c.MetaPRs)
					}
				} else {
					ch := make(chan db.GapHealCandidate)
					var wg sync.WaitGroup
					for range workers {
						wg.Add(1)
						go func() {
							defer wg.Done()
							for c := range ch {
								healOne(c)
							}
						}()
					}
					for _, c := range candidates {
						ch <- c
					}
					close(ch)
					wg.Wait()
				}
				processed += len(candidates)
				after = candidates[len(candidates)-1].RepoID
			}

			if dryRun {
				fmt.Printf("\n%d candidate repo(s). Re-run without --dry-run to heal.\n", processed)
				return nil
			}
			logger.Info("gap heal complete",
				"candidates", processed, "healed", totalVisited,
				"items_filled", totalFilled, "skipped_collecting", totalSkipped,
				"failed", totalFailed)
			if totalFailed > 0 {
				return fmt.Errorf("gap heal finished with %d failure(s) — rerun retries them (the candidate query is the resume state)", totalFailed)
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "list candidates and their count gaps without healing")
	cmd.Flags().IntVar(&limit, "limit", 0, "cap the number of candidates visited this run (0 = all)")
	cmd.Flags().IntVar(&workers, "workers", 4, "concurrent per-repo heals")
	cmd.Flags().Int64Var(&repoID, "repo-id", 0, "heal exactly one repo by id (bypasses candidate selection)")
	cmd.Flags().Int64Var(&afterRepoID, "after-repo-id", 0, "keyset resume point")
	cmd.Flags().BoolVar(&sweepAll, "all", false, "sweep every collected repo, not just count-gap candidates (completeness mode; not recommended for routine use)")
	return cmd
}
