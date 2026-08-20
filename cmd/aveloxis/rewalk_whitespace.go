// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// rewalk-whitespace (v0.27.105) — operator-driven full-history
// whitespace bootstrap for the existing fleet (fill-audit Workstream C).
//
// Why it exists: commits.cmt_whitespace was 0% populated for the
// product's life while SUM(cmt_whitespace) feeds six live aggregate
// queries. The forward path (the facade's per-cycle whitespace phase)
// only walks incrementally past a stamped marker; this command does the
// one-time full-history walk per repo — reusing the facade's PERSISTENT
// bare clones, so no re-clone cost for already-collected repos — and
// stamps the marker so every subsequent cycle stays incremental.
//
// RUNTIME EXPECTATION (grounded 2026-08-19): git log -p measured at
// ~209 MB/s single-threaded on an M-series laptop; the fleet's patch
// volume estimates at ~5-10 TB (162.7M commits, ~146B changed lines).
// On kate at 8 workers expect roughly 8-24 h wall-clock (outer bound
// ~2 days if disk-bound); the per-repo progress lines + running totals
// let you extrapolate from the first hour. Safe alongside a running
// serve: repos mid-collection are skipped this pass, and overlapping
// updates are same-value idempotent.
//
// Resumable: the marker IS the resume state — re-running skips every
// walked repo. --after-repo-id additionally skips the keyset ahead.

func rewalkWhitespaceCmd(cfgPath *string) *cobra.Command {
	var (
		workers     int
		limit       int64
		repoID      int64
		afterRepoID int64
	)
	cmd := &cobra.Command{
		Use:   "rewalk-whitespace",
		Short: "Full-history whitespace walk (Augur-parity cmt_whitespace) over the tracked fleet",
		Long: `Walks each tracked repo's full git history with git log -p, computing
Augur-parity whitespace-adjusted (added, removed, whitespace) per file
per commit, updating the commits rows, and stamping the incremental
marker the facade's per-cycle phase resumes from.

Canary first: rewalk-whitespace --limit 5. The full fleet run is a
many-hour job; progress lines include running totals for extrapolation.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
			// Run `aveloxis migrate` first — it adds whitespace_head_hash.

			if workers <= 0 {
				workers = max(runtime.NumCPU()/2, 1)
			}
			fc := collector.NewFacadeCollector(store, logger, cfg.Collection.RepoCloneDir)

			// Single-repo mode.
			if repoID > 0 {
				repo, err := store.GetRepoByID(ctx, repoID)
				if err != nil {
					return fmt.Errorf("repo %d: %w", repoID, err)
				}
				n, err := fc.RewalkWhitespace(ctx, repoID, repo.GitURL)
				if err != nil {
					return err
				}
				fmt.Printf("repo %d: %d rows updated\n", repoID, n)
				return nil
			}

			var (
				reposDone   atomic.Int64
				rowsUpdated atomic.Int64
				failed      atomic.Int64
			)
			start := time.Now()
			jobs := make(chan db.WhitespaceRewalkTarget)
			var wg sync.WaitGroup
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for t := range jobs {
						n, err := fc.RewalkWhitespace(ctx, t.RepoID, t.GitURL)
						rowsUpdated.Add(n)
						if err != nil {
							failed.Add(1)
							logger.Warn("whitespace rewalk failed", "repo_id", t.RepoID, "error", err)
							continue
						}
						done := reposDone.Add(1)
						logger.Info("whitespace rewalk repo complete",
							"repo_id", t.RepoID, "rows_updated", n)
						if done%50 == 0 {
							elapsed := time.Since(start)
							logger.Info("whitespace rewalk progress",
								"repos_done", done, "rows_updated", rowsUpdated.Load(),
								"failed", failed.Load(), "elapsed", elapsed.Round(time.Second),
								"repos_per_min", fmt.Sprintf("%.1f", float64(done)/elapsed.Minutes()))
						}
					}
				}()
			}

			after := afterRepoID
			var dispatched int64
		pageLoop:
			for {
				targets, err := store.GetReposForWhitespaceRewalk(ctx, after, 200)
				if err != nil {
					close(jobs)
					wg.Wait()
					return fmt.Errorf("page after repo_id %d: %w", after, err)
				}
				if len(targets) == 0 {
					break
				}
				for _, t := range targets {
					if limit > 0 && dispatched >= limit {
						break pageLoop
					}
					select {
					case jobs <- t:
						dispatched++
					case <-ctx.Done():
						break pageLoop
					}
					after = t.RepoID
				}
			}
			close(jobs)
			wg.Wait()

			fmt.Printf("rewalk-whitespace: %d repos walked, %d rows updated, %d failed, last repo_id %d, elapsed %s\n",
				reposDone.Load(), rowsUpdated.Load(), failed.Load(), after,
				time.Since(start).Round(time.Second))
			if err := ctx.Err(); err != nil {
				fmt.Printf("interrupted — resume with --after-repo-id %d (already-walked repos are skipped anyway)\n", after)
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&workers, "workers", 0, "parallel walkers (default NumCPU/2)")
	cmd.Flags().Int64Var(&limit, "limit", 0, "stop after N repos (canary runs)")
	cmd.Flags().Int64Var(&repoID, "repo-id", 0, "walk a single repo by id")
	cmd.Flags().Int64Var(&afterRepoID, "after-repo-id", 0, "keyset resume point")
	return cmd
}
