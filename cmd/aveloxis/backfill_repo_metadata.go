// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
	"github.com/spf13/cobra"
)

// backfill-repo-metadata (v0.27.79) — operator-driven fleet sweep that
// refreshes repos.repo_description / primary_language / languages /
// repo_archived / forked_from from the forge APIs, one FetchRepoInfo
// per repo, through the SAME UpdateRepoMetadata write Phase 0 uses.
//
// Why it exists: the 2026-08-02 production audit found forked_from on
// 0 of 94,104 rows (nothing captured fork status before v0.27.78).
// The forward path heals each repo on its next collection cycle
// (~6-21 days); this command front-loads the whole fleet in about an
// hour so the public launch doesn't wait. Cost: 1 GraphQL point per
// GitHub repo (~94K points ≈ a quarter of ONE hour of the 73-key
// pool's budget); GitLab repos cost a few REST calls each.
//
// Resumable via --after-repo-id (candidates page by repo_id keyset —
// the v0.26.6 lesson; the summary line prints the last id processed).
// Re-running from zero is safe: every write is the forge's current
// truth, idempotent by construction.

func backfillRepoMetadataCmd(cfgPath *string) *cobra.Command {
	var (
		limit       int
		afterRepoID int64
		workers     int
		dryRun      bool
	)
	cmd := &cobra.Command{
		Use:   "backfill-repo-metadata",
		Short: "Refresh description/language/archived/forked-from for the whole fleet from the forge APIs",
		Long: `Sweeps every GitHub/GitLab repo in the catalog (generic-git repos have
no API and are skipped), fetching current metadata and writing it via
the same path the collector's Phase 0 uses. Primarily a launch
accelerator for repos.forked_from (v0.27.78 fork capture) — the
per-cycle forward path makes this command unnecessary in steady state.

Resumable: on interruption, re-run with --after-repo-id <last logged id>.`,
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

			if dryRun {
				n, err := countMetadataRefreshCandidates(ctx, store, afterRepoID)
				if err != nil {
					return err
				}
				fmt.Printf("dry run: %d forge-backed repos would be refreshed (after repo_id %d)\n", n, afterRepoID)
				return nil
			}

			ghKeys, glKeys, err := loadKeys(ctx, cfg, store, false, logger)
			if err != nil {
				return fmt.Errorf("loading API keys: %w", err)
			}
			ghClient := github.New(cfg.GitHub.BaseURL, ghKeys, logger)
			glClient := gitlab.New(cfg.GitLab.BaseURL, glKeys, logger)

			return runBackfillRepoMetadata(ctx, store, ghClient, glClient, logger, limit, afterRepoID, workers)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 0, "stop after N repos (0 = the whole fleet); canary with --limit 100")
	cmd.Flags().Int64Var(&afterRepoID, "after-repo-id", 0, "resume after this repo_id (keyset cursor; printed in the summary)")
	cmd.Flags().IntVar(&workers, "workers", 8, "concurrent fetchers (breadth_fetch_concurrency precedent)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "count candidates and exit without fetching or writing")
	return cmd
}

func countMetadataRefreshCandidates(ctx context.Context, store *db.PostgresStore, afterRepoID int64) (int, error) {
	var n int
	err := store.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repos
		WHERE repo_id > $1 AND platform_id IN (1, 2)
		  AND COALESCE(repo_owner, '') != '' AND COALESCE(repo_name, '') != ''`,
		afterRepoID).Scan(&n)
	return n, err
}

func runBackfillRepoMetadata(ctx context.Context, store *db.PostgresStore, ghClient, glClient platform.Client,
	logger *slog.Logger, limit int, afterRepoID int64, workers int) error {
	if workers < 1 {
		workers = 1
	}
	var processed, forks, skipped, failed atomic.Int64
	var lastID atomic.Int64
	lastID.Store(afterRepoID)

	jobs := make(chan db.RepoMetadataBackfillTarget)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				client := ghClient
				if t.PlatformID == int16(model.PlatformGitLab) {
					client = glClient
				}
				info, err := client.FetchRepoInfo(ctx, t.Owner, t.Name)
				if err != nil {
					if platform.ClassifyError(err) == platform.ClassSkip {
						// 404/410/403 — renamed, deleted, private. The
						// normal collection cycle (prelim) owns those
						// states; nothing to write here.
						skipped.Add(1)
						logger.Debug("metadata refresh skip", "repo_id", t.RepoID, "error", err)
					} else {
						failed.Add(1)
						logger.Warn("metadata refresh failed", "repo_id", t.RepoID,
							"owner", t.Owner, "repo", t.Name, "error", err)
					}
					continue
				}
				forkedFrom := info.ForkedFrom()
				if err := store.UpdateRepoMetadata(ctx, t.RepoID, info.Description, info.PrimaryLanguage,
					info.Languages, info.Status == "Archived", forkedFrom, info.PlatformRepoID); err != nil {
					failed.Add(1)
					logger.Warn("metadata refresh write failed", "repo_id", t.RepoID, "error", err)
					continue
				}
				if forkedFrom != "" {
					forks.Add(1)
				}
				if n := processed.Add(1); n%500 == 0 {
					logger.Info("metadata refresh progress", "processed", n, "forks", forks.Load(),
						"skipped", skipped.Load(), "failed", failed.Load(), "last_repo_id", lastID.Load())
				}
			}
		}()
	}

	cursor := afterRepoID
	remaining := limit
	total := 0
feed:
	for {
		pageSize := 500
		if limit > 0 && remaining < pageSize {
			pageSize = remaining
		}
		if limit > 0 && remaining <= 0 {
			break
		}
		targets, err := store.GetReposForMetadataRefresh(ctx, cursor, pageSize)
		if err != nil {
			close(jobs)
			wg.Wait()
			return fmt.Errorf("paging candidates after repo_id %d: %w", cursor, err)
		}
		if len(targets) == 0 {
			break
		}
		for _, t := range targets {
			select {
			case jobs <- t:
				cursor = t.RepoID
				lastID.Store(t.RepoID)
				total++
			case <-ctx.Done():
				break feed
			}
		}
		remaining -= len(targets)
	}
	close(jobs)
	wg.Wait()

	logger.Info("metadata refresh complete",
		"dispatched", total, "processed", processed.Load(), "forks", forks.Load(),
		"skipped", skipped.Load(), "failed", failed.Load(), "last_repo_id", lastID.Load(),
		"resume_hint", fmt.Sprintf("--after-repo-id %d", lastID.Load()))
	if ctx.Err() != nil {
		return fmt.Errorf("interrupted — resume with --after-repo-id %d", lastID.Load())
	}
	return nil
}
