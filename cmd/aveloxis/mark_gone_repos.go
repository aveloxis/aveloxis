// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// mark_gone_repos.go — v0.28.1 (A6): the one-shot healer for the
// historical gone cohort (the department-of-veterans-affairs
// incident: an org privatized every repo AFTER collection; prelim's
// 404 sideline archived + dequeued them, and the GUI misread the
// queueless state as "queued for first collection" over real data).
//
// The command probes every QUEUELESS candidate against the forge —
// the forge is the authority; DB shape alone cannot distinguish
// "gone upstream" from "operator removed from tracking" — and is
// BIDIRECTIONAL + idempotent:
//
//   - DEFINITIVE 404/410 → MarkRepoGone (repo_archived + repo_gone_at
//     in one statement).
//   - DEFINITIVE 200 on an already-gone-stamped repo → ClearRepoGone
//     + EnqueueRepo, so collection resumes if the org re-publicizes.
//   - Everything else (transport errors, rate limits, 5xx) SKIPS the
//     repo (SR-16: only definitive answers decide) — a rerun retries.
//
// Re-runnable on operator cadence; the probe uses prelim's own
// redirect-following ProbeRepoStatus (SR-17: one probe, two
// consumers).
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func markGoneReposCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun bool
		limit  int
	)

	cmd := &cobra.Command{
		Use:   "mark-gone-repos",
		Short: "Probe queueless repos against the forge and stamp/clear the gone state",
		Long: `Finds repos with no collection_queue row (prelim dequeues repos whose
URL returns a definitive 404/410 — privatized or deleted upstream) and
probes each URL against the forge:

  404/410  -> stamp repos.repo_gone_at (the GUI then shows the
              "no longer publicly available" notice over the data we hold)
  200 on a gone-stamped repo -> clear the stamp and re-enqueue
              (collection resumes — the org came back)
  anything else -> skipped; re-run later (only definitive answers decide)

Idempotent and re-runnable. Dataless stranded rows are not candidates
(nothing to display either way — reconcile-repos territory).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, logger)

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-mark-gone"), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			return runMarkGoneRepos(ctx, store, logger, dryRun, limit)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "list what each probe would do without writing anything")
	cmd.Flags().IntVar(&limit, "limit", 0, "probe at most N candidates (0 = all)")
	return cmd
}

func runMarkGoneRepos(ctx context.Context, store *db.PostgresStore, logger *slog.Logger, dryRun bool, limit int) error {
	cands, err := store.GetGoneProbeCandidates(ctx, limit)
	if err != nil {
		return fmt.Errorf("loading gone-probe candidates: %w", err)
	}
	logger.Info("mark-gone-repos starting", "candidates", len(cands), "dry_run", dryRun)

	var stamped, cleared, alreadyGone, alive, skipped int
	for _, c := range cands {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The SAME redirect-following probe prelim and reconcile-repos
		// use (SR-17: one probe, all consumers agree on what "gone"
		// means).
		_, status, perr := collector.ResolveRedirectTarget(ctx, c.GitURL)
		if perr != nil {
			// SR-16: a transport failure is not "no" — skip, retry on
			// the next run.
			logger.Warn("probe failed — skipping (rerun retries)",
				"repo_id", c.RepoID, "url", c.GitURL, "error", perr)
			skipped++
			continue
		}
		switch {
		case status == http.StatusNotFound || status == http.StatusGone:
			if c.GoneStamped {
				alreadyGone++
				continue
			}
			if dryRun {
				logger.Info("would stamp gone", "repo_id", c.RepoID, "url", c.GitURL, "status", status)
				stamped++
				continue
			}
			if err := store.MarkRepoGone(ctx, c.RepoID); err != nil {
				logger.Warn("failed to stamp gone", "repo_id", c.RepoID, "error", err)
				skipped++
				continue
			}
			stamped++
		case status >= 200 && status < 300:
			if !c.GoneStamped {
				alive++
				continue
			}
			// Resurrection: the forge serves the repo again.
			if dryRun {
				logger.Info("would clear gone + re-enqueue", "repo_id", c.RepoID, "url", c.GitURL)
				cleared++
				continue
			}
			if err := store.ClearRepoGone(ctx, c.RepoID); err != nil {
				// Still gone-stamped: count as skipped ONLY (not
				// alive) so the completion line's alive_unstamped
				// stays accurate (v0.28.4 review-lens finding).
				logger.Warn("failed to clear gone stamp", "repo_id", c.RepoID, "error", err)
				skipped++
				continue
			}
			if err := store.EnqueueRepo(ctx, c.RepoID, 10); err != nil {
				logger.Warn("cleared gone but failed to re-enqueue — add-repo can requeue manually",
					"repo_id", c.RepoID, "error", err)
			}
			cleared++
		default:
			// 3xx that didn't resolve, 403/429/5xx — indeterminate.
			logger.Warn("indeterminate probe status — skipping (rerun retries)",
				"repo_id", c.RepoID, "url", c.GitURL, "status", status)
			skipped++
		}
	}

	logger.Info("mark-gone-repos complete",
		"candidates", len(cands), "stamped_gone", stamped, "already_gone", alreadyGone,
		"resurrected", cleared, "alive_unstamped", alive, "skipped", skipped,
		"dry_run", dryRun)
	if skipped > 0 {
		logger.Info("some candidates were skipped on indeterminate probes — re-run to retry them")
	}
	return nil
}
