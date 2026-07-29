// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// aveloxis reconcile-repos (v0.27.39, summary/18 Phase 2): heals the
// stranded-repo class — non-archived repos rows with NO
// collection_queue row, invisible to the scheduler forever. Root
// cause (production-verified): GitHub renames + prelim's duplicate
// skip+dequeue; a smaller share are lost-enqueue leftovers.
//
// Per stranded repo, classified by LIVE redirect check:
//   - dead upstream (404/410)      → archive (matches prelim's sidelining)
//   - redirects to a TRACKED repo  → dataless: HealRenamedDuplicate
//     (repoint links, delete the dup row); data-bearing: consolidate
//     via the dedup-repos per-pair machinery (repoints + leaves-first
//     deletes)
//   - redirects to an UNTRACKED URL → re-enqueue (prelim renames it in
//     place on the next cycle — that is prelim's job)
//   - alive, no redirect            → re-enqueue (a lost queue row)
//
// Resumable/idempotent: healed repos drop out of the stranded set.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func reconcileReposCmd(cfgPath *string) *cobra.Command {
	var limit int
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "reconcile-repos",
		Short: "Heal non-archived repos that have no collection_queue row (v0.27.39)",
		Long: `Finds repos rows the scheduler can never see (non-archived, no queue row —
mostly rename-duplicate leftovers from prelim's skip+dequeue) and heals each
by live redirect classification: dead repos archive, dataless rename
duplicates heal onto their tracked winner, data-bearing rename duplicates
consolidate via the dedup-repos machinery, and everything else re-enqueues.

Use --dry-run to see the per-outcome plan first.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReconcileRepos(*cfgPath, limit, dryRun)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 0, "max stranded repos to process this pass (0 = all)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "classify and report without mutating")
	return cmd
}

func runReconcileRepos(cfgPath string, limit int, dryRun bool) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()
	// v0.21.5: store.Migrate(ctx) intentionally NOT called here.

	total, err := store.CountStrandedRepos(ctx)
	if err != nil {
		return fmt.Errorf("counting stranded repos: %w", err)
	}
	fmt.Printf("stranded repos (non-archived, no queue row): %d\n", total)
	if total == 0 {
		return nil
	}
	if limit <= 0 {
		limit = int(total)
	}
	stranded, err := store.ListStrandedRepos(ctx, limit)
	if err != nil {
		return fmt.Errorf("listing stranded repos: %w", err)
	}

	var dead, healedDataless, consolidated, enqueued, skipped int
	for _, sr := range stranded {
		if ctx.Err() != nil {
			break
		}
		finalURL, status, rerr := collector.ResolveRedirectTarget(ctx, sr.GitURL)
		if rerr != nil {
			logger.Warn("reconcile: redirect check failed — skipping this pass", "repo_id", sr.RepoID, "url", sr.GitURL, "error", rerr)
			skipped++
			continue
		}
		switch {
		case status == http.StatusNotFound || status == http.StatusGone:
			// Dead upstream — the outcome prelim would have applied.
			fmt.Printf("  dead:        %s (repo %d)\n", sr.GitURL, sr.RepoID)
			if !dryRun {
				if err := store.ArchiveRepo(ctx, sr.RepoID); err != nil {
					logger.Warn("reconcile: archive failed", "repo_id", sr.RepoID, "error", err)
					skipped++
					continue
				}
			}
			dead++
		case !strings.EqualFold(normalizeReconcileURL(finalURL), normalizeReconcileURL(sr.GitURL)):
			// Renamed upstream. Tracked winner → heal/consolidate;
			// untracked target → re-enqueue and let prelim rename it
			// in place (rename detection is prelim's job).
			winnerID, ferr := store.FindRepoByURL(ctx, finalURL)
			if ferr != nil {
				logger.Warn("reconcile: winner lookup failed — skipping", "repo_id", sr.RepoID, "error", ferr)
				skipped++
				continue
			}
			switch {
			case winnerID > 0 && winnerID != sr.RepoID && !sr.Collected:
				fmt.Printf("  heal (dataless dup): %s -> repo %d (dup %d)\n", sr.GitURL, winnerID, sr.RepoID)
				if !dryRun {
					healed, herr := store.HealRenamedDuplicate(ctx, sr.RepoID, winnerID)
					if herr != nil {
						logger.Warn("reconcile: heal failed — skipping", "repo_id", sr.RepoID, "error", herr)
						skipped++
						continue
					}
					if !healed {
						// v0.27.49: healed=false with no error means the
						// "dataless" dup (last_collected NULL) actually has
						// residual child rows — the heal's FK fail-safe
						// refused the delete (the 2026-07-22 apache/baremaps
						// class). The consolidation machinery handles
						// children properly; fall back to it instead of
						// stranding the row for the next run.
						logger.Info("reconcile: heal refused (residual children) — falling back to consolidation", "repo_id", sr.RepoID)
						winnerGit := finalURL
						if wr, gerr := store.GetRepoByID(ctx, winnerID); gerr == nil && wr.GitURL != "" {
							winnerGit = wr.GitURL
						}
						if derr := db.DedupRenamedRepoPair(ctx, store, winnerID, sr.RepoID, winnerGit, sr.GitURL); derr != nil {
							logger.Warn("reconcile: fallback consolidation failed — skipping", "repo_id", sr.RepoID, "error", derr)
							skipped++
							continue
						}
						consolidated++
						continue
					}
				}
				healedDataless++
			case winnerID > 0 && winnerID != sr.RepoID:
				fmt.Printf("  consolidate (data-bearing dup): %s -> repo %d (dup %d)\n", sr.GitURL, winnerID, sr.RepoID)
				if !dryRun {
					var winnerGit string
					if wr, gerr := store.GetRepoByID(ctx, winnerID); gerr == nil {
						winnerGit = wr.GitURL
					}
					if derr := db.DedupRenamedRepoPair(ctx, store, winnerID, sr.RepoID, winnerGit, sr.GitURL); derr != nil {
						logger.Warn("reconcile: consolidation failed — skipping", "repo_id", sr.RepoID, "error", derr)
						skipped++
						continue
					}
				}
				consolidated++
			default:
				fmt.Printf("  enqueue (renamed, target untracked): %s (repo %d)\n", sr.GitURL, sr.RepoID)
				if !dryRun {
					if err := store.EnqueueRepo(ctx, sr.RepoID, 100); err != nil {
						logger.Warn("reconcile: enqueue failed", "repo_id", sr.RepoID, "error", err)
						skipped++
						continue
					}
				}
				enqueued++
			}
		default:
			// Alive at its own URL — a lost queue row. Restore
			// scheduler visibility.
			fmt.Printf("  enqueue (lost queue row): %s (repo %d)\n", sr.GitURL, sr.RepoID)
			if !dryRun {
				if err := store.EnqueueRepo(ctx, sr.RepoID, 100); err != nil {
					logger.Warn("reconcile: enqueue failed", "repo_id", sr.RepoID, "error", err)
					skipped++
					continue
				}
			}
			enqueued++
		}
	}

	mode := ""
	if dryRun {
		mode = " (dry run — nothing written)"
	}
	fmt.Printf("reconcile-repos%s: dead=%d healed_dataless=%d consolidated=%d enqueued=%d skipped=%d of %d stranded\n",
		mode, dead, healedDataless, consolidated, enqueued, skipped, total)
	if skipped > 0 {
		fmt.Println("re-run to retry skipped repos")
	}
	return nil
}

// normalizeReconcileURL trims the pieces that don't affect identity
// for the rename comparison (matches prelim's normalization intent).
func normalizeReconcileURL(u string) string {
	u = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(u), "/"), ".git")
	return strings.ToLower(u)
}
