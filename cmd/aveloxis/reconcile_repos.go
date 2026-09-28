// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// aveloxis reconcile-repos (v0.27.39, summary/18 Phase 2): heals the
// stranded-repo class — non-archived repos rows with NO
// collection_queue row, invisible to the scheduler forever. Root
// cause (production-verified): GitHub renames + prelim's duplicate
// skip+dequeue; a smaller share are lost-enqueue leftovers.
//
// Per stranded repo, classified by LIVE redirect check:
//   - dead upstream (404/410/451)      → archive (matches prelim's sidelining)
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
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

	// The tally IS the report's struct (batch 7b review round 8): a
	// positional literal built from six locals at the interrupt was one
	// swapped pair from printing a counter under another's label.
	var c reconcileCounts
	// v0.28.18: consolidation arms refused by the email_message index
	// precondition. The loop keeps going (dead / re-enqueue arms need no
	// index) but the run exits nonzero so a script cannot read a fully
	// refused reconcile as success.
	// Interrupted (batch 7b review round 5, the mark-gone shape): what landed
	// stays — every store write is its own statement or transaction — and a
	// rerun walks the whole cohort again (idempotent; no resume marker).
	// Reached at the loop top, from a probe the cancel cut short, from
	// every store write it cut short, and after the loop. Through v0.29.67 a
	// cancelled walk printed the completion line and exited 0.
	mode := ""
	if dryRun {
		mode = " (dry run — nothing written)"
	}
	interrupted := func() error {
		return reconcileInterruptedReport(os.Stdout, mode, c, total, ctx.Err())
	}
	for _, sr := range stranded {
		if ctx.Err() != nil {
			return interrupted()
		}
		finalURL, status, rerr := collector.ResolveRedirectTarget(ctx, sr.GitURL)
		if rerr != nil && ctx.Err() != nil {
			return interrupted() // the cancel, not the probe, ended this row
		}
		if errors.Is(rerr, platform.ErrRedirectTargetUserinfo) {
			// The forge's redirect target, not the stored URL (round 7):
			// skipped, not refused.
			logger.Warn("reconcile: redirect target carries credentials — not followed; the stored URL is clean, skipping",
				"repo_id", sr.RepoID, "url", platform.RedactURLUserinfo(sr.GitURL), "error", rerr)
			c.skipped++
			continue
		}
		if errors.Is(rerr, platform.ErrURLUserinfo) {
			// Not a retryable skip: the probe refuses a URL carrying
			// credentials until repo_git is corrected (v0.29.57).
			logger.Error("reconcile: repo URL carries credentials — not probed; correct repo_git",
				"repo_id", sr.RepoID, "url", platform.RedactURLUserinfo(sr.GitURL), "error", rerr)
			c.refused++
			continue
		}
		if rerr != nil {
			logger.Warn("reconcile: redirect check failed — skipping this pass", "repo_id", sr.RepoID, "url", platform.RedactURLUserinfo(sr.GitURL), "error", rerr)
			c.skipped++
			continue
		}
		switch {
		case platform.IsRepoGoneStatus(status): // 404, 410, 451 — one rule (v0.29.58)
			// Dead upstream — the outcome prelim would have applied.
			fmt.Printf("  dead:        %s (repo %d)\n", sr.GitURL, sr.RepoID)
			if !dryRun {
				if err := store.ArchiveRepo(ctx, sr.RepoID); err != nil {
					if ctx.Err() != nil {
						return interrupted()
					}
					logger.Warn("reconcile: archive failed", "repo_id", sr.RepoID, "error", err)
					c.skipped++
					continue
				}
			}
			c.dead++
		case !strings.EqualFold(normalizeReconcileURL(finalURL), normalizeReconcileURL(sr.GitURL)):
			// Renamed upstream. Tracked winner → heal/consolidate;
			// untracked target → re-enqueue and let prelim rename it
			// in place (rename detection is prelim's job).
			winnerID, ferr := store.FindRepoByURL(ctx, finalURL)
			if ferr != nil {
				if ctx.Err() != nil {
					return interrupted()
				}
				logger.Warn("reconcile: winner lookup failed — skipping", "repo_id", sr.RepoID, "error", ferr)
				c.skipped++
				continue
			}
			switch {
			case winnerID > 0 && winnerID != sr.RepoID && !sr.Collected:
				fmt.Printf("  heal (dataless dup): %s -> repo %d (dup %d)\n", sr.GitURL, winnerID, sr.RepoID)
				if !dryRun {
					healed, herr := store.HealRenamedDuplicate(ctx, sr.RepoID, winnerID)
					if herr != nil {
						if ctx.Err() != nil {
							return interrupted()
						}
						logger.Warn("reconcile: heal failed — skipping", "repo_id", sr.RepoID, "error", herr)
						if errors.Is(herr, db.ErrEmailMessageIndexesNotReady) {
							c.preconditionUnmet++
						}
						c.skipped++
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
							if ctx.Err() != nil {
								return interrupted()
							}
							logger.Warn("reconcile: fallback consolidation failed — skipping", "repo_id", sr.RepoID, "error", derr)
							if errors.Is(derr, db.ErrEmailMessageIndexesNotReady) {
								c.preconditionUnmet++
							}
							c.skipped++
							continue
						}
						c.consolidated++
						continue
					}
				}
				c.healedDataless++
			case winnerID > 0 && winnerID != sr.RepoID:
				fmt.Printf("  consolidate (data-bearing dup): %s -> repo %d (dup %d)\n", sr.GitURL, winnerID, sr.RepoID)
				if !dryRun {
					winnerGit := finalURL
					if wr, gerr := store.GetRepoByID(ctx, winnerID); gerr == nil && wr.GitURL != "" {
						winnerGit = wr.GitURL
					}
					if derr := db.DedupRenamedRepoPair(ctx, store, winnerID, sr.RepoID, winnerGit, sr.GitURL); derr != nil {
						if ctx.Err() != nil {
							return interrupted()
						}
						logger.Warn("reconcile: consolidation failed — skipping", "repo_id", sr.RepoID, "error", derr)
						if errors.Is(derr, db.ErrEmailMessageIndexesNotReady) {
							c.preconditionUnmet++
						}
						c.skipped++
						continue
					}
				}
				c.consolidated++
			default:
				fmt.Printf("  enqueue (renamed, target untracked): %s (repo %d)\n", sr.GitURL, sr.RepoID)
				if !dryRun {
					if err := store.EnqueueRepo(ctx, sr.RepoID, 100); err != nil {
						if ctx.Err() != nil {
							return interrupted()
						}
						logger.Warn("reconcile: enqueue failed", "repo_id", sr.RepoID, "error", err)
						c.skipped++
						continue
					}
				}
				c.enqueued++
			}
		default:
			// Alive at its own URL — a lost queue row. Restore
			// scheduler visibility.
			fmt.Printf("  enqueue (lost queue row): %s (repo %d)\n", sr.GitURL, sr.RepoID)
			if !dryRun {
				if err := store.EnqueueRepo(ctx, sr.RepoID, 100); err != nil {
					if ctx.Err() != nil {
						return interrupted()
					}
					logger.Warn("reconcile: enqueue failed", "repo_id", sr.RepoID, "error", err)
					c.skipped++
					continue
				}
			}
			c.enqueued++
		}
	}
	if ctx.Err() != nil {
		return interrupted()
	}

	fmt.Printf("reconcile-repos%s: dead=%d healed_dataless=%d consolidated=%d enqueued=%d skipped=%d refused=%d of %d stranded\n",
		mode, c.dead, c.healedDataless, c.consolidated, c.enqueued, c.skipped, c.refused, total)
	if c.skipped > 0 {
		fmt.Println("re-run to retry skipped repos")
	}
	if c.refused > 0 {
		fmt.Printf("%d repos have a repo_git carrying credentials — correct them, then rerun\n", c.refused)
	}
	if c.preconditionUnmet > 0 {
		// db.DeployStepsAdvice, not a literal migrate: on a release whose
		// checklist migrates without --skip-views (v0.29.57), the literal
		// stamped the binary around its view definitions (L10 round 3).
		logger.Error("precondition unmet — consolidations refused: run "+db.DeployStepsAdvice+" on this binary first, then re-run",
			"repos_refused", c.preconditionUnmet)
		return fmt.Errorf("%d stranded repos refused for the email_message index precondition — run %s first", c.preconditionUnmet, db.DeployStepsAdvice)
	}
	if c.refused > 0 {
		return fmt.Errorf("%d stranded repos have a repo_git carrying credentials — correct them, then rerun", c.refused)
	}
	return nil
}

// normalizeReconcileURL trims the pieces that don't affect identity
// for the rename comparison (matches prelim's normalization intent).
func normalizeReconcileURL(u string) string {
	return strings.ToLower(model.NormalizeRepoGitURL(u)) // the stored spelling's suffix rule (follow-up 8)
}

// reconcileCounts is the walk's tally, in the order the lines print it.
type reconcileCounts struct {
	dead, healedDataless, consolidated, enqueued, skipped, refused, preconditionUnmet int
}

// reconcileInterruptedReport prints the interruption line — the mode
// marker and every counter, so the operator knows what landed — and
// returns the non-zero exit wrapping the cancellation (batch 7b review
// round 7: the closure's `return fmt.Errorf(...)` could become `return nil`
// with every pin green; a pure function is what the unit test drives).
func reconcileInterruptedReport(w io.Writer, mode string, c reconcileCounts, total int64, cause error) error {
	fmt.Fprintf(w, "reconcile-repos%s interrupted: dead=%d healed_dataless=%d consolidated=%d enqueued=%d skipped=%d refused=%d precondition_unmet=%d of %d stranded — a rerun walks the whole cohort again\n",
		mode, c.dead, c.healedDataless, c.consolidated, c.enqueued, c.skipped, c.refused, c.preconditionUnmet, total)
	return fmt.Errorf("reconcile-repos interrupted: %w", cause)
}
