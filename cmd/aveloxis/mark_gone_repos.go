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
//   - DEFINITIVE 404/410/451 → SidelineRepo (repo_archived + repo_gone_at +
//     the gone episode in one statement, the queue row removed in the same
//     transaction — 0.29.140, Copilot PR #252 sixth review: a stamp alone
//     left a concurrently resurrected repository gone AND queued).
//   - DEFINITIVE 200 on an already-gone-stamped repo → ResurrectRepo
//     (clear + re-enqueue in one transaction, v0.28.6), so collection
//     resumes if the org re-publicizes.
//   - DEFINITIVE 200 on a repo archived WITHOUT a stamp → ResurrectArchivedSideline
//     (0.29.140, operator decision 2026-11-07): the pre-0.29.140
//     reconcile-repos dead arm set the archived flag alone on a 404, and a
//     forge-archived repository keeps its queue row, so this is that
//     arm's sideline coming back. Any other 2xx (a queueless row that is
//     neither — a data-bearing duplicate prelim kept) is alive and left
//     as it is.
//   - Everything else (transport errors, rate limits, 5xx) SKIPS the
//     repo (SR-16: only definitive answers decide) — a rerun retries.
//
// Re-runnable on operator cadence; the probe is prelim's own
// redirect-following collector.ResolveRedirectTarget (SR-17: one probe,
// three consumers with reconcile-repos).
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
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
URL returns a definitive 404/410/451 — privatized, deleted or legally blocked upstream) and
probes each URL against the forge:

  404/410/451  -> stamp repos.repo_gone_at (the GUI then shows the
              "no longer publicly available" notice over the data we hold)
  200 on a gone-stamped repo -> clear the stamp and re-enqueue
              (collection resumes — the org came back)
  200 on a repo archived without a stamp -> restore the archived flag
              from the forge's latest snapshot and re-enqueue (the
              pre-0.29.140 reconcile-repos sideline coming back)
  200 on any other candidate -> counted alive, left as it is (re-add
              it with add-repo to resume collection)
  anything else -> skipped; re-run later (only definitive answers decide)

Idempotent and re-runnable. Dataless stranded rows are not candidates
(nothing to display either way — reconcile-repos territory).

Since v0.29.7 aveloxis serve re-probes every gone repo on its own every
collection.gone_repo_recheck_days (default 28); this command remains
the immediate, whole-cohort form and resets that cadence clock.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// A signal cancels the in-flight probe or statement and ends
			// the walk (worklist §4); the summary below says what landed,
			// and a rerun walks the whole cohort again (every verdict is
			// idempotent; there is no resume marker).
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
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

	// The tally IS the report's struct (batch 7b review round 8): a
	// positional literal built from seven locals at the interrupt was one
	// swapped pair from logging a counter under another's key.
	var tally markGoneCounts
	// v0.29.7: a manual run is a verification too. On every gone-stamped
	// row it probes — definitive-gone OR not definitive — stamp the
	// check so the scheduler's recheck ticker (the same rule, see
	// scheduler/gone_recheck.go) starts its cadence from now: one clock,
	// one stamping policy, every consumer. Never on --dry-run. A failed
	// stamp has its OWN counter (review round 2): the verdict counters
	// partition the candidates exactly once each, and a stamp failure
	// is a second event on a row already counted — folding it into
	// `skipped` double-counted the row.
	stampChecked := func(c db.GoneProbeCandidate) {
		if dryRun || !c.GoneStamped || ctx.Err() != nil {
			return // a stamp on a cancelled ctx fails and is not a stamp failure
		}
		if err := store.MarkRepoGoneChecked(ctx, c.RepoID); err != nil {
			if ctx.Err() != nil {
				return // the cancel landed mid-statement: the loop top, or the post-loop check, reports it
			}
			logger.Warn("failed to stamp gone check — rerun retries", "repo_id", c.RepoID, "error", err)
			tally.stampFailed++
		}
	}
	// Interrupted: what landed stays (every stamp is its own statement),
	// and a rerun walks the whole cohort again. Reached at the loop top,
	// after the loop (a cancel on the LAST candidate's check stamp has no
	// next loop top — round 4), from a probe the cancel cut short, and from
	// every store write it cut short
	// (batch 7b review rounds 1 and 2: a `context canceled` from the probe
	// or from an UPDATE fell into that arm's failure WARN and counter — one
	// row counted as skipped, a second WARN for the stamp on the dead ctx).
	interrupted := func() error {
		return markGoneInterruptedReport(logger, tally, len(cands), dryRun, ctx.Err())
	}
	for _, c := range cands {
		if ctx.Err() != nil {
			return interrupted()
		}
		// The SAME redirect-following probe prelim and reconcile-repos
		// use (SR-17: one probe, all consumers agree on what "gone"
		// means).
		_, status, perr := collector.ResolveRedirectTarget(ctx, c.GitURL)
		if perr != nil && ctx.Err() != nil {
			return interrupted() // the cancel, not the probe, ended this row
		}
		if errors.Is(perr, platform.ErrRedirectTargetUserinfo) {
			// The stored URL is clean; the FORGE's redirect target carries
			// credentials (round 7). Not the operator's row to correct —
			// skipped like an indeterminate probe, check stamped.
			logger.Warn("redirect target carries credentials — not followed; the stored URL is clean, skipping",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "error", perr)
			tally.skipped++
			stampChecked(c)
			continue
		}
		if errors.Is(perr, platform.ErrURLUserinfo) {
			// The probe refuses a URL carrying credentials (v0.29.57). Not a
			// retryable skip: a rerun cannot succeed until repo_git is
			// corrected, so it has its own counter and fails the run.
			logger.Error("repo URL carries credentials — not probed; correct repo_git",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "error", perr)
			tally.refused++
			stampChecked(c)
			continue
		}
		if errors.Is(perr, platform.ErrPrivateAddress) {
			// The outbound-address rule (0.29.132): skipped like an
			// indeterminate probe, under the phrase the operator greps for.
			logger.Warn("repository host is a private address — not probed; list it in collection.private_network_hosts, skipping",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "error", perr)
			tally.skipped++
			stampChecked(c)
			continue
		}
		if perr != nil {
			// SR-16: a transport failure is not "no" — the gone state is
			// untouched; a rerun retries. The check is still stamped on
			// a gone row (the ticker's rule).
			logger.Warn("probe failed — skipping (rerun retries)",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "error", perr)
			tally.skipped++
			stampChecked(c)
			continue
		}
		switch {
		case platform.IsRepoGoneStatus(status): // 404, 410, 451 — one rule (v0.29.58)
			if c.GoneStamped {
				tally.alreadyGone++
				stampChecked(c)
				continue
			}
			if dryRun {
				logger.Info("would stamp gone", "repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "status", status)
				tally.stamped++
				continue
			}
			if err := store.SidelineRepo(ctx, c.RepoID, status, db.GoneSourceMarkGoneRepos); err != nil {
				if ctx.Err() != nil {
					return interrupted()
				}
				logger.Warn("failed to stamp gone", "repo_id", c.RepoID, "error", err)
				tally.skipped++
				continue
			}
			tally.stamped++
		case status >= 200 && status < 300:
			switch onReachable(c) {
			case reachableAlive:
				tally.alive++
				continue
			case reachableLiftArchived:
				if dryRun {
					logger.Info("would restore the archived flag + re-enqueue (the old reconcile sideline)", "repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL))
					tally.liftedArchived++
					continue
				}
				lifted, err := store.ResurrectArchivedSideline(ctx, c.RepoID, 10, db.GoneSourceMarkGoneRepos)
				if err != nil {
					if ctx.Err() != nil {
						return interrupted()
					}
					logger.Warn("failed to lift the archived sideline — nothing committed, rerun retries", "repo_id", c.RepoID, "error", err)
					tally.skipped++
					continue
				}
				if !lifted {
					// Re-queued or stamped since the candidate read: not the
					// residue any more, and nothing was written.
					tally.alive++
					continue
				}
				logger.Info("archived sideline lifted — archived flag restored from the forge's snapshot, re-enqueued", "repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL))
				tally.liftedArchived++
				continue
			}
			// Resurrection: the forge serves the repo again.
			if dryRun {
				logger.Info("would clear gone + re-enqueue", "repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL))
				tally.cleared++
				continue
			}
			// v0.28.6 (Copilot round): clear + re-enqueue are ONE
			// store transaction — no two-statement ordering leaves a
			// recoverable failure here (clear-then-failed-enqueue
			// stranded the repo; enqueue-then-failed-clear dropped it
			// out of the queueless candidate set). On failure nothing
			// committed: the repo stays gone-stamped and queueless,
			// and a plain rerun retries.
			if err := store.ResurrectRepo(ctx, c.RepoID, 10, db.GoneSourceMarkGoneRepos); err != nil {
				if ctx.Err() != nil {
					return interrupted()
				}
				// Counted as skipped ONLY (not alive) so the
				// completion line's alive_unstamped stays accurate.
				logger.Warn("failed to resurrect repo — nothing committed, rerun retries",
					"repo_id", c.RepoID, "error", err)
				tally.skipped++
				continue
			}
			tally.cleared++
		default:
			// 3xx that didn't resolve, 403/429/5xx — indeterminate.
			logger.Warn("indeterminate probe status — skipping (rerun retries)",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "status", status)
			tally.skipped++
			stampChecked(c)
		}
	}
	if ctx.Err() != nil {
		return interrupted()
	}

	logger.Info("mark-gone-repos complete",
		"candidates", len(cands), "stamped_gone", tally.stamped, "already_gone", tally.alreadyGone,
		"resurrected", tally.cleared, "lifted_archived", tally.liftedArchived, "alive_unstamped", tally.alive, "skipped", tally.skipped,
		"refused", tally.refused, "check_stamp_failed", tally.stampFailed, "dry_run", dryRun)
	if tally.skipped > 0 || tally.stampFailed > 0 {
		logger.Info("some candidates were skipped on indeterminate probes or could not be stamped — re-run to retry them")
	}
	if tally.refused > 0 {
		return fmt.Errorf("%d candidate(s) have a repo_git carrying credentials — correct them, then rerun", tally.refused)
	}
	return nil
}

// markGoneCounts is the walk's tally.
type markGoneCounts struct {
	stamped, cleared, alreadyGone, alive, skipped, refused, stampFailed, liftedArchived int
}

// reachableAction is what a definitive 2xx means for a candidate.
type reachableAction int

const (
	// reachableAlive: a queueless row that is neither gone-stamped nor
	// archived (a data-bearing rename duplicate prelim dequeued on
	// purpose) — counted, left as it is.
	reachableAlive reachableAction = iota
	// reachableResurrect: a gone-stamped row — ResurrectRepo.
	reachableResurrect
	// reachableLiftArchived: archived without a stamp, the pre-0.29.140
	// reconcile-repos dead arm's sideline — ResurrectArchivedSideline.
	reachableLiftArchived
)

func (a reachableAction) String() string {
	switch a {
	case reachableResurrect:
		return "resurrect"
	case reachableLiftArchived:
		return "lift-archived"
	}
	return "alive"
}

// onReachable decides a definitive 2xx by the candidate's class (operator
// decision 2026-11-07): the stamp first, then the old arm's archived flag.
// The store re-checks the class in the statement; this picks the call.
func onReachable(c db.GoneProbeCandidate) reachableAction {
	switch {
	case c.GoneStamped:
		return reachableResurrect
	case c.Archived:
		return reachableLiftArchived
	}
	return reachableAlive
}

// markGoneInterruptedReport logs the interruption with every counter and
// returns the non-zero exit wrapping the cancellation (batch 7b review
// round 7: the same pure-function shape as reconcile-repos', so the unit
// test holds the exit status a source pin let become `return nil`).
func markGoneInterruptedReport(logger *slog.Logger, c markGoneCounts, total int, dryRun bool, cause error) error {
	logger.Warn("mark-gone-repos interrupted — a rerun walks the whole cohort again",
		"probed", c.stamped+c.cleared+c.alreadyGone+c.alive+c.skipped+c.refused+c.liftedArchived, "of", total,
		"stamped", c.stamped, "cleared", c.cleared, "already_gone", c.alreadyGone, "alive", c.alive, "skipped", c.skipped, "refused", c.refused, "stamp_failed", c.stampFailed, "lifted_archived", c.liftedArchived, "dry_run", dryRun)
	return fmt.Errorf("mark-gone-repos interrupted: %w", cause)
}
