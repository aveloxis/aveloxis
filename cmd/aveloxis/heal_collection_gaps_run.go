// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/spf13/cobra"
)

// gapHealStore is what one heal-collection-gaps run needs from the store —
// an interface so the run's interrupt behavior is tested by driving it
// (v0.29.65 review round 3: ten planted regressions of the round-2 fixes
// all passed the source pins that stood in for this test).
type gapHealStore interface {
	LockReposForDrain(ctx context.Context, repoIDs []int64, workerID string) ([]int64, error)
	ReleaseDrainLock(ctx context.Context, repoID int64, workerID string) error
	ReleaseDrainLocks(ctx context.Context, workerID string) (int64, error)
	RefreshQueueGatheredCounts(ctx context.Context, repoID int64) error
	GetGapHealCandidates(ctx context.Context, afterRepoID int64, limit int, all bool) ([]db.GapHealCandidate, error)
	GetRepoByID(ctx context.Context, repoID int64) (*model.Repo, error)
	GetRepoMetaCounts(ctx context.Context, repoID int64) (issues, prs int64, err error)
	GetQueueStatus(ctx context.Context, repoID int64) (db.QueueRowStatus, bool, error)
	StartDrainHeartbeat(ctx context.Context, logger *slog.Logger, workerID string) (stop func())
}

// gapHealFlags are heal-collection-gaps' command-line flags. Parsed by
// bindGapHealFlags and turned into a run by newGapHealRun, so a test can
// drive a real flag string into the run (review round 13: the flag →
// field step was untested, and dropping --dry-run there would heal the
// fleet).
type gapHealFlags struct {
	dryRun      bool
	limit       int
	workers     int
	repoID      int64
	afterRepoID int64
	sweepAll    bool
}

func bindGapHealFlags(cmd *cobra.Command, f *gapHealFlags) {
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "list what would be healed without healing (with --repo-id: that repo and whether a real run could lock it)")
	cmd.Flags().IntVar(&f.limit, "limit", 0, "cap the number of candidates visited this run (0 = all)")
	cmd.Flags().IntVar(&f.workers, "workers", 4, "concurrent per-repo heals")
	cmd.Flags().Int64Var(&f.repoID, "repo-id", 0, "heal exactly one repo by id (bypasses candidate selection)")
	cmd.Flags().Int64Var(&f.afterRepoID, "after-repo-id", 0, "keyset resume point")
	cmd.Flags().BoolVar(&f.sweepAll, "all", false, "sweep every collected repo, not just count-gap candidates (completeness mode; not recommended for routine use)")
}

// threshold is the gap-fill threshold the flags select. Round-23: --all
// and --repo-id run in FORCE-LIST mode — threshold 0 still requires
// metadata > gathered, which cannot see the count-netting case (retained
// deleted rows offsetting missing ones); the completeness modes make the
// listing itself the truth source.
func (f gapHealFlags) threshold() float64 {
	if f.sweepAll || f.repoID > 0 {
		return collector.GapForceList
	}
	return 0
}

// gapHealClients are the forge clients a heal lists from.
type gapHealClients struct {
	github, gitlab platform.Client
}

// forPlatform is the ONE rule for which client heals a platform: GitHub
// and GitLab have issue/PR listing APIs, generic git has none (nil). Both
// "can this repo be healed" and "which client heals it" come from here,
// so they cannot drift apart (review rounds 14-15: a second switch in the
// command could send GitLab repos a nil client — a panic that skipped the
// exit release — or GitHub's).
func (c gapHealClients) forPlatform(p model.Platform) platform.Client {
	switch p {
	case model.PlatformGitHub:
		return c.github
	case model.PlatformGitLab:
		return c.gitlab
	}
	return nil
}

// gapFiller heals one repo with the client chosen for its platform, at a
// threshold. The command's filler is the collector's GapFiller.
type gapFiller func(ctx context.Context, client platform.Client, c db.GapHealCandidate, threshold float64) (int, error)

// collectorGapFiller is the command's filler: the collector's GapFiller
// with the configured PR child mode, on the run's context, at the
// threshold the flags chose. Named (not a closure in RunE) so its whole
// body is pinned exactly (review round 16: substring needles let an
// inserted `threshold = 0` or `ctx = context.Background()` through).
func collectorGapFiller(store *db.PostgresStore, logger *slog.Logger, prChildMode string) gapFiller {
	return func(ctx context.Context, client platform.Client, c db.GapHealCandidate, threshold float64) (int, error) {
		gf := collector.NewGapFillerWithMode(store, client, logger, prChildMode)
		return gf.AssessAndFillGapsWithThreshold(ctx, c.RepoID, c.Owner, c.Name, c.MetaIssues, c.MetaPRs, threshold)
	}
}

// validate refuses flags that would silently mean something else: a
// negative --repo-id selected the FLEET heal (the dispatch tests
// repoID > 0), and a negative --limit meant "all" (review round 15;
// pre-existing). The command calls it before connecting (round 16: a
// flag error must not hide behind a connection or key error), and
// newGapHealRun calls it again so no caller can skip it.
func (f gapHealFlags) validate() error {
	if f.repoID < 0 {
		return fmt.Errorf("--repo-id %d: a repo id is positive (0 means the fleet)", f.repoID)
	}
	if f.limit < 0 {
		return fmt.Errorf("--limit %d: a limit is positive (0 means all candidates)", f.limit)
	}
	return nil
}

// runGapHeal builds the run and runs it: everything the command does after
// its setup. It returns the run it built (nil when the flags are refused)
// so the tests drive THIS function end to end and read its counters
// (review round 17: tests that built their own run left this body
// untested). pageSize is a parameter so tests use small pages without
// patching the run; the command passes gapHealPageSize.
func runGapHeal(ctx context.Context, restoreSignals func(), f gapHealFlags, store gapHealStore, logger *slog.Logger, out io.Writer,
	workerID string, pageSize int, clients gapHealClients, filler gapFiller) (*gapHealRun, error) {
	run, err := newGapHealRun(f, store, logger, out, workerID, pageSize, clients, filler)
	if err != nil {
		return nil, err
	}
	run.restoreSignals = restoreSignals
	return run, run.run(ctx, f.repoID, f.afterRepoID)
}

// newGapHealRun builds the run the flags describe; it refuses invalid
// flags (validate).
func newGapHealRun(f gapHealFlags, store gapHealStore, logger *slog.Logger, out io.Writer, workerID string,
	pageSize int, clients gapHealClients, filler gapFiller) (*gapHealRun, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}
	if pageSize < 1 {
		// A zero page ends every fleet run at "0 candidates", the
		// documented "done" signal (review rounds 14-15).
		return nil, fmt.Errorf("page size %d: must be at least 1", pageSize)
	}
	workers := f.workers
	if workers < 1 {
		workers = 1
	}
	threshold := f.threshold()
	return &gapHealRun{
		store: store, logger: logger, out: out, workerID: workerID,
		workers: workers, limit: f.limit, pageSize: pageSize,
		sweepAll: f.sweepAll, dryRun: f.dryRun,
		canHeal: func(c db.GapHealCandidate) bool { return clients.forPlatform(c.Platform) != nil },
		fill: func(ctx context.Context, c db.GapHealCandidate) (int, error) {
			client := clients.forPlatform(c.Platform)
			if client == nil {
				// canHeal screens these out; enforced here too, so a wrong
				// caller gets an error, never a nil-client panic (SR-18).
				return 0, fmt.Errorf("repo %d: no forge client for platform %d", c.RepoID, c.Platform)
			}
			return filler(ctx, client, c, threshold)
		},
	}, nil
}

// gapHealRun is one heal-collection-gaps run.
type gapHealRun struct {
	store    gapHealStore
	logger   *slog.Logger
	out      io.Writer // dry-run listing
	workerID string
	workers  int
	limit    int
	pageSize int
	sweepAll bool
	dryRun   bool
	// canHeal reports whether a candidate's forge has an API to list
	// (generic git does not); fill heals one repo and returns the items
	// it filled.
	canHeal func(db.GapHealCandidate) bool
	fill    func(ctx context.Context, c db.GapHealCandidate) (int, error)
	// restoreSignals ends the interrupt capture; run calls it on the first
	// interrupt, so a second Ctrl-C ends the process. Optional.
	restoreSignals func()

	filled, failed, skipped, visited int64
}

// run heals one repo (repoID > 0) or the candidate fleet from
// afterRepoID. On every exit, an interrupt included, it stops the
// heartbeat and then releases every row this run still parks (v0.29.65:
// an interrupted heal left them "collecting").
func (r *gapHealRun) run(ctx context.Context, repoID, afterRepoID int64) error {
	// Default signal handling comes back the moment the first interrupt
	// lands: a worker whose release stalls on a slow database must not
	// swallow every further Ctrl-C (review round 4). Deferred before the
	// exit release, so it stays armed through it: an interrupt during the
	// release restores it too.
	if r.restoreSignals != nil {
		stop := context.AfterFunc(ctx, r.restoreSignals)
		defer stop()
	}
	defer r.releaseAll() // registered first, so it runs after the heartbeat stops
	stopHB := r.store.StartDrainHeartbeat(ctx, r.logger, r.workerID)
	defer stopHB()
	if repoID > 0 {
		return r.runOne(ctx, repoID)
	}
	return r.runFleet(ctx, afterRepoID)
}

func (r *gapHealRun) releaseAll() {
	// A context the interrupt did not cancel.
	if n, err := r.store.ReleaseDrainLocks(context.Background(), r.workerID); err != nil {
		r.logger.Warn("releasing this run's parked rows failed — stale-lock recovery or the next serve start reclaims them", "worker_id", r.workerID, "error", err)
	} else if n > 0 {
		r.logger.Info("released parked rows on exit", "count", n, "worker_id", r.workerID)
	}
}

// healOne heals one candidate. After an interrupt nothing is counted as
// failed and nothing is logged above INFO.
func (r *gapHealRun) healOne(ctx context.Context, c db.GapHealCandidate) {
	if ctx.Err() != nil || !r.canHeal(c) {
		return // interrupted, or generic git (nothing to list)
	}
	// Drain-lock: only 'queued' rows lock; a repo mid-collection is
	// skipped (a rerun catches it).
	locked, lerr := r.store.LockReposForDrain(ctx, []int64{c.RepoID}, r.workerID)
	if lerr != nil {
		if ctx.Err() != nil {
			return // interrupted: not a failure (a lock that committed anyway is freed by the exit release)
		}
		r.logger.Warn("drain lock failed — skipping repo", "repo_id", c.RepoID, "error", lerr)
		atomic.AddInt64(&r.failed, 1)
		return
	}
	if len(locked) == 0 {
		// Only 'queued' rows lock: almost always a repo mid-collection, but
		// single-repo mode can also name a repo with no queue row, which
		// runOne explains.
		r.logger.Info("repo cannot be drain-locked (its queue row is not 'queued') — skipped", "repo_id", c.RepoID)
		atomic.AddInt64(&r.skipped, 1)
		return
	}
	defer func() {
		// Not the run's context: an interrupt must not turn the release
		// into a failure (the exit release covers it either way).
		if err := r.store.ReleaseDrainLock(context.Background(), c.RepoID, r.workerID); err != nil {
			r.logger.Warn("drain unlock failed — the exit release retries it", "repo_id", c.RepoID, "error", err)
		}
	}()
	filled, ferr := r.fill(ctx, c)
	atomic.AddInt64(&r.visited, 1)
	atomic.AddInt64(&r.filled, int64(filled))
	if ferr != nil {
		if ctx.Err() != nil {
			r.logger.Info("gap heal interrupted mid-repo — a rerun revisits it", "repo_id", c.RepoID)
			return
		}
		r.logger.Warn("gap heal error", "repo_id", c.RepoID, "owner", c.Owner, "repo", c.Name, "error", ferr)
		atomic.AddInt64(&r.failed, 1)
		return
	}
	// Round-23: refresh the queue's cached counts so a healed repo DROPS
	// OUT of the candidate set (the healer fills rows without a
	// CompleteJob pass). A failed refresh only means the rerun revisits
	// this repo's (cheap) listing — warn, don't fail the heal.
	if err := r.store.RefreshQueueGatheredCounts(ctx, c.RepoID); err != nil {
		if ctx.Err() != nil {
			r.logger.Info("repo healed; the gathered-count refresh was interrupted — a rerun revisits it", "repo_id", c.RepoID, "filled", filled)
			return
		}
		r.logger.Warn("gathered-count refresh failed — repo stays a candidate until rerun", "repo_id", c.RepoID, "error", err)
	}
	r.logger.Info("repo healed", "repo_id", c.RepoID, "owner", c.Owner, "repo", c.Name, "count_gap", c.Gap, "filled", filled)
}

// runOne heals exactly one repo. An interrupt is an error, never a
// silent success.
func (r *gapHealRun) runOne(ctx context.Context, repoID int64) error {
	interrupted := func() error {
		r.logger.Info("gap heal interrupted — rerun", "repo_id", repoID)
		return fmt.Errorf("gap heal of repo %d interrupted — rerun: %w", repoID, ctx.Err())
	}
	repo, err := r.store.GetRepoByID(ctx, repoID)
	if err != nil {
		if ctx.Err() != nil {
			return interrupted()
		}
		return fmt.Errorf("repo %d: %w", repoID, err)
	}
	metaIssues, metaPRs, err := r.store.GetRepoMetaCounts(ctx, repoID)
	if err != nil {
		if ctx.Err() != nil {
			return interrupted()
		}
		return fmt.Errorf("meta counts for repo %d: %w", repoID, err)
	}
	c := db.GapHealCandidate{RepoID: repoID, Owner: repo.Owner, Name: repo.Name,
		Platform: repo.Platform, MetaIssues: metaIssues, MetaPRs: metaPRs}
	if !r.canHeal(c) {
		r.logger.Info("repo has no forge API to list (generic git) — nothing to heal", "repo_id", repoID)
		return nil
	}
	// --dry-run touches nothing in single-repo mode too (review round 12:
	// it used to heal the repo) and says what a real run would do (round
	// 13): heal it, or refuse because it cannot be locked now.
	if r.dryRun {
		return r.dryRunOne(ctx, c, interrupted)
	}
	r.healOne(ctx, c)
	if ctx.Err() != nil {
		return interrupted()
	}
	if r.failed > 0 {
		return fmt.Errorf("gap heal finished with %d failure(s)", r.failed)
	}
	// Refused, not healed: a script looping until exit 0 must not read a
	// skip as done (review round 7), and the reason must be the real one —
	// a repo with no queue row is never collected again (round 8).
	if r.skipped > 0 {
		err := r.explainSkip(ctx, repoID)
		if ctx.Err() != nil {
			return interrupted() // a Ctrl-C inside the status read
		}
		return err
	}
	return nil
}

// dryRunOne prints the single candidate and whether a real run could lock
// it now; it locks, fills and refreshes nothing.
func (r *gapHealRun) dryRunOne(ctx context.Context, c db.GapHealCandidate, interrupted func() error) error {
	q, found, err := r.store.GetQueueStatus(ctx, c.RepoID)
	if err != nil {
		if ctx.Err() != nil {
			return interrupted()
		}
		return fmt.Errorf("repo %d: queue status: %w", c.RepoID, err)
	}
	fmt.Fprintf(r.out, "repo %d  %s/%s  (meta issues=%d prs=%d)\n", c.RepoID, c.Owner, c.Name, c.MetaIssues, c.MetaPRs)
	switch {
	case !found:
		fmt.Fprintf(r.out, "\nNot in the collection queue (gone or dequeued): a real run would refuse it — nothing to heal.\n")
	case q.Status != "queued":
		fmt.Fprintf(r.out, "\nNot queued now (being collected, or parked by a drain or heal): a real run would refuse it until it is queued.\n")
	default:
		fmt.Fprintf(r.out, "\nRe-run without --dry-run to heal.\n")
	}
	return nil
}

// explainSkip names why the single repo could not be drain-locked. Every
// answer is an error: nothing was healed.
func (r *gapHealRun) explainSkip(ctx context.Context, repoID int64) error {
	q, found, err := r.store.GetQueueStatus(ctx, repoID)
	switch {
	case err != nil:
		return fmt.Errorf("repo %d could not be locked for healing, and its queue status could not be read: %w", repoID, err)
	case !found:
		return fmt.Errorf("repo %d is not in the collection queue (gone or dequeued) — nothing to heal", repoID)
	case q.Status == "queued":
		// Only 'queued' and 'collecting' are ever written; a queued row
		// here means it was released between the lock attempt and this
		// read (a collection finished, or a drain released it).
		return fmt.Errorf("repo %d became queued after the lock attempt — nothing healed; rerun now", repoID)
	case q.Drain():
		return fmt.Errorf("repo %d is parked by a staging drain or another heal run — nothing healed; rerun when it is released (a crashed owner's park is reclaimed by stale-lock recovery or the next serve start)", repoID)
	default:
		return fmt.Errorf("repo %d is being collected — nothing healed; rerun when its collection finishes (a crashed owner's lock is reclaimed by stale-lock recovery or the next serve start)", repoID)
	}
}

// runFleet heals the candidates in keyset pages through a worker pool.
// An interrupt returns an error naming the start of the page it cut
// short: resuming there revisits the page (healed repos are cheap),
// never skips what went unvisited.
func (r *gapHealRun) runFleet(ctx context.Context, after int64) error {
	processed := 0
	interrupted := func(pageStart int64) error {
		r.logger.Info("gap heal interrupted — rerun, or resume with --after-repo-id",
			"candidates_before_this_page", processed, "after_repo_id", pageStart,
			"failed_before_interrupt", atomic.LoadInt64(&r.failed))
		return fmt.Errorf("gap heal interrupted (resume with --after-repo-id %d): %w", pageStart, ctx.Err())
	}
	for {
		if ctx.Err() != nil {
			return interrupted(after)
		}
		pageSize := r.pageSize
		if r.limit > 0 && r.limit-processed < pageSize {
			pageSize = r.limit - processed
		}
		if pageSize <= 0 {
			break
		}
		candidates, err := r.store.GetGapHealCandidates(ctx, after, pageSize, r.sweepAll)
		if err != nil {
			if ctx.Err() != nil {
				return interrupted(after)
			}
			return fmt.Errorf("candidate query: %w", err)
		}
		if len(candidates) == 0 {
			break
		}
		if r.dryRun {
			for _, c := range candidates {
				fmt.Fprintf(r.out, "repo %d  %s/%s  count_gap=%d  (meta issues=%d prs=%d)\n",
					c.RepoID, c.Owner, c.Name, c.Gap, c.MetaIssues, c.MetaPRs)
			}
		} else {
			ch := make(chan db.GapHealCandidate)
			var wg sync.WaitGroup
			for range r.workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for c := range ch {
						r.healOne(ctx, c)
					}
				}()
			}
			for _, c := range candidates {
				ch <- c
			}
			close(ch)
			wg.Wait()
			if ctx.Err() != nil {
				return interrupted(after) // BEFORE the keyset advances
			}
		}
		processed += len(candidates)
		after = candidates[len(candidates)-1].RepoID
	}
	if r.dryRun {
		fmt.Fprintf(r.out, "\n%d candidate repo(s). Re-run without --dry-run to heal.\n", processed)
		return nil
	}
	r.logger.Info("gap heal complete",
		"candidates", processed, "visited", r.visited,
		"items_filled", r.filled, "skipped_not_queued", r.skipped,
		"failed", r.failed)
	if r.failed > 0 {
		return fmt.Errorf("gap heal finished with %d failure(s) — rerun retries them (the candidate query is the resume state)", r.failed)
	}
	return nil
}
