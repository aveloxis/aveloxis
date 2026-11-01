// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
	"sort"
	"sync"
)

// activityRefresher is the narrow capability both drain-side callers
// need: recompute the queue row's cached gathered counts (which
// include last_activity_90d, the home-ranking signal), and stamp the
// repository's cache state.
type activityRefresher interface {
	RefreshQueueGatheredCounts(ctx context.Context, repoID int64) error
	StampReposChangedOutsideJob(ctx context.Context, repoIDs []int64) error
}

// drainStamps owns the cache-state stamps a drain-side process (the Jira
// and mailing-list drains) still has to make: the partial-write high-water
// marks of the rows it keeps retrying (A5) and the repositories whose stamp
// failed and is owed (Copilot's review of PR #246 on 0.29.125). Shared by
// the drain goroutines of one processor; the zero value is ready. Both are
// process-local: a restart forgets the marks (each process stamps each new
// mark once) and the owed stamps — a process that stops between a write and
// its stamp has always left that write to the repository's next collection,
// and a durable owed list would be a write to the database whose write just
// failed. The other out-of-job stampers (collect, heal-messages, the Jira
// identity and projection backfills, the scheduler's leftover-staging drain)
// are one-shot commands or stamp queue-backed repositories once, with no
// recurring pass to owe into: their WARN is the operator's signal (L10 round
// 1 of 0.29.126, F6).
type drainStamps struct {
	mu   sync.Mutex
	rows map[int64]int      // staged row → the furthest progress stamped
	owed map[int64]struct{} // repositories whose last stamp failed
}

// refreshRepoActivity is the ONE spelling (SR-17) of the drain-side
// activity-cache refresh (Copilot round 10 on PR #193): the Jira and
// mailing-list processors create issue rows OUTSIDE collection jobs,
// so collection_queue.last_activity_90d — refreshed only by
// CompleteJob and the gap healer — went stale until the repo's next
// unrelated recollection, and the home page ranked drain-fed repos as
// inactive. Best-effort: a failed or shutdown-canceled refresh only
// delays the ranking update (the cache self-heals on the next drain
// or collection cycle), so it must never fail the drain that already
// landed its rows.
//
// The cache-state stamp is not best-effort in that sense (D7, 0.29.120,
// summary/63): the API's answers are durable until the repository's state
// moves, and the refresh's queue stamp misses a stop, a failure and a
// repository without a queue row. So when the drain made a FINAL write for
// the repository (wrote: a row processed or dropped, an envelope that fully
// processed) it runs first, on a context the drain's cancel cannot stop
// (the store's), and only its error is logged. A row merely reached — one
// deferred for retry, an envelope that failed before writing — does not
// stamp: it comes back next drain, and a lasting failure would move the
// state every drain (round 1 finding 3 of 0.29.123). A row that committed
// part of its writes before failing stamps once (advanced): a
// repository without a queue row has no next collection to show them
// (whole-branch review of 0.29.119–0.29.123, A5).
//
// A stamp that fails is OWED, not dropped (Copilot's review of PR #246 on
// 0.29.125): the high-water mark was recorded before the stamp ran, so a
// retry reaching the same point would never stamp again, and a repository
// without a queue row would keep its committed rows invisible to the API's
// durable answers for good. The owed repository is stamped by settleOwed at
// the start of the next drain pass — the one consumer of the owed set (L10
// round 1 of 0.29.126, F5: a per-refresh re-stamp was a second mechanism
// settle already covered).
func (d *drainStamps) refreshRepoActivity(ctx context.Context, s activityRefresher, logger *slog.Logger, repoID int64, source string, wrote bool) {
	// Detached: a stop is no reason to skip the stamp (the rows landed), so
	// a cancellation cannot end it and is not classified here.
	detached := context.WithoutCancel(ctx)
	// A success clears no mark: one set while this stamp runs (another list
	// of the same repository, under more than one worker, whose rows landed
	// after this stamp's snapshot and whose own stamp failed) must survive it
	// (L10 round 1 of 0.29.126, F2).
	if wrote {
		if err := s.StampReposChangedOutsideJob(detached, []int64{repoID}); err != nil {
			d.owe(repoID)
			logger.Warn("cache-state stamp failed after drain (owed: the next drain pass stamps the repository again)",
				"source", source, "repo_id", repoID, "error", err)
		}
	}
	if ctx.Err() != nil {
		return // shutdown: don't start a new write on a dead ctx
	}
	if err := s.RefreshQueueGatheredCounts(ctx, repoID); err != nil {
		if ctx.Err() != nil {
			return // canceled mid-write: not a failure worth a WARN
		}
		logger.Warn("activity-cache refresh failed after drain (home ranking stays stale until the next cycle)",
			"source", source, "repo_id", repoID, "error", err)
	}
}

// settleOwed stamps every repository whose earlier stamp failed, in one
// store call on a context the drain's cancel cannot stop. Each drain pass
// calls it first, so an owed stamp waits one pass at most and a repository
// the pass does not touch again is not forgotten. A failure keeps them
// owed and logs one line (the per-item flood rule).
func (d *drainStamps) settleOwed(ctx context.Context, s activityRefresher, logger *slog.Logger, source string) {
	repoIDs := d.takeOwed() // taken before the stamp, re-owed on failure (F2): an owe during the stamp survives its success
	if len(repoIDs) == 0 {
		return
	}
	// Detached, as the stamp in refreshRepoActivity is: the rows landed, so a
	// stop is no reason to skip the stamp; a cancellation cannot end it and
	// is not classified here (the store bounds it, outOfJobStampTimeout).
	detached := context.WithoutCancel(ctx)
	if err := s.StampReposChangedOutsideJob(detached, repoIDs); err != nil {
		for _, id := range repoIDs {
			d.owe(id)
		}
		logger.Warn("owed cache-state stamps failed again (kept for the next drain pass)",
			"source", source, "repos", len(repoIDs), "error", err)
	}
}

func (d *drainStamps) owe(repoID int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.owed == nil {
		d.owed = map[int64]struct{}{}
	}
	d.owed[repoID] = struct{}{}
}

func (d *drainStamps) owes(repoID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.owed[repoID]
	return ok
}

// takeOwed forgets and returns the owed set, in a stable order (one
// deterministic store call).
func (d *drainStamps) takeOwed() []int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]int64, 0, len(d.owed))
	for id := range d.owed {
		out = append(out, id)
	}
	d.owed = nil
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// advanced reports whether progress — the store calls the row's attempt
// completed — is further than any this process stamped for rowID, and
// records it (whole-branch review of 0.29.119–0.29.123, A5): a row that
// committed some writes and then failed stays staged and is retried every
// drain. The data it committed is what the API's answers read, and a
// repository without a queue row (a gone one, its data kept) has no next
// collection to show it, so the row stamps whenever a retry got FURTHER
// than this process had seen — its progress is the number of store calls
// that completed in the row's fixed write sequence, which a retry walks
// again from the start (Copilot's review of PR #246 on 0.29.124: recorded
// as a first-time flag, a retry that committed one comment more before
// failing again never stamped). A retry that reaches the same point stamps
// nothing, so a lasting failure cannot move the repository's state every
// drain (round 1 finding 3 of 0.29.123). A row that completes is forgotten
// (done; its final write stamps), so the map holds only rows still staged
// and failing.
func (d *drainStamps) advanced(rowID int64, progress int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if progress <= d.rows[rowID] {
		return false
	}
	if d.rows == nil {
		d.rows = map[int64]int{}
	}
	d.rows[rowID] = progress
	return true
}

// done forgets rowID: it left the staging table.
func (d *drainStamps) done(rowID int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.rows, rowID)
}
