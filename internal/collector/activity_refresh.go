// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
)

// activityRefresher is the narrow capability both drain-side callers
// need: recompute the queue row's cached gathered counts (which
// include last_activity_90d, the home-ranking signal), and stamp the
// repository's cache state.
type activityRefresher interface {
	RefreshQueueGatheredCounts(ctx context.Context, repoID int64) error
	StampReposChangedOutsideJob(ctx context.Context, repoIDs []int64) error
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
// deferred for retry, an envelope that failed — does not stamp: it comes
// back next drain, and a lasting failure would move the state every drain
// (round 1 finding 3 of 0.29.123); its partial writes show when it
// succeeds, or at the next collection.
func refreshRepoActivity(ctx context.Context, s activityRefresher, logger *slog.Logger, repoID int64, source string, wrote bool) {
	// Detached: a stop is no reason to skip the stamp (the rows landed), so
	// a cancellation cannot end it and is not classified here.
	detached := context.WithoutCancel(ctx)
	if !wrote {
		// Nothing final to stamp.
	} else if err := s.StampReposChangedOutsideJob(detached, []int64{repoID}); err != nil {
		logger.Warn("cache-state stamp failed after drain (the repository's cached API answers stay until its next collection)",
			"source", source, "repo_id", repoID, "error", err)
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
