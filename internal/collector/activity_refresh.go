// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
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
// part of its writes before failing stamps once (partialWriteStamps): a
// repository without a queue row has no next collection to show them
// (whole-branch review of 0.29.119–0.29.123, A5).
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

// partialWriteStamps decides when a staged row's PARTIAL writes stamp its
// repository (whole-branch review of 0.29.119–0.29.123, A5): a row that
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
// drain (round 1 finding 3 of 0.29.123). A restart forgets the rows, so each
// process stamps each new high-water mark once; a row that completes is
// forgotten (its final write stamps). The map holds only rows still staged
// and failing. Safe for the drain goroutines that share a processor; the
// zero value is ready.
type partialWriteStamps struct {
	mu   sync.Mutex
	rows map[int64]int // staged row → the furthest progress stamped
}

// advanced reports whether progress — the store calls the row's attempt
// completed — is further than any this process stamped for rowID, and
// records it.
func (p *partialWriteStamps) advanced(rowID int64, progress int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if progress <= p.rows[rowID] {
		return false
	}
	if p.rows == nil {
		p.rows = map[int64]int{}
	}
	p.rows[rowID] = progress
	return true
}

// done forgets rowID: it left the staging table.
func (p *partialWriteStamps) done(rowID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.rows, rowID)
}
