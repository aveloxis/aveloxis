// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
)

// activityRefresher is the narrow capability both drain-side callers
// need: recompute the queue row's cached gathered counts (which
// include last_activity_90d, the home-ranking signal).
type activityRefresher interface {
	RefreshQueueGatheredCounts(ctx context.Context, repoID int64) error
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
func refreshRepoActivity(ctx context.Context, s activityRefresher, logger *slog.Logger, repoID int64, source string) {
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
