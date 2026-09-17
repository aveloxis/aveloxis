// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// keyPoolSummaryInterval paces the per-key pool summary. Five minutes:
// GitHub's secondary-limit Retry-After is 60 s (300 s escalated), so a
// throttled key's rest is visible in at least one line, and the
// history sweep's cycle (~1–2 min) is sampled several times per hour
// without the summary becoming the log's dominant line.
const keyPoolSummaryInterval = 5 * time.Minute

// keyPoolSummaryHotKeys bounds the per-key detail on the INFO line to
// the busiest keys; the full per-key snapshot goes to Debug.
const keyPoolSummaryHotKeys = 5

// logKeyPoolSummary emits ONE INFO line describing the GitHub pool's
// admission state — the 2026-09-12 observability item. /rate_limit
// cannot report secondary limits (they are observable only by tripping
// them), and an aggregate remaining/alive pair cannot show skew; this
// line carries the pool-wide in-flight count, how many keys are
// resting on a secondary cooldown, the lifetime secondary-hit total,
// the remaining-budget spread across keys, and the busiest keys by
// in-flight count — enough to see concentration the moment it starts.
// Per-key rows at Debug for the operator who wants all 54.
func (s *Scheduler) logKeyPoolSummary() {
	if s.ghKeys == nil {
		return
	}
	keys, inflight := s.ghKeys.Snapshot()
	if len(keys) == 0 {
		return
	}
	now := time.Now()
	var (
		alive, resting, quarantined, lent    int
		secondaryHits                        int
		refusedCore, refusedGQL, refusedSrch int
		refusals                             int
		minCore, maxCore, totalCore          = -1, -1, 0
		minGQL, maxGQL, totalGQL             = -1, -1, 0
	)
	for _, k := range keys {
		// Lifetime counters include invalidated keys: skipping them first
		// made a "lifetime" total go down when a key was invalidated
		// (Copilot review on PR #209). The current-state gauges below stay
		// limited to live keys.
		secondaryHits += k.SecondaryHits
		refusals += k.Refusals
		if k.Invalid {
			continue
		}
		alive++
		lent += k.Lent
		if now.Before(k.SecondaryUntil) {
			resting++
		}
		if now.Before(k.QuarantineUntil) {
			quarantined++
		}
		if now.Before(k.CoreRefusedUntil) {
			refusedCore++
		}
		if now.Before(k.GraphQLRefusedUntil) {
			refusedGQL++
		}
		if now.Before(k.SearchRefusedUntil) {
			refusedSrch++
		}
		totalCore += k.Core
		totalGQL += k.GraphQL
		if minCore < 0 || k.Core < minCore {
			minCore = k.Core
		}
		if k.Core > maxCore {
			maxCore = k.Core
		}
		if minGQL < 0 || k.GraphQL < minGQL {
			minGQL = k.GraphQL
		}
		if k.GraphQL > maxGQL {
			maxGQL = k.GraphQL
		}
	}
	// Busiest keys by in-flight count (ties by fewest points remaining —
	// the key that would trip a limit first).
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i].Inflight != keys[j].Inflight {
			return keys[i].Inflight > keys[j].Inflight
		}
		return keys[i].GraphQL < keys[j].GraphQL
	})
	hot := make([]string, 0, keyPoolSummaryHotKeys)
	for _, k := range keys {
		if len(hot) == keyPoolSummaryHotKeys || k.Inflight == 0 {
			break
		}
		hot = append(hot, fmt.Sprintf("%s:%d", k.Prefix, k.Inflight))
	}
	s.logger.Info("key pool summary",
		"keys_alive", alive,
		"inflight", inflight,
		"resting_secondary", resting,
		"quarantined", quarantined,
		"lent_to_subprocesses", lent,
		"secondary_hits_lifetime", secondaryHits,
		// 2026-09-17: keys GitHub has refused (403/429 + Remaining: 0)
		// whose refusal still stands. The remaining_* numbers below are
		// the pool's TRACKED balances; on chaoss.tv they read >= 4,305 on
		// every key while one key was being refused thousands of times.
		"refused_core_now", refusedCore,
		"refused_graphql_now", refusedGQL,
		"refused_search_now", refusedSrch,
		"refusals_lifetime", refusals,
		"graphql_remaining_total", totalGQL,
		"graphql_remaining_min", minGQL,
		"graphql_remaining_max", maxGQL,
		"core_remaining_total", totalCore,
		"core_remaining_min", minCore,
		"core_remaining_max", maxCore,
		"hot_keys", strings.Join(hot, ","))
	if s.logger.Enabled(context.Background(), slog.LevelDebug) {
		for _, k := range keys {
			s.logger.Debug("key pool key",
				"token_prefix", k.Prefix, "invalid", k.Invalid,
				"inflight", k.Inflight, "lent", k.Lent,
				"core", k.Core, "graphql", k.GraphQL,
				"secondary_hits", k.SecondaryHits,
				"secondary_until", k.SecondaryUntil,
				"core_refused_until", k.CoreRefusedUntil,
				"graphql_refused_until", k.GraphQLRefusedUntil,
				"search_refused_until", k.SearchRefusedUntil,
				"refusals", k.Refusals,
				"quarantine_until", k.QuarantineUntil)
		}
	}
}
