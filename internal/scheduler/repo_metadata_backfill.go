// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// runRepoMetadataBackfill iterates repos whose repo_description AND
// primary_language are both empty (i.e. tracked pre-v0.23.0 or pre-
// FetchRepoInfo-extension), fetches their metadata via the platform
// API, and updates the repos row.
//
// One-shot: spawned by Scheduler.Run at startup, exits when the
// candidate query returns an empty page. Idempotent: subsequent
// restarts re-target only repos still missing the data, so a partial
// run from a prior restart resumes from wherever it left off.
//
// Rate-limited at one FetchRepoInfo per second across the whole
// process so the backfill does not compete with main collection
// traffic for API budget. At that pace, a 100K-repo fleet completes
// in ~28 hours — well under the natural recollect cycle (21 days
// default) so the backfill leads recollection, not trails it.
//
// Every answer is stamped (metadata_backfill_attempted_at, v0.29.68,
// worklist 69) and the candidate query skips repos answered within one
// recollect interval (collection.days_until_recollect), so an honestly
// empty forge answer or a 404 is asked again once per interval, not at
// every restart. Non-answers (network, rate limit, 5xx) are logged and
// skipped unstamped; the next restart retries them. A GitHub candidate met with no usable GitHub key is not
// a failure: it is counted apart (skipped_no_github_key) and left for a
// restart with a key. Permanent 404s (renamed/deleted repos) are asked
// again once per recollect interval until prelim's rename-detect or the
// operator removes the repo.
//
// v0.23.0 — see summary/changelog/v0.23.md, "Capture description and primary languages"
// rationale.
const (
	metadataBackfillPageSize     = 500
	metadataBackfillSleepBetween = 1 * time.Second
)

func (s *Scheduler) runRepoMetadataBackfill(ctx context.Context) {
	s.logger.Info("repo metadata backfill starting (v0.23.0)")
	totalProcessed := 0
	totalFailed := 0
	skippedNoKey := 0 // GitHub candidates met with no usable GitHub key
	// Keyset cursor: pages advance past every repo seen, including the ones
	// whose fetch failed (nothing is stamped on failure, so a plain LIMIT
	// re-served them on every later page).
	var afterRepoID int64

	for {
		if ctx.Err() != nil {
			s.logger.Info("repo metadata backfill stopping (ctx cancelled)",
				"processed", totalProcessed, "failed", totalFailed, "skipped_no_github_key", skippedNoKey)
			return
		}

		targets, err := s.store.ReposNeedingMetadataBackfill(ctx, afterRepoID, metadataBackfillPageSize, s.cfg.Collection.RecollectAfterDuration())
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure
		}
		if err != nil {
			s.logger.Warn("repo metadata backfill: failed to load candidate page",
				"error", err)
			return
		}
		if len(targets) == 0 {
			s.logger.Info("repo metadata backfill complete",
				"processed", totalProcessed, "failed", totalFailed, "skipped_no_github_key", skippedNoKey)
			return
		}

		for _, t := range targets {
			if ctx.Err() != nil {
				return
			}

			// Pick the platform client by repo's platform_id.
			var client platform.Client
			switch model.Platform(t.PlatformID) {
			case model.PlatformGitHub:
				if !s.githubKeysAvailable() {
					// GitLab-only keys (items 40/21, review round 2): the
					// fetch would fail on the pool's "no API keys", one
					// Info line and the pacing sleep per repository, every
					// restart. GitLab candidates still run. Counted apart
					// from failures (review round 3): "failed=N" with no
					// line explaining it sent the operator looking.
					skippedNoKey++
					continue
				}
				client = s.ghClient
			case model.PlatformGitLab:
				client = s.glClient
			default:
				// Generic-git repos have no API to ask. The candidate
				// query excludes them (platform_id IN (1, 2), v0.29.57),
				// so this arm should be unreachable — it stays as a
				// backstop for a platform added to the query and not to
				// the switch. Reaching it once per restart forever is
				// what the filter fixed: nothing here stamps the row, so
				// without the filter the same rows came back every time
				// and were counted as failures.
				totalFailed++
				continue
			}
			if client == nil {
				totalFailed++
				continue
			}

			info, err := client.FetchRepoInfo(ctx, t.Owner, t.Name)
			if errors.Is(err, context.Canceled) {
				return // shutdown, not a failure
			}
			// v0.29.68 (worklist 69): stamp every ANSWER so the candidate
			// query leaves the repo alone for one recollect interval — an
			// honestly empty description and language, or a definitive
			// 404/gone, otherwise kept ~5,600 repos candidates forever.
			// A non-answer (rate limit, 5xx, auth, empty key pool) is not
			// stamped: it says nothing about the repo (SR-5/SR-16, the
			// IsDefinitiveAnswer rule enrichment and search-resolve follow),
			// and stamping it would park a whole pool-level outage's worth
			// of repos for a full interval. A failed UpdateRepoMetadata is
			// not stamped either: the answer was never written (SR-3).
			answered := false
			if err != nil {
				answered = platform.IsDefinitiveAnswer(err)
				retry := "will retry next restart"
				if answered {
					retry = "definitive; will retry after the recollect interval"
				}
				s.logger.Info("repo metadata backfill: FetchRepoInfo failed ("+retry+")",
					"owner", t.Owner, "repo", t.Name, "error", err)
				totalFailed++
			} else {
				updErr := s.store.UpdateRepoMetadata(ctx, t.RepoID, info.Description, info.PrimaryLanguage, info.Languages, info.Status == "Archived", info.ForkedFrom(), info.PlatformRepoID, info.CreatedAt, info.LastUpdated)
				if errors.Is(updErr, context.Canceled) {
					return // shutdown, not a failure
				}
				if updErr != nil {
					s.logger.Warn("repo metadata backfill: UpdateRepoMetadata failed",
						"owner", t.Owner, "repo", t.Name, "error", updErr)
					totalFailed++
				} else {
					totalProcessed++
					answered = true
				}
			}
			if answered {
				if mErr := s.store.MarkMetadataBackfillAttempted(ctx, t.RepoID); mErr != nil {
					if errors.Is(mErr, context.Canceled) {
						return // shutdown, not a failure
					}
					// The repo is asked again at the next start; nothing lost.
					s.logger.Warn("repo metadata backfill: stamping the attempt failed (the repo is asked again next restart)",
						"owner", t.Owner, "repo", t.Name, "repo_id", t.RepoID, "error", mErr)
				}
			}

			// Rate-limit. ctx-aware sleep so a cancellation wakes
			// immediately instead of waiting out the timer.
			select {
			case <-time.After(metadataBackfillSleepBetween):
			case <-ctx.Done():
				return
			}
		}

		afterRepoID = metadataBackfillCursor(targets, afterRepoID)

		// Log progress every page so operators can monitor.
		s.logger.Info("repo metadata backfill progress",
			"processed", totalProcessed, "failed", totalFailed, "skipped_no_github_key", skippedNoKey, "after_repo_id", afterRepoID)
	}
}

// metadataBackfillCursor returns the keyset cursor for the next page: the
// highest repo_id in this page, or the current cursor for an empty page.
func metadataBackfillCursor(targets []db.RepoMetadataBackfillTarget, current int64) int64 {
	for _, t := range targets {
		if t.RepoID > current {
			current = t.RepoID
		}
	}
	return current
}
