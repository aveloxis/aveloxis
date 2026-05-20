// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"time"

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
// Per-repo failures (network, 404, rate limit) are logged and
// skipped; the repo stays in the candidate set and the next restart
// retries it. Permanent 404s (renamed/deleted repos) cycle until
// prelim's rename-detect or the operator removes the repo.
//
// v0.23.0 — see CLAUDE.md "Capture description and primary languages"
// rationale.
const (
	metadataBackfillPageSize     = 500
	metadataBackfillSleepBetween = 1 * time.Second
)

func (s *Scheduler) runRepoMetadataBackfill(ctx context.Context) {
	s.logger.Info("repo metadata backfill starting (v0.23.0)")
	totalProcessed := 0
	totalFailed := 0

	for {
		if ctx.Err() != nil {
			s.logger.Info("repo metadata backfill stopping (ctx cancelled)",
				"processed", totalProcessed, "failed", totalFailed)
			return
		}

		targets, err := s.store.ReposNeedingMetadataBackfill(ctx, metadataBackfillPageSize)
		if err != nil {
			s.logger.Warn("repo metadata backfill: failed to load candidate page",
				"error", err)
			return
		}
		if len(targets) == 0 {
			s.logger.Info("repo metadata backfill complete",
				"processed", totalProcessed, "failed", totalFailed)
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
				client = s.ghClient
			case model.PlatformGitLab:
				client = s.glClient
			default:
				// Generic-git repos have no API; skip them. They'll
				// be excluded from the next candidate query
				// automatically once we stamp something on the row,
				// but for now the simplest thing is to leave them
				// in the candidate set and let the SELECT filter
				// out generic-git via repo_archived = FALSE
				// (generic-git repos aren't archived but they also
				// have no useful description source).
				totalFailed++
				continue
			}
			if client == nil {
				totalFailed++
				continue
			}

			info, err := client.FetchRepoInfo(ctx, t.Owner, t.Name)
			if err != nil {
				s.logger.Info("repo metadata backfill: FetchRepoInfo failed (will retry next restart)",
					"owner", t.Owner, "repo", t.Name, "error", err)
				totalFailed++
			} else {
				if updErr := s.store.UpdateRepoMetadata(ctx, t.RepoID, info.Description, info.PrimaryLanguage, info.Languages); updErr != nil {
					s.logger.Warn("repo metadata backfill: UpdateRepoMetadata failed",
						"owner", t.Owner, "repo", t.Name, "error", updErr)
					totalFailed++
				} else {
					totalProcessed++
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

		// Log progress every page so operators can monitor.
		s.logger.Info("repo metadata backfill progress",
			"processed", totalProcessed, "failed", totalFailed)
	}
}
