// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
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
	var after int64 // keyset cursor: each repository is visited once per run

	for {
		if ctx.Err() != nil {
			s.logger.Info("repo metadata backfill stopping (ctx cancelled)",
				"processed", totalProcessed, "failed", totalFailed)
			return
		}

		targets, err := s.store.ReposNeedingMetadataBackfill(ctx, after, metadataBackfillPageSize)
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
				"processed", totalProcessed, "failed", totalFailed)
			return
		}

		after = targets[len(targets)-1].RepoID
		for _, t := range targets {
			if ctx.Err() != nil {
				return
			}

			// Pick the platform client by the repo's platform_id and, for a
			// GitLab instance, its URL (v0.30.0: never another instance's).
			var client platform.Client
			switch p := model.Platform(t.PlatformID); {
			case p == model.PlatformGitHub:
				client = s.ghClient
			case p.IsGitLab():
				c, err := s.gl.ForRepo(p, t.GitURL)
				if err != nil {
					mismatch := errors.Is(err, gitlab.ErrInstanceMismatch)
					if _, warned := s.unconfiguredInstanceWarned.LoadOrStore(fmt.Sprintf("metadata/%d/%t", p, mismatch), struct{}{}); !warned {
						s.logger.Info("repo metadata backfill: skipping GitLab repositories this process cannot collect (logged once per instance and reason)", "platform_id", p, "repo_id", t.RepoID, "error", err)
					}
					totalFailed++
					continue
				}
				client = c
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
			if errors.Is(err, context.Canceled) {
				return // shutdown, not a failure
			}
			if err != nil {
				s.logger.Info("repo metadata backfill: FetchRepoInfo failed (will retry next restart)",
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
