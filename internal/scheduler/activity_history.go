// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

// activity_history.go — the v0.27.58 daily contributor-history sweep.
// For each claimed contributor: fetch account meta (createdAt +
// contributionYears), tile the account's active span into
// operator-configured windows (activity_history_window_days, default
// 180), fetch each window's per-(day, repo) public activity + daily
// calendar totals (the fetcher subdivides on cap hits and logs them at
// INFO), and store everything with the gh_history_backfilled_at stamp
// in one transaction. The claim's NULL-first ordering makes newly
// discovered contributors self-staging, and its 90-day cooldown IS the
// quarterly re-audit (operator decision 2026-07-30).
//
// GITHUB-ONLY via the narrow capability interface below (GitLab has no
// contributionsCollection equivalent; platform.Client is not widened).

// contributorHistoryFetcher is the capability the sweep needs from the
// GitHub client.
type contributorHistoryFetcher interface {
	FetchContributorHistoryMeta(ctx context.Context, login string) (time.Time, []int, error)
	FetchContributorDailyHistory(ctx context.Context, login string, windows []github.HistoryWindow) ([]model.ContributorDayActivity, []model.ContributorDayTotal, error)
}

// Cadence constants — derived, not magic. The full-fat window query
// costs 1 rate-limit point (live-verified 2026-07-30), so the budget
// constraint is query COUNT, not points. Batch 25 contributors per
// 1-minute tick at an average ~10-30 windows each ≈ 250-750 queries/
// minute peak ≈ 12% of the 73-token pool's hourly capacity, and the
// bootstrap covers the active-classified million (claimed first) in
// roughly two weeks. The re-audit cooldown is quarterly.
const (
	activityHistoryInterval = time.Minute
	activityHistoryBatch    = 25
	activityHistoryCooldown = 90 * 24 * time.Hour
)

// runActivityHistory performs one sweep tick. Per-contributor failure
// contract: a missing account (meta 404 → ErrNotFound class) is
// mark-only stamped; any OTHER error stamps nothing so the contributor
// retries on the next claim.
func (s *Scheduler) runActivityHistory(ctx context.Context) {
	fetcher, ok := s.ghClient.(contributorHistoryFetcher)
	if !ok {
		return // no GitHub GraphQL client (GitLab-only deployment or test fake)
	}
	claimed, err := s.store.GetContributorsForHistoryBackfill(ctx, activityHistoryBatch, activityHistoryCooldown)
	if err != nil {
		s.logger.Warn("activity history: claim failed", "error", err)
		return
	}
	if len(claimed) == 0 {
		return
	}
	windowDays := s.cfg.Collection.ActivityHistoryWindowDaysOrDefault()
	start := time.Now()
	stored, marked, failed := 0, 0, 0
	for _, c := range claimed {
		if ctx.Err() != nil {
			return
		}
		created, years, err := fetcher.FetchContributorHistoryMeta(ctx, c.Login)
		if err != nil {
			if errors.Is(err, platform.ErrNotFound) || platform.ClassifyError(err) == platform.ClassSkip {
				// Deleted/renamed account: stamp so the claim head drains.
				// A failed stamp leaves the contributor at the claim head
				// for a pointless re-fetch every tick — log it and count
				// it as a failure so the operator sees the churn (Copilot
				// review, PR #171).
				if merr := s.store.MarkHistoryBackfilled(ctx, c.ID); merr != nil {
					s.logger.Warn("activity history: mark-only stamp failed — contributor will be re-claimed", "login", c.Login, "error", merr)
					failed++
				} else {
					marked++
				}
				continue
			}
			s.logger.Warn("activity history: meta fetch failed — will retry on next claim", "login", c.Login, "error", err)
			failed++
			continue
		}
		windows := github.HistoryWindows(created, years, time.Now().UTC(), windowDays)
		if len(windows) == 0 {
			// Account exists but has zero contribution years: nothing to
			// fetch, ever — stamp it. Same failure treatment as above.
			if merr := s.store.MarkHistoryBackfilled(ctx, c.ID); merr != nil {
				s.logger.Warn("activity history: zero-years stamp failed — contributor will be re-claimed", "login", c.Login, "error", merr)
				failed++
			} else {
				marked++
			}
			continue
		}
		days, totals, err := fetcher.FetchContributorDailyHistory(ctx, c.Login, windows)
		if err != nil {
			s.logger.Warn("activity history: fetch failed — will retry on next claim", "login", c.Login, "windows", len(windows), "error", err)
			failed++
			continue
		}
		if err := s.store.StoreContributorActivityHistory(ctx, c.ID, days, totals); err != nil {
			s.logger.Warn("activity history: store failed — will retry on next claim", "login", c.Login, "error", err)
			failed++
			continue
		}
		stored++
	}
	s.logger.Info("activity history cycle complete",
		"claimed", len(claimed), "stored", stored, "marked_no_data", marked, "failed", failed,
		"window_days", windowDays, "duration", time.Since(start).Truncate(time.Millisecond))
}
