// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/safego"
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

// Cadence is configurable since v0.28.3 (collection.activity_history_*
// — accessors are the single default layer, SR-10). The original
// hardcoded serial design assumed a cycle finished inside its 1-minute
// tick; measured reality was ~30-minute cycles (each contributor is
// ~21 sequential GraphQL round-trips) under singleFlight → ~1,170
// contributors/day against a 2.4M pool ≈ 5.6 years. Rate limit was
// never the constraint (each window query costs 1 point,
// live-verified 2026-07-30) — RTT serialization was. The pooled
// defaults land at ~13-18% of a 54-key pool's GraphQL point budget.

// activityHistoryOutcome is one contributor's sweep result.
type activityHistoryOutcome int

const (
	historyStored activityHistoryOutcome = iota
	historyMarked
	historyFailed
	// historyCanceled: the worker observed ctx cancellation mid-flight.
	// Counted nowhere — shutdown is not a failure (pass 34); the
	// contributor stays unstamped and re-claims next tick.
	historyCanceled
)

// runActivityHistory performs one sweep tick: claim a batch, fetch
// each contributor's history through a bounded worker pool (v0.28.3 —
// the BreadthWorker pattern), store per contributor. Per-contributor
// failure contract unchanged: a missing account (meta 404 →
// ErrNotFound class) is mark-only stamped; any OTHER error stamps
// nothing so the contributor retries on the next claim.
func (s *Scheduler) runActivityHistory(ctx context.Context) {
	fetcher, ok := s.ghClient.(contributorHistoryFetcher)
	if !ok {
		return // no GitHub GraphQL client (GitLab-only deployment or test fake)
	}
	// v0.28.3: window-level parallelism lives in the CLIENT (the
	// windows of one contributor fetch concurrently); wire the
	// configured value each tick — cheap, idempotent, and keeps the
	// platform package config-free.
	if wc, ok := s.ghClient.(interface{ SetHistoryWindowConcurrency(int) }); ok {
		wc.SetHistoryWindowConcurrency(s.cfg.Collection.ActivityHistoryWindowConcurrencyValue())
	}
	claimed, err := s.store.GetContributorsForHistoryBackfill(ctx,
		s.cfg.Collection.ActivityHistoryBatchValue(),
		s.cfg.Collection.ActivityHistoryCooldownValue())
	if errors.Is(err, context.Canceled) {
		return // shutdown, not a failure
	}
	if err != nil {
		s.logger.Warn("activity history: claim failed", "error", err)
		return
	}
	if len(claimed) == 0 {
		return
	}
	windowDays := s.cfg.Collection.ActivityHistoryWindowDaysOrDefault()
	concurrency := s.cfg.Collection.ActivityHistoryConcurrencyValue()
	start := time.Now()
	var stored, marked, failed atomic.Int64
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, c := range claimed {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(c db.ActivityCheckContributor) {
			defer safego.Recover(s.logger, "activity-history-worker")
			defer wg.Done()
			defer func() { <-sem }()
			// v0.28.10 (Copilot round 7): commit the outcome in a
			// DEFER so a panicking worker still counts — the recover
			// bypassed the switch, letting the cycle log report fewer
			// outcomes than claimed and zero failures for a panic.
			// The default is failed; only a completed call overwrites
			// it (the contributor stays unstamped and re-claims
			// either way — this is counter honesty, not recovery).
			outcome := historyFailed
			defer func() {
				switch outcome {
				case historyStored:
					stored.Add(1)
				case historyMarked:
					marked.Add(1)
				case historyCanceled:
				default:
					failed.Add(1)
					// Code-review round 2026-09-06 (finding 3): stamp the
					// failure so HistoryFailureCooldown retires this
					// contributor from the NULLS FIRST claim head instead
					// of re-burning a subdivision chain every tick (the
					// failing-cohort-dominates-pool class). Best-effort:
					// a lost stamp is one extra retry, and shutdown
					// (historyCanceled) never stamps.
					if merr := s.store.MarkHistoryFetchFailed(ctx, c.ID); merr != nil {
						if errors.Is(merr, context.Canceled) {
							return // shutdown, not a failure — re-claims next tick
						}
						s.logger.Warn("activity history: failure stamp failed — contributor stays at the claim head", "login", c.Login, "error", merr)
					}
				}
			}()
			outcome = s.processHistoryContributor(ctx, fetcher, c, windowDays)
		}(c)
	}
	wg.Wait()
	if ctx.Err() != nil {
		// A canceled cycle never logs "complete" (L14): the counters
		// describe a cut-short sweep, and every unfinished contributor
		// re-claims next tick.
		s.logger.Info("activity history cycle stopped by shutdown",
			"claimed", len(claimed), "stored", stored.Load(), "marked_no_data", marked.Load(), "failed", failed.Load())
		return
	}
	// Log the EFFECTIVE knob values with the throughput so operators
	// can measure config changes directly.
	s.logger.Info("activity history cycle complete",
		"claimed", len(claimed), "stored", stored.Load(), "marked_no_data", marked.Load(), "failed", failed.Load(),
		"window_days", windowDays, "concurrency", concurrency,
		"window_concurrency", s.cfg.Collection.ActivityHistoryWindowConcurrencyValue(),
		"duration", time.Since(start).Truncate(time.Millisecond))
}

// processHistoryContributor handles ONE contributor's meta fetch →
// window fetch → store sequence (the pre-v0.28.3 loop body, verbatim
// semantics — the pool changed WHO runs it, not what it does).
func (s *Scheduler) processHistoryContributor(ctx context.Context, fetcher contributorHistoryFetcher,
	c db.ActivityCheckContributor, windowDays int) activityHistoryOutcome {
	created, years, err := fetcher.FetchContributorHistoryMeta(ctx, c.Login)
	if errors.Is(err, context.Canceled) {
		return historyCanceled // shutdown, not a failure
	}
	if err != nil {
		if errors.Is(err, platform.ErrNotFound) || platform.ClassifyError(err) == platform.ClassSkip {
			// Deleted/renamed account: stamp so the claim head drains.
			// A failed stamp leaves the contributor at the claim head
			// for a pointless re-fetch every tick — log it and count
			// it as a failure so the operator sees the churn (Copilot
			// review, PR #171).
			merr := s.store.MarkHistoryBackfilled(ctx, c.ID)
			if errors.Is(merr, context.Canceled) {
				return historyCanceled
			}
			if merr != nil {
				s.logger.Warn("activity history: mark-only stamp failed — contributor will be re-claimed", "login", c.Login, "error", merr)
				return historyFailed
			}
			return historyMarked
		}
		s.logger.Warn("activity history: meta fetch failed — will retry on next claim", "login", c.Login, "error", err)
		return historyFailed
	}
	windows := github.HistoryWindows(created, years, time.Now().UTC(), windowDays)
	if len(windows) == 0 {
		// Account exists but has zero contribution years: nothing to
		// fetch, ever — stamp it. Same failure treatment as above.
		merr := s.store.MarkHistoryBackfilled(ctx, c.ID)
		if errors.Is(merr, context.Canceled) {
			return historyCanceled
		}
		if merr != nil {
			s.logger.Warn("activity history: zero-years stamp failed — contributor will be re-claimed", "login", c.Login, "error", merr)
			return historyFailed
		}
		return historyMarked
	}
	days, totals, err := fetcher.FetchContributorDailyHistory(ctx, c.Login, windows)
	if errors.Is(err, context.Canceled) {
		return historyCanceled
	}
	if err != nil {
		s.logger.Warn("activity history: fetch failed — will retry on next claim", "login", c.Login, "windows", len(windows), "error", err)
		return historyFailed
	}
	err = s.store.StoreContributorActivityHistory(ctx, c.ID, days, totals)
	if errors.Is(err, context.Canceled) {
		return historyCanceled
	}
	if err != nil {
		s.logger.Warn("activity history: store failed — will retry on next claim", "login", c.Login, "error", err)
		return historyFailed
	}
	return historyStored
}
