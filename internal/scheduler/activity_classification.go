// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// activity_classification.go — the v0.27.57 contributor
// activity-classification sweep. GitHub's GraphQL
// contributionsCollection is the only API surface that distinguishes
// "publicly active" / "privately active but disclosed"
// (restrictedContributionsCount) / "dormant" / "no observable
// activity"; the REST events feed the breadth worker uses returns an
// empty list for all non-public states indistinguishably. The sweep
// claims contributors oldest-checked-first (same jittered-cooldown
// contract as breadth), fetches summaries in ~100-login batched
// GraphQL queries, classifies via model.ClassifyContributorActivity,
// and writes the results onto contributors.gh_activity_*.
//
// GITHUB-ONLY: GitLab has no restricted-contributions equivalent
// (private profiles are simply invisible), so the fetch is consumed
// via the narrow capability interface below — satisfied by
// *github.Client, absent on GitLab clients and test fakes, in which
// case the sweep is a no-op. platform.Client is deliberately NOT
// widened.

// contributorActivityFetcher is the capability the sweep needs from
// the GitHub client (the digestMailer / breadthStore narrow-interface
// pattern).
type contributorActivityFetcher interface {
	FetchContributorActivity(ctx context.Context, logins []string) (map[string]model.ContributionActivity, error)
}

// Cadence constants — derived, not magic. Batch 2,500 every 15 minutes
// = 240K checks/day = the 2.44M-contributor pool every ~10 days,
// comfortably inside the breadth cooldown the sweep shares (the two
// stay roughly in phase). API cost: 2,500 logins / 100-per-query = 25
// GraphQL queries per tick ≈ 2,400/day — noise against the pooled
// 5K-points/hour/token GraphQL budget.
const (
	activityCheckInterval = 15 * time.Minute
	activityCheckBatch    = 2500
)

// runActivityClassification performs one sweep tick. Failure contract:
// a failed FETCH marks nothing (the claimed batch stays at the queue
// head and retries next tick — a transient GraphQL outage must not
// stamp 2,500 contributors dataless for a whole cooldown period);
// contributors ABSENT from a successful fetch (deleted/renamed
// accounts, per-path NOT_FOUND) are mark-only stamped so they leave
// the NULLS-FIRST claim head (the v0.20.17 lesson).
func (s *Scheduler) runActivityClassification(ctx context.Context) {
	fetcher, ok := s.ghClient.(contributorActivityFetcher)
	if !ok {
		return // no GitHub GraphQL client (GitLab-only deployment or test fake)
	}
	cooldown := s.cfg.Collection.BreadthCooldownDuration()
	claimed, err := s.store.GetContributorsForActivityCheck(ctx, activityCheckBatch, cooldown)
	if err != nil {
		s.logger.Warn("activity classification: claim failed", "error", err)
		return
	}
	if len(claimed) == 0 {
		return
	}
	logins := make([]string, 0, len(claimed))
	for _, c := range claimed {
		logins = append(logins, c.Login)
	}
	start := time.Now()
	activities, err := fetcher.FetchContributorActivity(ctx, logins)
	if err != nil {
		s.logger.Warn("activity classification: fetch failed — batch will retry next tick", "claimed", len(claimed), "error", err)
		return
	}

	// classification split: present → classified update; absent → mark-only.
	var updates []db.ContributorActivityUpdate
	var absent []string
	for _, c := range claimed {
		act, ok := activities[c.Login]
		if !ok {
			absent = append(absent, c.ID)
			continue
		}
		public := act.PublicContributions()
		lastYear := act.LastContributionYear()
		updates = append(updates, db.ContributorActivityUpdate{
			CntrbID:              c.ID,
			PublicContribs:       public,
			RestrictedContribs:   act.Restricted,
			LastContributionYear: lastYear,
			ActivityClass:        model.ClassifyContributorActivity(public, act.Restricted, lastYear),
		})
	}
	if len(updates) > 0 {
		if err := s.store.UpdateContributorActivityBatch(ctx, updates); err != nil {
			s.logger.Warn("activity classification: update failed", "count", len(updates), "error", err)
			return
		}
	}
	if len(absent) > 0 {
		if err := s.store.MarkActivityCheckedBatch(ctx, absent); err != nil {
			s.logger.Warn("activity classification: mark-absent failed", "count", len(absent), "error", err)
		}
	}
	s.logger.Info("activity classification cycle complete",
		"claimed", len(claimed), "classified", len(updates), "absent", len(absent),
		"duration", time.Since(start).Truncate(time.Millisecond))
}
