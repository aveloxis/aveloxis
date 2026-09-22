// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
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
// the GitHub client (the DigestMailer / breadthStore narrow-interface
// pattern).
type contributorActivityFetcher interface {
	// Returns the activity it fetched, the logins whose chunk did NOT
	// complete (their absence from the map says nothing), and an error
	// describing any chunk failures.
	FetchContributorActivity(ctx context.Context, logins []string) (map[string]model.ContributionActivity, []string, error)
}

// Cadence constants — derived, not magic. Batch 2,500 every 15 minutes
// = 240K checks/day = the 2.44M-contributor pool every ~10 days,
// comfortably inside the breadth cooldown the sweep shares (the two
// stay roughly in phase). API cost: 2,500 logins at
// contributorActivityBatchSize (25) per query = 100 GraphQL queries per
// tick ≈ 9,600/day — noise against the pooled 5K-points/hour/token
// GraphQL budget. (This said "100-per-query = 25 queries" until
// v0.29.56; the batch size has been 25 since v0.27.81.)
const (
	activityCheckInterval = 15 * time.Minute
	activityCheckBatch    = 2500
)

// runActivityClassification performs one sweep tick. Failure contract, per
// CHUNK rather than per tick (v0.29.56):
//
//   - A login the fetch got no answer about — its chunk failed, or was
//     never attempted — is UNKNOWN. It is neither classified nor stamped,
//     so it stays at the queue head and retries next tick (a transient
//     GraphQL outage must not stamp 2,500 contributors dataless for a
//     whole cooldown period).
//   - Every row the fetch returned is written, whichever chunk it came
//     from: proven data. Discarding a tick because one chunk failed is why
//     the sweep made no progress at all for three days (7 of 7 ticks on
//     2026-09-17; nobody checked since 2026-09-14).
//   - A contributor ABSENT from a chunk that COMPLETED (deleted or renamed
//     account, per-path NOT_FOUND) is mark-only stamped so they leave the
//     NULLS-FIRST claim head — even when another chunk in the same tick
//     failed. Waiting for a wholly successful tick meant that during a run
//     of partial failures nobody ever retired, and an un-retired cohort
//     pins the head (the v0.20.17 lesson).
func (s *Scheduler) runActivityClassification(ctx context.Context) {
	fetcher, ok := s.ghClient.(contributorActivityFetcher)
	if !ok {
		return // no GitHub GraphQL client (GitLab-only deployment or test fake)
	}
	cooldown := s.cfg.Collection.BreadthCooldownDuration()
	claimed, err := s.store.GetContributorsForActivityCheck(ctx, activityCheckBatch, cooldown)
	if errors.Is(err, context.Canceled) {
		return // shutdown, not a failure
	}
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
	activities, unfetched, fetchErr := fetcher.FetchContributorActivity(ctx, logins)
	if errors.Is(fetchErr, context.Canceled) {
		return // shutdown, not a failure: nothing is stamped, the batch is re-claimed next tick
	}

	// Absence is only meaningful inside a chunk that completed. Marking on
	// the whole batch's success (v0.29.56's first shape) meant one failing
	// chunk per tick stopped every deleted account from ever retiring, and
	// an un-retired cohort of 2,500 pins the NULLS-FIRST claim head (the
	// v0.20.17 lesson).
	updates, absent := planActivityWrites(claimed, activities, unfetched)
	if len(updates) > 0 {
		err := s.store.UpdateContributorActivityBatch(ctx, updates)
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure
		}
		if err != nil {
			s.logger.Warn("activity classification: update failed", "count", len(updates), "error", err)
			return
		}
	}
	// Marking comes BEFORE the fetch-error return: `absent` holds only
	// logins whose own chunk completed, so they are deleted or renamed
	// whatever happened elsewhere in the tick. Returning first (v0.29.56's
	// first shape) made the per-chunk split dead code — with one failing
	// chunk per tick no deleted account ever retired, and an un-retired
	// cohort pins the NULLS-FIRST claim head (the v0.20.17 lesson).
	if len(absent) > 0 {
		err := s.store.MarkActivityCheckedBatch(ctx, absent)
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure
		}
		if err != nil {
			s.logger.Warn("activity classification: mark-absent failed", "count", len(absent), "error", err)
		}
	}
	if fetchErr != nil {
		s.logger.Warn("activity classification: some chunks failed — fetched rows kept, unanswered logins retry next tick",
			"claimed", len(claimed), "classified", len(updates), "retired", len(absent), "error", fetchErr)
		return
	}
	s.logger.Info("activity classification cycle complete",
		"claimed", len(claimed), "classified", len(updates), "absent", len(absent),
		"duration", time.Since(start).Truncate(time.Millisecond))
}

// planActivityWrites splits a fetch result into classified updates (logins
// the fetch returned) and mark-only absentees (logins a COMPLETED chunk did
// not return — deleted or renamed accounts). A login the fetch never got an
// answer for is unknown, not deleted: it is left unstamped and retried.
func planActivityWrites(claimed []db.ActivityCheckContributor, activities map[string]model.ContributionActivity, unfetched []string) (updates []db.ContributorActivityUpdate, absent []string) {
	noAnswer := make(map[string]bool, len(unfetched))
	for _, l := range unfetched {
		noAnswer[l] = true
	}
	for _, c := range claimed {
		act, ok := activities[c.Login]
		if !ok {
			if !noAnswer[c.Login] {
				absent = append(absent, c.ID)
			}
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
	return updates, absent
}
