// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// gone_recheck.go — v0.29.7: the periodic re-verification of the gone
// cohort. prelim's 404/410 sideline stamps repo_gone_at and DELETES the
// queue row, and the scheduler only ever visits queued repositories —
// so until this ticker existed, a repository that was made private and
// later public again stayed "gone" until an operator ran
// `aveloxis mark-gone-repos` by hand. Organizations do flip
// repositories private and back; it is rare, but it happens.
//
// The verdict rule is the one every consumer of the probe shares
// (prelim, mark-gone-repos, here — SR-16/SR-17):
//
//   - DEFINITIVE 2xx  → ResurrectRepo (clear the stamp + re-enqueue,
//     one transaction).
//   - DEFINITIVE 404/410 → still gone; stamp the check.
//   - anything else — an indeterminate HTTP status (403/429/5xx,
//     unresolved 3xx) OR a transport error after the probe's own four
//     attempts — → still gone; stamp the check anyway. The row was
//     checked and the answer was "unknown"; it keeps its gone state
//     and is retried next CADENCE, not next tick. Both non-definitive
//     arms MUST be bounded the same way: an unstamped row sorts first
//     (NULLS FIRST), so a cohort that never gets a definitive answer
//     — a decommissioned self-hosted GitLab host answering NXDOMAIN
//     forever, a forge-side 429 on the HTML host — would otherwise
//     head every tick's claim and, at ≥ one batch, starve the rest of
//     the gone cohort for good (fresh-context review round 1, MEDIUM;
//     feedback_failing_cohort_dominates_pool). The probe already
//     retries transient network errors over four attempts (13 s of
//     backoff) inside resolveRedirects, so a "transport error" here is
//     not a blip.

package scheduler

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
)

// goneRecheckTick is how often the scheduler looks for due rows. The
// CADENCE is the operator's collection.gone_repo_recheck_days; the
// tick only needs to be short against it so a batch that lands late
// (restart, single-flight skip) is caught up within the hour.
const goneRecheckTick = time.Hour

// goneRecheckBatch bounds probes per tick. Derived from the design
// point, not picked: a 400K-repo fleet with a 300K-repo gone cohort
// (the Department-of-Veterans-Affairs class scaled to the largest
// catalog aveloxis is built for) must cycle inside the default 28-day
// cadence — 300,000 / (28 × 24) ≈ 446 per hour — so 500 leaves the
// whole cohort re-verified once per cadence with headroom. Each probe
// is one unauthenticated HEAD and spends no API-key budget; a batch of
// answering hosts (~200 ms each) finishes in under two minutes of a
// single goroutine, while a batch of hosts that never answer costs the
// probe's full retry ladder per row (13 s for a fast failure such as
// NXDOMAIN, up to 73 s for an i/o timeout — hours per batch). That is
// why the cycle-complete line carries `elapsed`: a cohort like that is
// bounded to once per cadence after review round 1, and the elapsed
// figure is how an operator sees it.
const goneRecheckBatch = 500

// goneProbe is the probe seam. It defaults to the ONE
// redirect-following probe prelim and mark-gone-repos use, so every
// consumer agrees on what "gone" means (SR-17); tests substitute a
// verdict table so no test ever reaches the network.
var goneProbe = collector.ResolveRedirectTarget

// runGoneRecheck is one tick: claim the due rows, probe each, act on
// the verdict. One goroutine, sequential probes — the batch bound
// above is the pacing.
func (s *Scheduler) runGoneRecheck(ctx context.Context) {
	cands, err := s.store.GetGoneRecheckCandidates(ctx, s.cfg.Collection.GoneRepoRecheckInterval(), goneRecheckBatch)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (the v0.27.28 ClassCanceled rule)
		}
		s.logger.Warn("gone recheck: failed to load candidates", "error", err)
		return
	}
	if len(cands) == 0 {
		return
	}
	s.logger.Info("gone recheck cycle starting", "candidates", len(cands),
		"recheck_every", s.cfg.Collection.GoneRepoRecheckInterval())
	started := time.Now()

	var resurrected, stillGone, indeterminate, unreachable, failed int
	stampChecked := func(c db.GoneProbeCandidate) {
		if err := s.store.MarkRepoGoneChecked(ctx, c.RepoID); err != nil && !errors.Is(err, context.Canceled) {
			failed++
			s.logger.Warn("gone recheck: failed to stamp check", "repo_id", c.RepoID, "error", err)
		}
	}
	for _, c := range cands {
		if ctx.Err() != nil {
			return
		}
		_, status, perr := goneProbe(ctx, c.GitURL)
		if perr != nil {
			if errors.Is(perr, context.Canceled) {
				return
			}
			// SR-16: a transport failure is not "no" — the gone state
			// is untouched. The CHECK is stamped (see the header): the
			// row is retried next cadence, never allowed to head every
			// tick.
			unreachable++
			s.logger.Warn("gone recheck: probe failed — repo stays gone, retried next cadence",
				"repo_id", c.RepoID, "url", c.GitURL, "error", perr)
			stampChecked(c)
			continue
		}
		switch {
		case status >= 200 && status < 300:
			if err := s.store.ResurrectRepo(ctx, c.RepoID, 10); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				failed++
				s.logger.Warn("gone recheck: failed to resurrect repo — nothing committed, next tick retries",
					"repo_id", c.RepoID, "url", c.GitURL, "error", err)
				continue
			}
			resurrected++
			s.logger.Info("gone recheck: repository is reachable again — cleared gone state and re-enqueued",
				"repo_id", c.RepoID, "url", c.GitURL)
		case status == http.StatusNotFound || status == http.StatusGone:
			stillGone++
			stampChecked(c)
		default:
			indeterminate++
			s.logger.Warn("gone recheck: indeterminate probe status — repo stays gone, retried next cadence",
				"repo_id", c.RepoID, "url", c.GitURL, "status", status)
			stampChecked(c)
		}
	}

	s.logger.Info("gone recheck cycle complete",
		"candidates", len(cands), "resurrected", resurrected, "still_gone", stillGone,
		"indeterminate", indeterminate, "unreachable", unreachable, "failed", failed,
		"elapsed", time.Since(started).Round(time.Second))
}
