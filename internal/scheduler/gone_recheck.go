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

	"github.com/aveloxis/aveloxis/internal/platform"
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
// cadence — 300,000 / (28 × 24) ≈ 446 per hour — so 500 per tick leaves
// the whole cohort re-verified once per cadence with headroom, PROVIDED
// the batch finishes inside its tick. Each probe is one unauthenticated
// HEAD and spends no API-key budget; a batch of answering hosts (~200 ms
// each) finishes in under two minutes of a single goroutine.
//
// What the design point does NOT cover (Copilot review round 2 on PR
// #203): a host that never answers costs the probe's full retry ladder
// per row — 13 s for a fast failure such as NXDOMAIN, up to 73 s for an
// i/o timeout. A batch of 500 such rows runs ~10 h, singleFlight skips
// the ticks behind it, and throughput falls to ~49/h. Each such row is
// stamped and costs that once per cadence, so the cadence holds while
// N_unanswering × 73 s + N_answering × 0.2 s fits in it: at most
// (gone_repo_recheck_days × 86,400 s) / 73 s never-answering rows —
// ~33,000 at the 28-day default, ~1,200 at 1 day, ~432,000 at 365.
//
// The overrun WARN (goneRecheckOverrun) is NOT that slip signal. It fires
// whenever one cycle outlasts its tick — about 50 never-answering rows in
// a single batch is enough (50 × 73 s > 1 h) — i.e. when a batch ran
// below the per-tick design rate. Whether the cadence actually slips
// depends on how many such rows the whole cohort holds.
//
// Declined at the same review: a concurrent probe pool. The production
// gone cohort is 1,113 repositories, ALL on github.com (chaoss.tv,
// 2026-09-12), and the mark-gone-repos backlog waiting to join it is
// github.com too. Parallel unauthenticated HEADs against one forge's
// HTML host trade a hypothetical dead-host backlog for a real 429
// exposure, and a 429 is an indeterminate verdict — a lost check for a
// whole cadence (see the header). Revisit with per-host lanes (one
// in-flight probe per host, hosts in parallel) if the WARN keeps firing
// AND the never-answering share of the cohort approaches the bound
// above — one dead self-hosted host with 50 repositories fires the WARN
// once per cadence and is harmless.
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

	var resurrected, stillGone, indeterminate, unreachable, failed, refused int
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
		// A gone row whose URL carries credentials is not probed (the HEAD
		// would send them as basic auth) — stamped checked so it waits out
		// the cadence like an unreachable one, never every tick (round 2).
		if uerr := platform.RefuseURLUserinfo(c.GitURL); uerr != nil {
			s.logger.Error("gone recheck: repo URL carries credentials — not probed; correct repo_git",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "error", uerr)
			refused++
			stampChecked(c)
			continue
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
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "error", perr)
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
					"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "error", err)
				continue
			}
			resurrected++
			s.logger.Info("gone recheck: repository is reachable again — cleared gone state and re-enqueued",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL))
		case status == http.StatusNotFound || status == http.StatusGone:
			stillGone++
			stampChecked(c)
		default:
			indeterminate++
			s.logger.Warn("gone recheck: indeterminate probe status — repo stays gone, retried next cadence",
				"repo_id", c.RepoID, "url", platform.RedactURLUserinfo(c.GitURL), "status", status)
			stampChecked(c)
		}
	}

	elapsed := time.Since(started)
	s.logger.Info("gone recheck cycle complete",
		"candidates", len(cands), "resurrected", resurrected, "still_gone", stillGone,
		"indeterminate", indeterminate, "unreachable", unreachable, "refused", refused, "failed", failed,
		"elapsed", elapsed.Round(time.Second))
	if overran, perHour := goneRecheckOverrun(len(cands), elapsed); overran {
		// Observation-only (SR-7): nothing is cancelled or resized.
		s.logger.Warn("gone recheck cycle overran its tick — this batch ran below the per-tick design rate; slow or unreachable hosts dominated it",
			"candidates", len(cands), "unreachable", unreachable, "indeterminate", indeterminate,
			"elapsed", elapsed.Round(time.Second), "tick", goneRecheckTick,
			"probes_per_hour", perHour, "design_probes_per_hour", goneRecheckBatch,
			"recheck_every", s.cfg.Collection.GoneRepoRecheckInterval())
	}
}

// goneRecheckOverrun reports whether a cycle that probed `probed` rows in
// `elapsed` ran longer than its tick — the case in which singleFlight
// skips the ticks behind it and the per-tick batch stops being the
// throughput — and the rate it actually achieved, in probes per hour.
func goneRecheckOverrun(probed int, elapsed time.Duration) (overran bool, perHour int) {
	if probed <= 0 || elapsed <= 0 {
		return false, 0
	}
	perHour = int(int64(probed) * int64(time.Hour) / int64(elapsed))
	return elapsed > goneRecheckTick, perHour
}
