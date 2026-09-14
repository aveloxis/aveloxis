// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// scheduler/mailinglist_wiring.go bridges the v0.25.7 MailingListWorker
// into the scheduler's lifetime. Kept separate so scheduler.go stays
// focused on the core polling loop.

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/hostid"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
)

// mailingListIdleInterval is how long a runner waits before re-polling when
// nothing is eligible (or the source breaker is open). Also the minimum gap
// between successful claims per runner — the v0.21.3 minimum-gap pacing, not
// a throughput cap.
const mailingListIdleInterval = 30 * time.Second

// spawnMailingListWorker starts the MailingListWorker pool for the
// apache_ponymail system. Called from Run only when MailingListEnabled.
//
// All runners share one Pacer and one Breaker so strain on the archive host
// backs off the whole pool and an open breaker pauses every runner
// (dispatcher-pause, §8). The backend is shared too (stateless HTTP).
func (s *Scheduler) spawnMailingListWorker(ctx context.Context) {
	systems, err := mailinglist.LoadSystems()
	if err != nil {
		s.logger.Error("mailing-list: failed to load system definitions — worker not started", "error", err)
		return
	}

	// #9: clear the locks a worker that died mid-scan left behind once they
	// have aged past MailingListStaleLock (the claim query gates on the same
	// window). A worker's own shutdown path releases its lock best-effort
	// (pass 37; the scheduler waits for it before closing the pool, pass
	// 38) — this startup pass is the fallback when that lost.
	n, err := s.store.RecoverStaleListLocks(ctx)
	if errors.Is(err, context.Canceled) {
		return // shutdown, not a failure
	}
	if err != nil {
		s.logger.Warn("mailing-list: stale-lock recovery failed", "error", err)
	} else if n > 0 {
		s.logger.Info("mailing-list: recovered stale list locks", "count", n)
	}

	ua := mailinglist.DefaultUserAgent
	if s.cfg.Collection.MailingListPoliteEmail != "" {
		// #13: validate the polite email so a fat-fingered value doesn't ship
		// a malformed From-contact to archive admins.
		if !strings.Contains(s.cfg.Collection.MailingListPoliteEmail, "@") {
			s.logger.Warn("mailing-list: mailing_list_polite_email is not a valid email address — using default User-Agent",
				"value", s.cfg.Collection.MailingListPoliteEmail)
		} else {
			ua = fmt.Sprintf("Aveloxis/%s (+https://github.com/aveloxis/aveloxis; %s)",
				db.ToolVersion, s.cfg.Collection.MailingListPoliteEmail)
		}
	}

	workers := s.cfg.Collection.MailingListWorkersOrDefault()
	if workers <= 0 {
		workers = 2
	}
	cadence := s.cfg.Collection.MailingListCadenceDuration()
	if cadence <= 0 {
		cadence = db.DefaultMailingListCadence
	}
	pid := os.Getpid()
	// v0.28.18: the REAL kernel boot id (one shared reader with the
	// scancode worker) — what the column name promises. Pre-.18 this was
	// a per-process synthetic (pid-nanos). Informational: nothing
	// compares it (PIDs are namespaced and boot ids host-global under the
	// container deployment, so no same-host PID rule can be built on it;
	// the shutdown release is keyed on the claim's own lock stamp).
	bootID := hostid.BootID()

	// Spawn a worker pool per system definition that has a supported backend
	// (Phase 3: Apache Pony Mail + lore public-inbox). Each pool shares one
	// Pacer/Breaker for its source and only claims lists tagged with its
	// system name, so the kernel and Apache pools never contend.
	spawned := 0
	for _, sys := range systems {
		backend := mailingListBackendFor(sys, ua)
		if backend == nil {
			continue // unsupported backend (future Mailman/etc.)
		}
		pacer := mailinglist.NewPacer(mailinglist.DefaultMinInterval, mailinglist.DefaultMaxInterval)
		breaker := mailinglist.NewBreaker(mailinglist.DefaultCircuitThreshold, mailinglist.DefaultCircuitPause)
		s.logger.Info("mailing-list worker starting",
			"system", sys.Name, "backend", sys.ArchiveBackend, "workers", workers, "cadence", cadence,
			"backfill_months", s.cfg.Collection.MailingListBackfillMonthsOrDefault(), "mirror_handling", s.cfg.Collection.MailingListMirrorHandlingOrDefault())
		for i := 0; i < workers; i++ {
			w := collector.NewMailingListWorker(s.store, sys, backend, pacer, breaker,
				cadence, s.cfg.Collection.MailingListBackfillMonthsOrDefault(),
				pid, bootID, s.logger)
			s.goTracked("mailing-list-worker", func() { s.runMailingListLoop(ctx, w) })
		}

		// The resolve+write half: a MailingListProcessor drains this system's
		// staged messages one list at a time (single-threaded per list,
		// summary/12 §11) so the hot-table writes never reproduce Augur's
		// contention. Default one drain goroutine per system; >1 fans out across
		// DISTINCT lists (the Processor's in-process per-list guard keeps two
		// goroutines off the same list).
		drainWorkers := s.cfg.Collection.MailingListProcessorWorkersOrDefault()
		if drainWorkers <= 0 {
			drainWorkers = 1
		}
		proc := collector.NewMailingListProcessor(s.store, sys.Name, s.cfg.Collection.MailingListMirrorHandlingOrDefault(), sys.ProjectionClean(), s.logger)
		for i := 0; i < drainWorkers; i++ {
			s.goTracked("mailing-list-drain", func() { s.runMailingListDrainLoop(ctx, proc) })
		}

		spawned++
	}
	if spawned == 0 {
		s.logger.Warn("mailing-list: no supported system definitions — no workers started")
		return
	}

	// §5d: periodically re-resolve unresolved mailing-list sender identities
	// against the now-fuller contributors table ("coverage improves over
	// time"). Single goroutine; runs on the same cadence knob as enrichment.
	s.goTracked("mailing-list-sender-backfill", func() { s.runMailingListSenderBackfill(ctx) })

	// Phase 2 (summary/12 §5): for senders the DB-only backfill can't resolve,
	// run them through the shared email→identity chain (Search + global
	// commit-search) and link/create the contributor. Single goroutine.
	s.goTracked("mailing-list-sender-resolve", func() { s.runMailingListSenderResolve(ctx) })
}

// runMailingListSenderResolve config. The min-message threshold (6) is the
// "meaningful participant" cutoff from the 2026-06-04 bootstrap survey
// (summary/12 §5g); the cooldown mirrors the contributor search-resolve
// ticker's 30 days; the batch bounds the search-API spend per tick.
const (
	mailingListSenderResolveInterval    = time.Hour
	mailingListSenderResolveBatch       = 100
	mailingListSenderResolveMinMessages = 6
	mailingListSenderResolveCooldown    = 30 * 24 * time.Hour
)

// runMailingListSenderResolve takes mailing-list senders the DB can't resolve
// (>= the message threshold, past cooldown) and runs each through the shared
// collector.ResolveEmailToIdentity chain. On a hit it links/creates the
// contributor (LinkMailingListSender); the existing sender-backfill ticker then
// stamps the sender's cntrb_id onto their messages rows. Bot/junk senders are
// stamped resolved (terminal) so they leave the candidate pool permanently.
func (s *Scheduler) runMailingListSenderResolve(ctx context.Context) {
	if s.ghClient == nil {
		return // no GitHub client → the API tail can't run; DB backfill still does
	}
	t := time.NewTicker(mailingListSenderResolveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cands, err := s.store.GetMailingListSenderResolveCandidates(ctx,
				mailingListSenderResolveMinMessages, mailingListSenderResolveCooldown.Seconds(), mailingListSenderResolveBatch)
			if errors.Is(err, context.Canceled) {
				return // shutdown, not a failure
			}
			if err != nil {
				s.logger.Warn("mailing-list: sender-resolve candidate query error", "error", err)
				continue
			}
			linked := 0
			created := 0
			for _, c := range cands {
				if ctx.Err() != nil {
					return
				}
				// Bots are never people — terminal stamp so they drop out.
				if collector.IsAutomationEmail(c.SenderEmail) {
					_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, true, "bot", "")
					continue
				}
				login, ghUserID, source, rerr := collector.ResolveEmailToIdentity(ctx, s.store, s.ghClient, c.SenderEmail)
				if rerr != nil {
					// Transient (transport/5xx): stamp the attempt so we back off
					// to the cooldown rather than hammering on a persistent error.
					_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, false, "", "")
					continue
				}
				if login == "" {
					// Phase 4 (§5g 2a): no platform identity found. For a
					// DIRECT-HUMAN sender (not a Jira/GitBox/CI relay), create an
					// email-only contributor so they're attributed and ride the
					// convergence ticker. Bot-relayed senders get no contributor.
					if c.HumanClass && !collector.IsAutomationEmail(c.SenderEmail) {
						createdID, cerr := s.store.CreateEmailOnlyContributor(ctx, c.SenderEmail)
						if errors.Is(cerr, context.Canceled) {
							return // shutdown, not a failure: no attempt stamped
						}
						if cerr != nil {
							s.logger.Warn("mailing-list: email-only contributor create failed", "email", c.SenderEmail, "error", cerr)
							_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, false, "", "")
							continue
						}
						// Code-review round 2026-09-06 (finding 7): ("", nil) is
						// the documented invalid-email outcome (no '@') — NOTHING
						// was created. Stamping it "email-only" would lie about a
						// contributor that does not exist and created++ would
						// over-report. A malformed From header can never become
						// valid, so the terminal stamp is right — but under its
						// honest source, and never counted as a creation.
						if createdID == "" {
							_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, true, "invalid-email", "")
							continue
						}
						_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, true, "email-only", "")
						created++
						continue
					}
					_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, false, "", "")
					continue
				}
				_, lerr := s.store.LinkMailingListSender(ctx, c.SenderEmail, login, ghUserID)
				if errors.Is(lerr, context.Canceled) {
					return // shutdown, not a failure: no attempt stamped
				}
				if lerr != nil {
					s.logger.Warn("mailing-list: sender link failed", "email", c.SenderEmail, "login", login, "error", lerr)
					_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, false, "", "")
					continue
				}
				_ = s.store.MarkSenderResolveAttempt(ctx, c.SenderEmail, true, source, login)
				linked++
			}
			if linked > 0 || created > 0 {
				s.logger.Info("mailing-list: sender resolution pass",
					"linked", linked, "email_only_created", created, "candidates", len(cands))
			}
		}
	}
}

// mailingListSenderBackfillWindow is the msg_id keyset-window width of
// one BackfillMailingListSenderIDs call. 500K measured as an Index Scan
// (~6s/window on the production aveloxis DB); at 2M the planner flips to
// a parallel seq scan.
const mailingListSenderBackfillWindow = int64(500_000)

// mailingListSenderBackfillMaxWindowsPerTick is the window-count
// ceiling per tick; the LOAD-BEARING bound is elapsed time (below) —
// Copilot round 5 on PR #193: 200 windows × ~6 s each is ~20 minutes,
// while the interval knob accepts one minute, so a count-only budget
// let a small knob value queue back-to-back ticks that ran the large
// UPDATEs continuously. A full production pass is ~57 windows today;
// 200 is headroom, not a target.
const mailingListSenderBackfillMaxWindowsPerTick = 200

// mailingListSenderBackfillTickFraction caps one tick's wall-clock at
// this fraction of the configured interval, so lowering the knob
// LOWERS per-tick work instead of monopolizing the database: at the
// 60-minute default a full ~20-minute pass still fits in one tick; at
// a 1-minute interval each tick does ~30 s of windows and the pass
// cursor carries the rest to the next tick.
const mailingListSenderBackfillTickFraction = 2 // interval / N

// mailingListBackendFor builds the ArchiveSource for a system definition,
// or nil for an unsupported backend.
func mailingListBackendFor(sys *mailinglist.System, userAgent string) mailinglist.ArchiveSource {
	switch sys.ArchiveBackend {
	case "apache_ponymail":
		return mailinglist.NewPonyMail(sys.BaseURL, userAgent)
	case "public_inbox":
		return mailinglist.NewPublicInbox(sys.BaseURL, "") // clone dir defaults to temp
	default:
		return nil
	}
}

// runMailingListSenderBackfill walks the sender→cntrb_id backfill in
// keyset windows at the knob-driven cadence
// (collection.mailing_list_sender_backfill_interval_minutes). The pass
// cursor PERSISTS across ticks: if a tick's window budget truncates a
// pass, the next tick resumes where it stopped — restart-from-zero
// would starve the high-msg_id tail forever. A pass ends when the
// cursor clears the ceiling (never on rows-affected — sparse windows
// legally resolve 0); the floor is cached for the process lifetime
// (~17.5s to compute, and it never moves down).
func (s *Scheduler) runMailingListSenderBackfill(ctx context.Context) {
	interval := s.cfg.Collection.MailingListSenderBackfillInterval()
	// SR-10's logging half: the EFFECTIVE cadence, post-default.
	s.logger.Info("mailing-list: sender backfill ticker starting",
		"interval", interval, "window", mailingListSenderBackfillWindow)
	t := time.NewTicker(interval)
	defer t.Stop()
	var (
		floor        int64 // process-lifetime cache (0 = not yet known)
		cursor       int64 // pass cursor — persists across ticks
		passCeil     int64 // 0 = start a fresh pass on the next tick
		passResolved int64
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tickStart := time.Now()
			tickBudget := interval / mailingListSenderBackfillTickFraction
			for w := 0; w < mailingListSenderBackfillMaxWindowsPerTick; w++ {
				if ctx.Err() != nil {
					return
				}
				// The elapsed-time bound (never count alone): the first
				// window always runs; later windows only while the tick
				// is inside its share of the cadence.
				if w > 0 && time.Since(tickStart) >= tickBudget {
					break
				}
				if passCeil == 0 {
					if floor == 0 {
						f, err := s.store.MailingListMsgIDFloor(ctx)
						if errors.Is(err, context.Canceled) {
							return
						}
						if err != nil {
							s.logger.Warn("mailing-list: sender backfill floor query failed", "error", err)
							break
						}
						if f == 0 {
							break // no mailing-list bodies at all yet
						}
						floor = f
					}
					c, err := s.store.MailingListMsgIDCeiling(ctx)
					if errors.Is(err, context.Canceled) {
						return
					}
					if err != nil {
						s.logger.Warn("mailing-list: sender backfill ceiling query failed", "error", err)
						break
					}
					if c == 0 {
						break
					}
					passCeil = c
					cursor = floor - 1
					passResolved = 0
				}
				n, err := s.store.BackfillMailingListSenderIDs(ctx, cursor, mailingListSenderBackfillWindow)
				if errors.Is(err, context.Canceled) {
					return // shutdown, not a failure
				}
				if err != nil {
					// Cursor unchanged — the SAME window retries next tick.
					s.logger.Warn("mailing-list: sender backfill window failed",
						"error", err, "after_msg_id", cursor)
					break
				}
				passResolved += n
				cursor += mailingListSenderBackfillWindow
				if cursor >= passCeil {
					if passResolved > 0 {
						s.logger.Info("mailing-list: resolved sender identities",
							"count", passResolved, "ceiling", passCeil)
					}
					passCeil = 0 // next tick starts a fresh pass
					break
				}
			}
		}
	}
}

// mailingListDrainInterval is how long the drain loop waits before re-checking
// for staged messages when the last pass found nothing to drain.
const mailingListDrainInterval = 30 * time.Second

// mailingListDrainListLimit bounds how many lists one drain pass considers.
const mailingListDrainListLimit = 200

// runMailingListDrainLoop drives one MailingListProcessor: drain every list
// with staged messages (single-threaded per list), idle, repeat.
func (s *Scheduler) runMailingListDrainLoop(ctx context.Context, proc *collector.MailingListProcessor) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := proc.DrainOnce(ctx, mailingListDrainListLimit)
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure
		}
		if err != nil {
			s.logger.Warn("mailing-list: drain cycle error", "error", err)
		}
		if n > 0 {
			s.logger.Info("mailing-list: drained staged messages", "processed", n)
			continue // more may have arrived; keep draining
		}
		t := time.NewTimer(mailingListDrainInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// runMailingListLoop drives one worker: claim+scan a list, repeat. When
// nothing is eligible (or the breaker is open) it idles before re-polling.
func (s *Scheduler) runMailingListLoop(ctx context.Context, w *collector.MailingListWorker) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		claimed, err := w.RunOnce(ctx)
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure
		}
		if err != nil {
			s.logger.Warn("mailing-list: run cycle error", "error", err)
		}
		if claimed {
			continue
		}
		// Nothing to do right now — idle, but wake on shutdown.
		t := time.NewTimer(mailingListIdleInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}
