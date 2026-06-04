// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// scheduler/mailinglist_wiring.go bridges the v0.25.7 MailingListWorker
// into the scheduler's lifetime. Kept separate so scheduler.go stays
// focused on the core polling loop.

package scheduler

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
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

	// #9: clear locks left by a worker that died mid-scan, so those lists are
	// reclaimable immediately rather than after the stale-lock gate.
	if n, err := s.store.RecoverStaleListLocks(ctx); err != nil {
		s.logger.Warn("mailing-list: stale-lock recovery failed", "error", err)
	} else if n > 0 {
		s.logger.Info("mailing-list: recovered stale list locks", "count", n)
	}

	ua := mailinglist.DefaultUserAgent
	if s.cfg.MailingListPoliteEmail != "" {
		// #13: validate the polite email so a fat-fingered value doesn't ship
		// a malformed From-contact to archive admins.
		if !strings.Contains(s.cfg.MailingListPoliteEmail, "@") {
			s.logger.Warn("mailing-list: mailing_list_polite_email is not a valid email address — using default User-Agent",
				"value", s.cfg.MailingListPoliteEmail)
		} else {
			ua = fmt.Sprintf("Aveloxis/%s (+https://github.com/aveloxis/aveloxis; %s)",
				db.ToolVersion, s.cfg.MailingListPoliteEmail)
		}
	}

	workers := s.cfg.MailingListWorkers
	if workers <= 0 {
		workers = 2
	}
	cadence := s.cfg.MailingListCadence
	if cadence <= 0 {
		cadence = db.DefaultMailingListCadence
	}
	pid := os.Getpid()
	bootID := fmt.Sprintf("%d-%d", pid, time.Now().UnixNano())

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
			"backfill_months", s.cfg.MailingListBackfillMonths, "mirror_handling", s.cfg.MailingListMirrorHandling)
		for i := 0; i < workers; i++ {
			w := collector.NewMailingListWorker(s.store, sys, backend, pacer, breaker,
				cadence, s.cfg.MailingListBackfillMonths,
				pid, bootID, s.logger)
			go s.runMailingListLoop(ctx, w)
		}

		// The resolve+write half: a MailingListProcessor drains this system's
		// staged messages one list at a time (single-threaded per list,
		// summary/12 §11) so the hot-table writes never reproduce Augur's
		// contention. Default one drain goroutine per system; >1 fans out across
		// DISTINCT lists (the Processor's in-process per-list guard keeps two
		// goroutines off the same list).
		drainWorkers := s.cfg.MailingListProcessorWorkers
		if drainWorkers <= 0 {
			drainWorkers = 1
		}
		proc := collector.NewMailingListProcessor(s.store, sys.Name, s.cfg.MailingListMirrorHandling, s.logger)
		for i := 0; i < drainWorkers; i++ {
			go s.runMailingListDrainLoop(ctx, proc)
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
	go s.runMailingListSenderBackfill(ctx)
}

// mailingListSenderBackfillInterval is how often unresolved sender→cntrb
// links are retried. Hourly is ample — it only matters as commit resolution
// + search-resolve add identities over days.
const mailingListSenderBackfillInterval = time.Hour
const mailingListSenderBackfillBatch = 5000

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

func (s *Scheduler) runMailingListSenderBackfill(ctx context.Context) {
	t := time.NewTicker(mailingListSenderBackfillInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := s.store.BackfillMailingListSenderIDs(ctx, mailingListSenderBackfillBatch)
			if err != nil {
				s.logger.Warn("mailing-list: sender backfill error", "error", err)
				continue
			}
			if n > 0 {
				s.logger.Info("mailing-list: resolved sender identities", "count", n)
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
