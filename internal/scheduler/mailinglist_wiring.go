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
	apache := systems["apache_ponymail"]
	if apache == nil {
		s.logger.Error("mailing-list: apache_ponymail system definition missing — worker not started")
		return
	}

	ua := mailinglist.DefaultUserAgent
	if s.cfg.MailingListPoliteEmail != "" {
		ua = fmt.Sprintf("Aveloxis/%s (+https://github.com/aveloxis/aveloxis; %s)",
			db.ToolVersion, s.cfg.MailingListPoliteEmail)
	}
	backend := mailinglist.NewPonyMail(apache.BaseURL, ua)

	pacer := mailinglist.NewPacer(mailinglist.DefaultMinInterval, mailinglist.DefaultMaxInterval)
	breaker := mailinglist.NewBreaker(mailinglist.DefaultCircuitThreshold, mailinglist.DefaultCircuitPause)

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

	s.logger.Info("mailing-list worker starting",
		"system", apache.Name, "workers", workers, "cadence", cadence,
		"backfill_months", s.cfg.MailingListBackfillMonths,
		"mirror_handling", s.cfg.MailingListMirrorHandling,
		"polite_email_set", s.cfg.MailingListPoliteEmail != "")

	for i := 0; i < workers; i++ {
		w := collector.NewMailingListWorker(s.store, apache, backend, pacer, breaker,
			cadence, s.cfg.MailingListBackfillMonths, s.cfg.MailingListMirrorHandling,
			pid, bootID, s.logger)
		go s.runMailingListLoop(ctx, w)
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
