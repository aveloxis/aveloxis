// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_wiring.go — C3: the Jira collector's scheduler wiring, the
// mailing-list shape applied to a tracker. Spawned from Run only when
// collection.jira_enabled; every long-lived goroutine goes through
// goTracked so the shutdown arm waits for post-cancel bookkeeping —
// concretely the worker's bounded-background claim release
// (releaseClaimBestEffort, Copilot round 4 on PR #193) — before
// closing the pgx pool.

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
)

// jiraDrainInterval is the drain loop's idle wait when no staging
// remains (the mailing-list drain cadence).
const jiraDrainInterval = 30 * time.Second

// spawnJiraWorkers starts the fetch pool + one drain loop.
func (s *Scheduler) spawnJiraWorkers(ctx context.Context) {
	workers := s.cfg.Collection.JiraWorkersOrDefault()
	cadence := s.cfg.Collection.JiraCadenceDuration()
	// SR-10's logging half: the EFFECTIVE knob values.
	s.logger.Info("jira: collector starting",
		"workers", workers, "cadence", cadence,
		"polite_email_set", s.cfg.Collection.JiraPoliteEmail != "")
	for i := 0; i < workers; i++ {
		w := collector.NewJiraWorker(s.store, cadence, s.cfg.Collection.JiraPoliteEmail,
			0 /* default page size */, 0, s.logger)
		s.goTracked("jira-worker", func() { w.Run(ctx) })
	}
	p := collector.NewJiraProcessor(s.store, s.logger)
	s.goTracked("jira-drain", func() { s.runJiraDrainLoop(ctx, p) })
}

// runJiraDrainLoop drains staged envelopes, idles when none remain.
func (s *Scheduler) runJiraDrainLoop(ctx context.Context, p *collector.JiraProcessor) {
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := p.DrainOnce(ctx)
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure
		}
		if err != nil {
			s.logger.Warn("jira: drain error", "error", err)
		}
		if n > 0 {
			s.logger.Info("jira: drained staged issues", "count", n)
			continue // more may be waiting
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(jiraDrainInterval):
		}
	}
}
