// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// scheduler/distribution_wiring.go bridges the v0.24.0
// DistributionWorker into the scheduler's lifetime. Kept in its own
// file so the main scheduler.go stays focused on the core polling
// loop.

package scheduler

import (
	"context"
	"net/http"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector/distribution"
	gh "github.com/aveloxis/aveloxis/internal/platform/github"

	"github.com/aveloxis/aveloxis/internal/platform/depsdev"
	"github.com/aveloxis/aveloxis/internal/platform/ecosystems"
)

// spawnDistributionWorker constructs and runs a DistributionWorker
// with the production CompositeScanner. Called from Run when
// DistributionTrackingEnabled is true; never called otherwise.
//
// Defaults: 4 runners, 30s start gap, 180-day cadence. Production
// config overrides each via aveloxis.json.
//
// Note: the github client used for distribution evidence is a
// REUSE of the main collection client (s.ghClient). It already has
// the shared KeyPool, ETag cache, retry logic, and rate-limit
// awareness — sharing one client means distribution traffic
// participates in the same fairness pool as the rest of GitHub
// collection rather than competing with a separate budget.
func (s *Scheduler) spawnDistributionWorker(ctx context.Context) {
	workers := s.cfg.Collection.DistributionTrackingWorkersOrDefault()
	if workers <= 0 {
		workers = 4
	}
	startInterval := s.cfg.Collection.DistributionTrackingStartInterval()
	if startInterval <= 0 {
		startInterval = 30 * time.Second
	}
	cadence := s.cfg.Collection.DistributionTrackingInterval()
	if cadence <= 0 {
		cadence = 180 * 24 * time.Hour
	}

	// External-API clients. deps.dev and ecosyste.ms are
	// unauthenticated; they use a plain HTTP client.
	httpClient := &http.Client{Timeout: 30 * time.Second}
	depsDevClient := depsdev.New(depsdev.Options{
		UserAgent:  s.cfg.Collection.DistributionTrackingUserAgent,
		HTTPClient: httpClient,
	})
	ecoClient := ecosystems.New(ecosystems.Options{
		UserAgent:   s.cfg.Collection.DistributionTrackingUserAgent,
		PoliteEmail: s.cfg.Collection.DistributionTrackingPoliteEmail,
		HTTPClient:  httpClient,
	})

	// The github.Client uses the shared KeyPool — we already have
	// s.ghClient on the scheduler. Type-assert to the concrete
	// *github.Client; the platform interface doesn't expose the
	// distribution-tracking methods.
	ghClient, ok := s.ghClient.(*gh.Client)
	if !ok {
		s.logger.Warn("distribution worker: ghClient is not a *github.Client — distribution tracking disabled for this run")
		return
	}

	scanner := distribution.NewCompositeScanner(depsDevClient, ecoClient, ghClient, s.logger)
	// v0.25.0: honor the operator's cross-check setting.
	// NewCompositeScanner defaults to true; we only override when
	// the operator explicitly set it (the cfg field is plumbed
	// through from CollectionConfig.DistributionTrackingCrossCheckSourcesValue()).
	scanner.CrossCheckSources = s.cfg.Collection.DistributionTrackingCrossCheckSourcesValue()

	worker := distribution.NewWorker(distribution.WorkerOptions{
		Store:                   s.store,
		Scanner:                 scanner,
		Workers:                 workers,
		StartInterval:           startInterval,
		Cadence:                 cadence,
		Logger:                  s.logger,
		ImmediatePartialReclaim: s.cfg.Collection.DistributionTrackingImmediatePartialReclaimValue(), // v0.25.3
	})

	s.logger.Info("distribution worker starting",
		"workers", workers,
		"start_interval", startInterval,
		"cadence", cadence,
		"polite_email_set", s.cfg.Collection.DistributionTrackingPoliteEmail != "")

	// Tracked (pass 39): the runners finish their claims on Background
	// contexts after cancel; Run returns once they have, and the
	// scheduler waits for that before closing the pool.
	s.goTracked("distribution-worker", func() { worker.Run(ctx) })
}
