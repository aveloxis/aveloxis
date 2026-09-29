// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// scheduler/distribution_wiring.go bridges the v0.24.0
// DistributionWorker into the scheduler's lifetime. Kept in its own
// file so the main scheduler.go stays focused on the core polling
// loop.

package scheduler

import (
	"context"
	"errors"
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
	opts, err := s.distributionWorkerOptions()
	if err != nil {
		s.logger.Warn("distribution worker: distribution tracking disabled for this run", "error", err)
		return
	}
	if !s.githubKeysAvailable() {
		// Item 21, decided in review round 1: the GitHub source stays ON.
		// With no usable key it answers every scan of a GitHub repository
		// with a non-answer, which FAILS the scan — a strike, quadratic
		// backoff, the 10-strike sideline — and the stored snapshot is kept
		// (the v0.29.55 decision (a)). Dropping the source instead made a
		// registries-only scan COMPLETE: it deleted the repository's
		// GitHub-sourced rows (release assets, packages, every manifest)
		// and stamped it complete for the 180-day cadence. The scanner is
		// GitHub-only (a non-github.com repository completes with no
		// evidence, as always), so on a GitLab-only serve the worker
		// produces nothing: GitHub repositories sideline, by design.
		s.logger.Warn("distribution worker: no usable GitHub API key — GitHub repositories will fail their scans and sideline (their stored snapshots are kept); the scanner is GitHub-only, so this worker produces nothing until a key is added")
	}

	worker := newDistributionWorker(opts)

	s.logger.Info("distribution worker starting",
		"workers", opts.Workers,
		"start_interval", opts.StartInterval,
		"cadence", opts.Cadence,
		"polite_email_set", s.cfg.Collection.DistributionTrackingPoliteEmail != "")

	// Tracked (pass 39): the runners finish their claims on Background
	// contexts after cancel; Run returns once they have, and the
	// scheduler waits for that before closing the pool.
	s.goTracked("distribution-worker", func() { worker.Run(ctx) })
}

// distributionRunner is what the distribution worker is to the scheduler:
// something to Run until the context ends.
type distributionRunner interface {
	Run(ctx context.Context)
}

// newDistributionWorker builds the worker from its options. A seam (the
// sendSignal pattern): TestDistributionWorkerReceivesTheGitHubSource swaps
// it for a recorder and asserts on the options the WORKER receives — the
// property four generations of source pin on the wiring failed to hold
// (review round 5 of items 40/21). The production default is pinned.
var newDistributionWorker = func(o distribution.WorkerOptions) distributionRunner {
	return distribution.NewWorker(o)
}

// errDistributionGitHubClient is returned by distributionWorkerOptions when the
// scheduler's GitHub client is not the concrete *github.Client the scanner
// needs (the platform interface does not expose the distribution methods).
var errDistributionGitHubClient = errors.New("ghClient is not a *github.Client")

// distributionWorkerOptions builds everything the distribution worker runs
// with: the scanner — the unauthenticated deps.dev and ecosyste.ms clients,
// and the scheduler's own GitHub client, always, whatever the key pool
// holds — and the operator's worker count, pacing and cadence. Item 21,
// decided in review round 1: with no usable key the GitHub source answers
// every scan of a GitHub repository with a non-answer, which FAILS the scan
// (a strike, the backoff, the sideline) and keeps the stored snapshot (the
// v0.29.55 decision (a)). Dropping the source instead made a registries-only
// scan COMPLETE, deleting the repository's GitHub-sourced rows for the
// cadence. This seam and newDistributionWorker exist so that property is
// asserted at RUNTIME on the options the worker receives
// (TestDistributionWorkerReceivesTheGitHubSource; review rounds 3–5): every
// source pin on the wiring so far was escaped by a respelling.
func (s *Scheduler) distributionWorkerOptions() (distribution.WorkerOptions, error) {
	ghClient, ok := s.ghClient.(*gh.Client)
	if !ok {
		return distribution.WorkerOptions{}, errDistributionGitHubClient
	}
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
	scanner := distribution.NewCompositeScanner(depsDevClient, ecoClient, ghClient, s.logger)
	// v0.25.0: honor the operator's cross-check setting.
	// NewCompositeScanner defaults to true; we only override when
	// the operator explicitly set it (the cfg field is plumbed
	// through from CollectionConfig.DistributionTrackingCrossCheckSourcesValue()).
	scanner.CrossCheckSources = s.cfg.Collection.DistributionTrackingCrossCheckSourcesValue()
	return distribution.WorkerOptions{
		Store:                   s.store,
		Scanner:                 scanner,
		Workers:                 workers,
		StartInterval:           startInterval,
		Cadence:                 cadence,
		Logger:                  s.logger,
		ImmediatePartialReclaim: s.cfg.Collection.DistributionTrackingImmediatePartialReclaimValue(), // v0.25.3
	}, nil
}
