// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package scheduler runs continuous collection backed by a Postgres priority queue.
//
// Design goals (contrast with Augur's Celery-based scheduler):
//   - Deterministic ordering: repos are collected in strict priority order.
//   - Priority boost: any repo can be pushed to the top via API or CLI at any time.
//   - Transparent: queue state lives in Postgres, queryable with plain SQL.
//   - Durable: survives restarts. No Celery, no RabbitMQ, no Redis.
//   - Scalable: multiple Aveloxis instances can share the same queue via SKIP LOCKED.
//   - Stale lock recovery: crashed workers' jobs are automatically re-queued.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/safego"
)

// Config configures the scheduler.
type Config struct {
	// Workers is the FLAG-RESOLVED concurrent collection goroutine
	// count: serve's --workers overrides collection.workers in main.go.
	// The one deliberate exception to the no-mirror rule (see below).
	Workers int

	// Scheduler-internal cadences with no aveloxis.json knob.
	PollInterval       time.Duration // how often to check for due jobs (default 10s)
	StaleLockTimeout   time.Duration // how long before reclaiming a locked job (default 1h)
	OrgRefreshInterval time.Duration // how often to re-scan orgs for new/renamed repos (default 4h)

	// Collection is the operator's aveloxis.json `collection` block —
	// THE single source for every runtime knob (v0.25.37, tech-debt
	// Action 1). The scheduler reads values through the CollectionConfig
	// accessors at the point of use so each default/clamp exists in
	// exactly one place. The pre-v0.25.37 45-field mirror struct was a
	// proven incident generator: mailing_list_backfill_months was
	// silently defeated for weeks by a duplicate clamp, and the
	// v0.23.7 scancode immediate-kill default was still being re-clamped
	// 0→30m here when the mirror was removed. Do NOT add per-knob
	// fields to this struct — the mirror-detector tripwire
	// (config_collapse_test.go) fails the build if you do.
	Collection *config.CollectionConfig

	// Mail is the operator's aveloxis.json `mail` block (same
	// single-source pattern as Collection — read through its
	// accessors at the point of use, never mirrored per-knob).
	// Drives the v0.27.12 operator vulnerability digest; nil or an
	// empty OperatorEmail disables it.
	Mail *config.MailConfig
}

// digestMailer is the narrow mailer surface the digest ticker needs
// (v0.25.38 role-interface pattern). *mailer.Mailer satisfies it via
// the SendVulnerabilityDigest adapter in cmd/aveloxis/main.go.
type digestMailer interface {
	SendVulnerabilityDigest(to string, since time.Time, items []db.VulnDigestItem) error
}

// Scheduler polls the Postgres-backed queue and dispatches collection workers.
type Scheduler struct {
	store    *db.PostgresStore
	ghClient platform.Client
	glClient platform.Client
	ghKeys   *platform.KeyPool
	logger   *slog.Logger
	cfg      Config
	workerID string

	// matviewPending is set by the weekly matview ticker and cleared by the
	// rebuild goroutine. The poll loop starts the rebuild once active worker
	// count drops below the ShouldStartMatviewRebuild threshold — see
	// matview_gate.go for the design rationale.
	matviewPending atomic.Bool

	// dbHealthy gates new collection on database availability. The
	// runDBHealthMonitor goroutine probes the DB and flips this; fillWorkerSlots
	// refuses to claim new work while it's false. This turns a Postgres restart
	// (the nightly unattended-upgrades/needrestart event) from a 500K-line
	// connection-error storm + reconnect deadlock pile-up into a clean
	// pause/resume. Starts true (the DB was reachable at startup).
	dbHealthy atomic.Bool

	// apiClaimsPaused tracks the v0.27.34 API-outage claim pause for
	// transition-only logging (the pool logs trip/recovery; this logs
	// the scheduler-side effect exactly once per state change).
	apiClaimsPaused atomic.Bool

	// v0.27.40 single-flight guards for the ticker-fired background
	// tasks (see singleFlight).
	enrichmentActive    atomic.Bool
	activityClassActive atomic.Bool
	activityHistActive  atomic.Bool
	searchActive        atomic.Bool
	affiliationsActive  atomic.Bool
	breadthActive       atomic.Bool
	// v0.27.52: guards the orgRefreshTicker's unscoped full pass.
	// v0.27.83: the poll-tick demand scan (maybeScanNewOrgs) runs
	// under its OWN flag below — sharing this one meant an org
	// registered mid-pass waited for the ENTIRE fleet pass before its
	// ~10s pickup. Overlap between the two passes is safe: every
	// write in the scan path is idempotent (ON CONFLICT), and the
	// demand pass is scoped to never-scanned rows.
	userOrgScanActive   atomic.Bool
	userOrgDemandActive atomic.Bool

	// ghAPIBase is the GitHub REST base URL the org scan enumerates
	// against — "https://api.github.com" in production, an httptest
	// server in the v0.27.83 dedup behavioral suite.
	ghAPIBase string

	// v0.27.35 large-repo skip (collection.skip_largest_percent): the
	// cached top-N% repo_id set excluded from claims, refreshed lazily
	// on a 6-hour horizon (fleet composition changes on collection
	// cadence — days — so anything fresher buys nothing). Guarded by
	// largeSkipMu; empty slice = disabled or nothing qualifies.
	largeSkipMu        sync.Mutex
	largeSkipIDs       []int64
	largeSkipRefreshed time.Time

	// digestMailer + digestStampPath drive the v0.27.12 operator
	// vulnerability digest. mailer is injected via SetDigestMailer
	// from runServe; stampPath defaults to ~/.aveloxis/vuln-digest-last
	// and is overridable in tests.
	digestMailer    digestMailer
	digestStampPath string

	// breadthWorker is constructed ONCE and reused across ticks
	// (v0.27.18). The pre-v0.27.18 runBreadth built a NEW
	// BreadthWorker every tick, so the v0.22.12 circuit-breaker
	// pause (circuitOpenUntil on the worker struct) never persisted
	// across ticks — a GitHub-side 5xx storm re-tripped from scratch
	// every 15 minutes instead of pausing for its full hour.
	breadthWorker *collector.BreadthWorker

	// osvCache (v0.27.21 C0) is the process-wide OSV answer cache
	// shared by every vulnerability scan — the politeness layer the
	// Phase C plan requires before any transitive rollout, and an
	// immediate ~5×/~94× traffic cut on the direct workload.
	osvCache *collector.OSVCache
}

// SetDigestMailer injects the operator-notification mailer (v0.27.12).
// Called by runServe after constructing the Gmail mailer; the digest
// ticker only runs when both this and cfg.Mail.OperatorEmail are set.
func (s *Scheduler) SetDigestMailer(m digestMailer) {
	s.digestMailer = m
}

// New creates a scheduler.
func New(store *db.PostgresStore, ghClient, glClient platform.Client, logger *slog.Logger, cfg Config) *Scheduler {
	return NewWithKeys(store, ghClient, glClient, nil, logger, cfg)
}

// NewWithKeys creates a scheduler with access to the GitHub key pool for commit resolution.
func NewWithKeys(store *db.PostgresStore, ghClient, glClient platform.Client, ghKeys *platform.KeyPool, logger *slog.Logger, cfg Config) *Scheduler {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Second
	}
	if cfg.StaleLockTimeout == 0 {
		cfg.StaleLockTimeout = 1 * time.Hour
	}
	if cfg.OrgRefreshInterval == 0 {
		cfg.OrgRefreshInterval = 4 * time.Hour
	}
	// Collection knobs are NOT defaulted here — their defaults live in
	// exactly one place, the CollectionConfig accessors (v0.25.37).
	// Re-clamping here is how the v0.23.7 scancode immediate-kill
	// default got silently defeated (0 → 30m) for two months.
	if cfg.Collection == nil {
		defaults := config.DefaultConfig().Collection
		cfg.Collection = &defaults
	}

	hostname, _ := os.Hostname()
	workerID := hostname + "-" + time.Now().Format("150405")

	s := &Scheduler{
		store:    store,
		ghClient: ghClient,
		glClient: glClient,
		ghKeys:   ghKeys,
		logger:   logger,
		cfg:      cfg,
		workerID: workerID,
		// Overridable in tests so the org scan can run against an
		// httptest GitHub (v0.27.83 dedup behavioral suite).
		ghAPIBase: "https://api.github.com",
	}

	// Install a permanent-redirect hook on both platform clients so that a
	// 301/308 observed mid-collection surfaces as a WARN log. We do NOT
	// auto-update repos.repo_git here — prelim.RunPrelim already owns
	// repo-rename detection at job start, and mutating repo identity
	// mid-job risks splitting collected rows between old and new names.
	// The log gives operators a signal; automated action is deferred.
	renameHook := func(from, to string) {
		s.logger.Warn("permanent redirect observed during collection — possible repo rename",
			"from", from, "to", to,
			"note", "prelim handles repo renames at job start; this may indicate a rename that occurred mid-collection")
	}
	if ghClient != nil {
		ghClient.OnPermanentRedirect(renameHook)
	}
	if glClient != nil {
		glClient.OnPermanentRedirect(renameHook)
	}

	return s
}

// Run starts the scheduler loop. Blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	s.logger.Info("scheduler started",
		"workers", s.cfg.Workers,
		"poll_interval", s.cfg.PollInterval,
		"recollect_after", s.cfg.Collection.RecollectAfterDuration(),
		"worker_id", s.workerID,
		"force_full_collection", s.cfg.Collection.ForceFullCollection,
	)
	if s.cfg.Collection.ForceFullCollection {
		s.logger.Warn("FORCE FULL COLLECTION enabled — all repos will be fully re-collected. Set collection.force_full to false in aveloxis.json after this pass completes.")
	}

	// v0.27.96 (log-the-effective-value): state the matview schedule the
	// scheduler will ACTUALLY follow. Found live 2026-08-18: the operator
	// set matview_rebuild_day to "disable" (unrecognized pre-v0.27.96),
	// the accessor silently fell back to Saturday, and the 11-hour weekly
	// rebuild kept firing with no runtime signal that the disable hadn't
	// taken effect. This line — and the WARN below — is that signal.
	effectiveDay := s.cfg.Collection.MatviewRebuildWeekday()
	dayName := "DISABLED"
	if effectiveDay >= 0 {
		dayName = time.Weekday(effectiveDay).String()
	}
	s.logger.Info("matview rebuild schedule",
		"configured_value", s.cfg.Collection.MatviewRebuildDay,
		"effective_day", dayName,
		"skip_dm_aggregates", s.cfg.Collection.MatviewRebuildSkipDMAggregates)
	if !s.cfg.Collection.MatviewRebuildDayRecognized() {
		s.logger.Warn("matview_rebuild_day value not recognized — falling back to Saturday; use a weekday name or disabled/disable/none/off",
			"configured_value", s.cfg.Collection.MatviewRebuildDay)
	}

	// On startup: check for tool updates (monthly), then release any
	// stale locks BEFORE processing leftover staging. Lock recovery
	// is a single UPDATE that takes milliseconds; leftover staging
	// can block for many minutes on a realistic backlog. Running
	// lock recovery first gets the queue into a correct state
	// immediately — monitor shows accurate "collecting" counts,
	// orphaned jobs from a crashed prior process stop appearing as
	// in-flight, and the next fillWorkerSlots tick sees reality.
	collector.CheckAndUpdateTools(s.logger)

	// Immediately reclaim all locks held by dead worker IDs. A fresh process
	// cannot have any legitimate in-flight work, so all locks from other
	// worker IDs are definitively stale — no need to wait for the 1-hour
	// timeout. This fixes repos stuck in 'collecting' after a restart.
	if recovered, err := s.store.RecoverOtherWorkerLocks(ctx, s.workerID); err != nil {
		s.logger.Error("failed to recover other workers' locks", "error", err)
	} else if recovered > 0 {
		s.logger.Warn("recovered stale locks from previous process on startup",
			"count", recovered, "current_worker", s.workerID)
	}

	s.recoverStale(ctx)
	s.releaseOurLocks(ctx)

	// Recompute due_at = last_collected + recollectAfter for already-queued
	// rows so a changed days_until_recollect takes effect immediately. Without
	// this, due_at is baked in by CompleteJob under the old setting and stays
	// that way until each repo's next completion — which defeats the point of
	// changing the cooldown in the config.
	//
	// Runs BEFORE processLeftoverStaging (v0.18.26). Previously this was
	// called after the drain, which meant a restart with a non-empty staging
	// backlog silently delayed the realignment by however long ProcessRepo
	// took across every repo with unprocessed rows — minutes to hours on a
	// large fleet. During that window the monitor's Due column showed stale
	// due_at values, and operators reasonably concluded "config change
	// didn't take effect." Realignment is a single UPDATE; it has no data
	// dependency on staging being drained, so it goes first and is visible
	// within seconds of restart. The fillWorkerSlots invariant (no new
	// claims until staging is drained) is still enforced by the explicit
	// call order below.
	if realigned, err := s.store.RealignDueDates(ctx, s.cfg.Collection.RecollectAfterDuration(),
		s.cfg.Collection.ArchivedRecollectMultiplierValue()); err != nil {
		s.logger.Error("failed to realign queue due_at from config", "error", err)
	} else if realigned > 0 {
		// v0.28.1 (A7): log the EFFECTIVE multiplier alongside — the
		// stretch is applied inside the store's due_at writers.
		s.logger.Info("realigned queue due_at from current days_until_recollect",
			"rows_updated", realigned, "recollect_after", s.cfg.Collection.RecollectAfterDuration(),
			"archived_recollect_multiplier", s.cfg.Collection.ArchivedRecollectMultiplierValue())
	}

	// v0.27.39 (summary/18 Phase 2): stranded-repo gauge. Non-archived
	// repos with no queue row are invisible to the scheduler forever
	// (rename-duplicate leftovers from prelim's skip+dequeue, plus
	// lost enqueues). Observation only — consolidating a data-bearing
	// duplicate is a deliberate operator action (reconcile-repos), and
	// auto-enqueueing a rename duplicate would re-collect a duplicate.
	if stranded, serr := s.store.CountStrandedRepos(ctx); serr != nil {
		s.logger.Warn("stranded-repo gauge failed", "error", serr)
	} else if stranded > 0 {
		s.logger.Warn("non-archived repos with no collection_queue row — invisible to the scheduler",
			"count", stranded, "action", "aveloxis reconcile-repos --dry-run")
	}

	// Identify the leftover-staging drain set and lock-park those repos
	// as status='collecting', locked_by='<workerID>:drain' BEFORE
	// launching the background drain. The lock keeps fillWorkerSlots
	// (which only claims status='queued') from racing the goroutine
	// and triggering CollectRepo.PurgeStagedForRepo, which would wipe
	// the in-flight staging rows. As each repo finishes draining, the
	// goroutine releases its lock so it rejoins the queue mid-drain.
	//
	// v0.18.29 change from v0.18.28: the drain used to run synchronously
	// here, blocking fillWorkerSlots for hours on a backlogged fleet
	// (production observed 33+ hours per repo, ~3 days total). Moving
	// it to a goroutine unblocks worker scheduling immediately while
	// keeping data-integrity intact via the lock-park.
	drainSet, lockErr := s.identifyLeftoverDrainSet(ctx)
	if lockErr != nil {
		s.logger.Warn("failed to identify leftover drain set; skipping drain this cycle", "error", lockErr)
	} else if len(drainSet) > 0 {
		locked, lockErr := s.store.LockReposForDrain(ctx, drainSet, s.workerID)
		if lockErr != nil {
			s.logger.Error("failed to lock-park leftover drain set; falling back to synchronous drain to preserve data integrity", "error", lockErr)
			s.processLeftoverStaging(ctx)
		} else if len(locked) > 0 {
			s.logger.Info("launching background leftover-staging drain", "repos", len(locked))
			safego.Go(s.logger, "leftover-staging-drain", func() { s.processLeftoverStagingBackground(ctx, locked) })
		}
	}

	sem := make(chan struct{}, s.cfg.Workers)

	pollTicker := time.NewTicker(s.cfg.PollInterval)
	defer pollTicker.Stop()

	recoveryTicker := time.NewTicker(5 * time.Minute)
	defer recoveryTicker.Stop()

	orgRefreshTicker := time.NewTicker(s.cfg.OrgRefreshInterval)
	defer orgRefreshTicker.Stop()
	// Run org refresh once on startup too.
	safego.Go(s.logger, "org-refresh", func() { s.refreshOrgs(ctx) })

	// Contributor breadth: discovers cross-repo activity for
	// every contributor with a gh_login. v0.20.17: cadence and
	// batch size are now config-driven (default 15min / 2000
	// per tick / 7-day cooldown). The pre-fix hardcoded
	// 6h/100/no-cooldown capped throughput to 400 contribs/day
	// and left zero-event contributors stuck at the queue head
	// forever.
	breadthTicker := time.NewTicker(s.cfg.Collection.BreadthIntervalDuration())
	// v0.27.57: contribution-activity classification sweep (GraphQL
	// contributionsCollection). Cadence constants derived in
	// activity_classification.go.
	activityTicker := time.NewTicker(activityCheckInterval)
	defer activityTicker.Stop()
	// v0.27.58: daily contributor-history sweep (bootstrap + quarterly
	// re-audit in one claim mechanism; constants derived in
	// activity_history.go).
	historyTicker := time.NewTicker(s.cfg.Collection.ActivityHistoryIntervalValue())
	defer historyTicker.Stop()
	defer breadthTicker.Stop()
	// v0.27.18: construct the breadth worker ONCE, here (not lazily in
	// runBreadth, which runs in a per-tick goroutine — lazy init would
	// race). The circuit-breaker pause lives on this struct and now
	// survives across ticks.
	if s.osvCache == nil {
		s.osvCache = collector.NewOSVCache()
	}
	if s.ghKeys != nil && s.breadthWorker == nil {
		s.breadthWorker = collector.NewBreadthWorker(s.store, s.ghKeys, s.logger).
			WithFetchConcurrency(s.cfg.Collection.BreadthFetchConcurrencyOrDefault())
	}

	// v0.21.0 — ScancodeWorker goroutine. Runs its own pool of N
	// scancode runners (default 2, configurable via
	// collection.scancode_workers) with a 6-month default cadence
	// and 90-second start-pacing. Decoupled from the main worker
	// pool so a slow scancode run on one repo can't stall main
	// collection across the fleet. See docs/architecture/scancode.md.
	//
	// v0.27.6: options built via the shared ScancodeOptionsFromConfig
	// mapping so this spawn site and the dedicated
	// `aveloxis scancode-worker` command can never drift.
	//
	// scancode_workers: 0 (EXPLICIT in aveloxis.json — an absent key
	// keeps the DefaultConfig value of 2) disables the pool on this
	// process. That's the dedicated-scancode-host recipe: the primary
	// server sets 0 and the adjacent machine runs
	// `aveloxis scancode-worker` against the shared DB. See
	// docs/guide/dedicated-scancode-host.md. Pre-v0.27.6 an explicit
	// 0 was silently clamped to 2 by the accessor — surprise workers
	// on a host the operator tried to opt out.
	if s.cfg.Collection.ScancodeWorkers == 0 {
		s.logger.Info("scancode worker disabled on this process (scancode_workers: 0) — run `aveloxis scancode-worker` on a dedicated host instead")
	} else {
		scancodeWorker := collector.NewScancodeWorker(
			s.store, s.logger,
			collector.ScancodeOptionsFromConfig(s.cfg.Collection),
		)
		safego.Go(s.logger, "scancode-worker", func() { scancodeWorker.Run(ctx) })
	}

	// v0.24.0 — DistributionWorker goroutine. Off by default; only
	// spawned when collection.distribution_tracking_enabled = true.
	// Records evidence of where each repo is published via package
	// managers (deps.dev, ecosyste.ms, GitHub Packages, release
	// assets) plus in-repo manifest evidence. Independent of every
	// other collection workload; failures here cannot affect main
	// collection.
	if s.cfg.Collection.DistributionTrackingEnabled {
		s.spawnDistributionWorker(ctx)
	}

	// v0.25.7 MailingListWorker: ingests mailing-list archives (Apache
	// Pony Mail) into email_message + messages. Off by default; depends on
	// a populated per-PMC repo_group (load-foundation-orgs). Independent
	// of the per-repo collection pipeline.
	if s.cfg.Collection.MailingListEnabled {
		s.spawnMailingListWorker(ctx)
	}

	// Materialized view rebuild: check hourly, run on Saturdays.
	// Collection is suspended during the rebuild.
	matviewCheckTicker := time.NewTicker(1 * time.Hour)
	defer matviewCheckTicker.Stop()
	var lastMatviewRebuild time.Time

	// Staging table cleanup: delete processed rows older than 7 days
	// so the table doesn't accumulate bloat indefinitely. v0.18.15
	// observed a 21.5M-row table on a long-running deployment
	// (PurgeStagedProcessed was defined but wired to nothing),
	// enough to visibly slow every staging INSERT and DELETE.
	// Hourly keeps the bloat bounded with negligible overhead.
	stagingCleanupTicker := time.NewTicker(1 * time.Hour)
	defer stagingCleanupTicker.Stop()

	// Thin contributor enrichment: runs on a single goroutine at a
	// scheduled cadence (default 30 min). v0.18.29 moved this off the
	// per-job hot path — running it inside runJob meant 120 workers
	// each fired EnrichThinContributors(14000) after their tiny repo
	// finished, attempting ~1.68M REST calls in parallel and
	// exhausting all GitHub keys in ~11 minutes (production verified).
	enrichTicker := time.NewTicker(s.cfg.Collection.EnrichIntervalDuration())
	defer enrichTicker.Stop()

	// v0.27.12: operator vulnerability digest. The ticker fires
	// hourly as a CHECK cadence; runVulnDigest itself enforces the
	// configured interval (default 24h) via the stamp file, sends
	// only when new findings exist, and advances the window
	// monotonically. Disabled entirely (channel stays nil) unless an
	// operator email is configured AND a mailer was injected.
	var vulnDigestC <-chan time.Time
	if s.digestMailer != nil && s.cfg.Mail != nil && s.cfg.Mail.OperatorEmail != "" {
		vulnDigestTicker := time.NewTicker(1 * time.Hour)
		defer vulnDigestTicker.Stop()
		vulnDigestC = vulnDigestTicker.C
		s.logger.Info("operator vulnerability digest enabled",
			"operator_email", s.cfg.Mail.OperatorEmail,
			"min_severity", s.cfg.Mail.VulnDigestMinSeverityOrDefault(),
			"interval", s.cfg.Mail.VulnDigestInterval())
	}

	// v0.19.2: search-resolve background task. Takes contributors
	// with email but no gh_user_id, calls /search/users?q=email at
	// controlled rate (search API is 30/min/token — separate from
	// the core 5000/hour budget), and backfills gh_user_id on
	// successful matches WITHOUT changing cntrb_id or cntrb_login
	// (those would orphan FK refs / trip the partial unique index).
	searchResolveTicker := time.NewTicker(s.cfg.Collection.SearchResolveIntervalDuration())
	defer searchResolveTicker.Stop()

	// v0.19.7: contributor_affiliations population. Was per-job
	// (Phase 5b in runJob), where every of N workers fired
	// PopulateAffiliations after every repo completed. The function
	// scans the global contributors table and INSERTs (ca_domain,
	// company) pairs racing on UNIQUE (ca_domain), producing the
	// ShareLock contention the operator's pg_locks watch caught on
	// 2026-05-08. As a periodic singleton, one writer at a time
	// touches the table — contention disappears. The map only changes
	// when contributor enrichment data changes (bounded by the 30-day
	// cntrb_last_enriched_at cooldown), so hourly cadence is plenty
	// fresh.
	affiliationsTicker := time.NewTicker(s.cfg.Collection.AffiliationIntervalDuration())
	defer affiliationsTicker.Stop()

	// v0.23.0: kick off the repo-metadata backfill in the background.
	// Per operator direction "for those repos already collected, we
	// need to go get that information on the next restart" — this
	// runs once at startup and exits when all repos with empty
	// description+primary_language have been processed. Heavily
	// rate-limited (one FetchRepoInfo per second) so it does not
	// compete with main collection traffic for API budget.
	// Idempotent: each restart re-targets only repos still missing
	// the data, so a partial run from a prior restart picks up
	// where it left off.
	safego.Go(s.logger, "repo-metadata-backfill", func() { s.runRepoMetadataBackfill(ctx) })

	// DB-health monitor: probes the database and pauses collection while it's
	// unavailable (the nightly unattended-upgrades Postgres restart). The DB
	// was reachable at startup (migrate succeeded), so start healthy.
	s.dbHealthy.Store(true)
	safego.Go(s.logger, "db-health-monitor", func() { s.runDBHealthMonitor(ctx) })

	// Immediately fill worker slots on startup instead of waiting for the
	// first poll tick (default 10s). With 30 workers and 78 queued repos,
	// this avoids a visible delay before collection begins.
	s.fillWorkerSlots(ctx, sem)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler stopping, waiting for workers to finish",
				"shutdown_grace", s.cfg.Collection.ShutdownGraceDuration())
			// Drain semaphore to wait for active workers, bounded by
			// ShutdownGrace. Pre-v0.20.0 this loop was unbounded — a
			// single 26-minute commits UPDATE blocked shutdown for the
			// full duration, and any backend that didn't finish in
			// time became an orphan once the parent process exited.
			drained := 0
			deadline := time.After(s.cfg.Collection.ShutdownGraceDuration())
		drain:
			for drained < s.cfg.Workers {
				select {
				case sem <- struct{}{}:
					drained++
				case <-deadline:
					s.logger.Warn("shutdown grace expired, proceeding with pool close",
						"workers_drained", drained, "workers_total", s.cfg.Workers)
					break drain
				}
			}
			// Release queue locks so repos return to 'queued' immediately
			// instead of waiting for stale-lock timeout.
			s.releaseOurLocks(context.Background())
			// Explicitly close the pgx pool so backends disconnect
			// cleanly. Without this, FIN-to-postgres only fires when
			// runServe's defer chain runs — which can miss SIGKILL
			// paths AND leaves connections held by mid-statement worker
			// goroutines grinding for the full TCP-keepalive window.
			// The 2026-05-08 26-minute orphan was the canonical case.
			s.store.Close()
			s.logger.Info("scheduler stopped, locks released, pgx pool closed")
			return

		case <-recoveryTicker.C:
			s.recoverStale(ctx)

		case <-orgRefreshTicker.C:
			safego.Go(s.logger, "org-refresh", func() { s.refreshOrgs(ctx) })
			// Full pass (all orgs) — this is what discovers new repos in
			// long-tracked orgs. Guarded by its own flag so full passes
			// never overlap EACH OTHER; the poll-tick demand scan runs
			// under a separate flag since v0.27.83 (see maybeScanNewOrgs).
			s.singleFlight(&s.userOrgScanActive, "user-org-refresh", func() { s.refreshUserOrgs(ctx, false) })

		case <-breadthTicker.C:
			s.singleFlight(&s.breadthActive, "contributor-breadth", func() { s.runBreadth(ctx) })

		case <-activityTicker.C:
			s.singleFlight(&s.activityClassActive, "activity-classification", func() { s.runActivityClassification(ctx) })

		case <-historyTicker.C:
			s.singleFlight(&s.activityHistActive, "activity-history", func() { s.runActivityHistory(ctx) })

		case <-matviewCheckTicker.C:
			now := time.Now()
			rebuildDay := s.cfg.Collection.MatviewRebuildWeekday()
			// Mark the rebuild as owed; the poll loop starts it once the
			// worker pool has naturally drained below the threshold. This
			// replaces the previous inline call that drained the semaphore
			// and blocked the main goroutine until every in-flight job
			// finished (see matview_gate.go for the incident history).
			if rebuildDay >= 0 && int(now.Weekday()) == rebuildDay && now.Sub(lastMatviewRebuild) > 20*time.Hour {
				if s.matviewPending.CompareAndSwap(false, true) {
					s.logger.Info("matview rebuild queued — will start once active workers drop below threshold",
						"threshold_active_workers", s.cfg.Workers/3, "total_workers", s.cfg.Workers)
				}
			}

		case <-stagingCleanupTicker.C:
			safego.Go(s.logger, "staging-cleanup", func() { s.runStagingCleanup(ctx) })

		case <-enrichTicker.C:
			s.singleFlight(&s.enrichmentActive, "contributor-enrichment", func() { s.runEnrichment(ctx) })

		case <-vulnDigestC:
			safego.Go(s.logger, "vuln-digest", func() { s.runVulnDigest(ctx) })

		case <-searchResolveTicker.C:
			s.singleFlight(&s.searchActive, "search-resolve", func() { s.runSearchResolve(ctx) })

		case <-affiliationsTicker.C:
			s.singleFlight(&s.affiliationsActive, "affiliations-population", func() { s.runAffiliationsPopulation(ctx) })

		case <-pollTicker.C:
			s.fillWorkerSlots(ctx, sem)
			s.maybeStartMatviewRebuild(ctx, sem, &lastMatviewRebuild)
			s.maybeScanNewOrgs(ctx)
		}
	}
}

// runStagingCleanup deletes processed staging rows older than 7 days.
// The DELETE is run in a background goroutine so an unusually slow
// cleanup pass (e.g. just after a first enablement against a table
// with millions of stale rows) does not block the scheduler's main
// poll loop. PurgeStagedProcessed itself is serializable — concurrent
// fires are rare (ticker is hourly, cleanup typically finishes in
// seconds) and at worst race on the same DELETE WHERE, which is
// safe because the predicate is monotonic.
func (s *Scheduler) runStagingCleanup(ctx context.Context) {
	deleted, err := s.store.PurgeStagedProcessed(ctx, s.cfg.Collection.StagingRetentionDuration())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (the v0.27.28 ClassCanceled rule)
		}
		s.logger.Warn("staging cleanup failed", "error", err)
		return
	}
	if deleted > 0 {
		s.logger.Info("staging cleanup complete", "rows_deleted", deleted)
	}

	// Mailing-list staging (summary/12 §11) shares the same retention. The
	// predicate is processed-gated, so undrained no-repo rows are never
	// touched — they persist until the list's repo_group gains a repo and the
	// MailingListProcessor drains them. Always-on (harmless DELETE on an empty
	// table) so leftover processed rows from a prior enablement don't bloat.
	mlDeleted, err := s.store.PurgeMailingListStagingProcessed(ctx, s.cfg.Collection.StagingRetentionDuration().Seconds())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (the v0.27.28 ClassCanceled rule)
		}
		s.logger.Warn("mailing-list staging cleanup failed", "error", err)
		return
	}
	if mlDeleted > 0 {
		s.logger.Info("mailing-list staging cleanup complete", "rows_deleted", mlDeleted)
	}
}

// runSearchResolve runs the v0.19.2 search-resolve background task.
// Takes a batch of contributors with email but no gh_user_id and
// calls /search/users?q=email for each — on hit, backfills the
// platform identity onto the existing row WITHOUT changing
// cntrb_id or cntrb_login. On miss / error, stamps
// cntrb_last_search_attempted_at so the row exits the candidate
// pool until the cooldown elapses.
//
// Batch size is bounded by SearchResolveBatchSize so a single tick
// can't burn through more than a fraction of the search-API quota.
// At default 100 candidates per hour, the task uses ~1.7 search
// requests per minute — comfortable headroom against the 30/min
// per-token budget.
// singleFlight launches task under safego unless a prior run of the
// SAME task is still in flight (v0.27.40, summary/18 Phase 3). The
// scheduler's ticker arms previously spawned a fresh goroutine every
// tick regardless — a slow pass overlapped its successor, double-
// spending API budget (enrichment: up to 14K REST calls/pass) and
// quietly breaking the v0.19.7 "periodic SINGLETON writer" property
// on affiliations. The guard is per-task CAS; a skipped tick logs at
// Debug and the work happens on the next free tick.
func (s *Scheduler) singleFlight(active *atomic.Bool, name string, task func()) {
	if !active.CompareAndSwap(false, true) {
		s.logger.Debug("background task still running — skipping this tick", "task", name)
		return
	}
	safego.Go(s.logger, name, func() {
		defer active.Store(false)
		task()
	})
}

func (s *Scheduler) runSearchResolve(ctx context.Context) {
	if s.ghClient == nil {
		return
	}
	candidates, err := s.store.GetContributorsNeedingSearch(ctx, SearchResolveBatchSize)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (the v0.27.28 ClassCanceled rule)
		}
		s.logger.Warn("search resolve: failed to get candidates", "error", err)
		return
	}
	if len(candidates) == 0 {
		return
	}
	s.logger.Info("search resolve cycle starting", "candidates", len(candidates))

	resolved := 0
	for _, c := range candidates {
		if ctx.Err() != nil {
			return
		}
		login, ghUserID, err := s.ghClient.SearchUserByEmail(ctx, c.Email)
		if err != nil {
			// API failure — stamp the attempt so we don't immediately
			// retry the same email next cycle, and continue.
			_ = s.store.MarkContributorSearchAttempted(ctx, c.CntrbID)
			s.logger.Debug("search resolve: API call failed", "email", c.Email, "error", err)
			continue
		}
		if login == "" || ghUserID == 0 {
			// No hit — stamp so we don't re-search the same email
			// every cycle until the cooldown.
			_ = s.store.MarkContributorSearchAttempted(ctx, c.CntrbID)
			continue
		}
		if err := s.store.LinkContributorToGitHubUser(ctx, c.CntrbID, login, ghUserID); err != nil {
			s.logger.Warn("search resolve: failed to link contributor",
				"cntrb_id", c.CntrbID, "login", login, "error", err)
			continue
		}
		resolved++
	}

	if resolved > 0 {
		s.logger.Info("search resolve cycle complete",
			"resolved", resolved, "of", len(candidates))
	}
}

// SearchResolveBatchSize bounds how many candidates per
// runSearchResolve tick get a search-API call. With a default
// SearchResolveInterval of 1 hour, 100 candidates/hour = ~1.7
// requests/minute — well under the 30/min/token search limit.
const SearchResolveBatchSize = 100

// runEnrichment runs thin-contributor enrichment as a single periodic
// task. Replaces the v0.18.28 per-job EnrichThinContributors call that
// fired from every worker after every repo collection — that pattern
// fanned out to ~120 concurrent EnrichThinContributors(14000) calls and
// burned the GitHub key pool in ~11 minutes.
//
// Picks a single platform client per tick: GitHub when configured
// (matching the production fleet's typical 70+ GitHub keys vs single
// GitLab key); falls back to GitLab if no GitHub client is wired. A
// future iteration could split the enrichment queue per platform if a
// deployment needs symmetric coverage.
func (s *Scheduler) runEnrichment(ctx context.Context) {
	var client platform.Client
	if s.ghClient != nil {
		client = s.ghClient
	} else if s.glClient != nil {
		client = s.glClient
	} else {
		return
	}
	resolver := db.NewContributorResolver(s.store)
	collector.EnrichThinContributors(ctx, s.store, resolver, client, s.logger)
}

// runAffiliationsPopulation runs PopulateAffiliations as a periodic
// singleton task. v0.19.7 replaced the per-job invocation in runJob
// (Phase 5b) with this ticker after the operator's 2026-05-08
// pg_locks watch caught ShareLock contention on the
// `INSERT INTO contributor_affiliations` statement: with 120 workers
// firing the same global table scan + ON CONFLICT race after every
// completed repo, the contention pile-up was visible as
// elevated CPU and intermittent deadlocks. As a singleton, one
// writer at a time touches contributor_affiliations — the contention
// pattern that drove the hotfix disappears. The map only changes
// when contributor enrichment data changes (bounded by the
// 30-day cntrb_last_enriched_at cooldown), so hourly cadence is
// plenty fresh.
func (s *Scheduler) runAffiliationsPopulation(ctx context.Context) {
	count, err := s.store.PopulateAffiliations(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (the v0.27.28 ClassCanceled rule)
		}
		s.logger.Warn("affiliations population failed", "error", err)
		return
	}
	if count > 0 {
		s.logger.Info("affiliations population complete", "rows", count)
	}
}

// fillWorkerSlots fills all available semaphore slots with jobs from the queue.
// Called on startup (immediate) and on every poll tick. Keeps claiming jobs
// until the queue is empty or all worker slots are busy.
//
// Returns immediately without claiming when MatviewRebuildActive is set —
// the weekly refresh wants a quiet window, so no new jobs start while it
// runs. Existing in-flight jobs finish normally; this only gates claims.
// largeRepoExclusions returns the cached "skip the largest repos"
// id set (v0.27.35). Zero-cost when the knob is off. The 6-hour
// refresh horizon is derived, not magic: membership in the top 0.5%
// only changes as repos are re-measured on their multi-day collection
// cadence. Errors keep the previous set (fail-open to stale rather
// than un-skipping the monsters mid-storm).
func (s *Scheduler) largeRepoExclusions(ctx context.Context) []int64 {
	fraction := s.cfg.Collection.SkipLargestFraction()
	if fraction == 0 {
		return nil
	}
	s.largeSkipMu.Lock()
	defer s.largeSkipMu.Unlock()
	if time.Since(s.largeSkipRefreshed) < 6*time.Hour && s.largeSkipIDs != nil {
		return s.largeSkipIDs
	}
	ids, commitTh, prTh, err := s.store.LargestRepoIDs(ctx, fraction)
	if err != nil {
		s.logger.Warn("large-repo skip: refresh failed — keeping previous set",
			"error", err, "previous_count", len(s.largeSkipIDs))
		return s.largeSkipIDs
	}
	if ids == nil {
		ids = []int64{}
	}
	s.largeSkipIDs = ids
	s.largeSkipRefreshed = time.Now()
	// Effective-value log: the thresholds actually in force, not the
	// configured percent.
	s.logger.Info("large-repo skip ACTIVE — excluding largest repos from collection claims",
		"skip_largest_percent", s.cfg.Collection.SkipLargestPercent,
		"repos_excluded", len(ids),
		"commit_count_threshold", int64(commitTh),
		"pr_count_threshold", int64(prTh))
	return ids
}

func (s *Scheduler) fillWorkerSlots(ctx context.Context, sem chan struct{}) {
	if MatviewRebuildActive.Load() {
		return
	}
	// Pause claiming new work while the database is unavailable (e.g. the
	// nightly Postgres restart). The DB-health monitor resumes us when the
	// server returns. Without this, every poll tick would dequeue → fail →
	// log, and the reconnect would trigger a contributor-deadlock storm.
	if !s.dbHealthy.Load() {
		return
	}
	// v0.27.34 GitHub-outage circuit breaker: during a hard API outage
	// (fleet-wide consecutive 5xx, no successes — see
	// platform.APIOutageThreshold) stop CLAIMING new work instead of
	// letting every worker burn a ~5-minute retry budget against a dead
	// gateway (the 2026-07-21 incident: 160 exhausted requests over a
	// 2-hour 502 storm). In-flight jobs keep their own retry/backoff —
	// their first success reopens the breaker instantly. Deliberate
	// simplification: the queue claim isn't platform-filtered, so a
	// GitHub outage also pauses the (tiny) GitLab share of the fleet;
	// splitting claims by platform isn't worth the complexity.
	if !s.ghKeys.APIHealthy() {
		if s.apiClaimsPaused.CompareAndSwap(false, true) {
			s.logger.Warn("pausing new collection claims — GitHub API-outage circuit breaker is open")
		}
		return
	}
	if s.apiClaimsPaused.CompareAndSwap(true, false) {
		s.logger.Info("resuming collection claims — GitHub API-outage circuit breaker closed")
	}
	// v0.27.35: compute once per fill cycle (cached 6h internally).
	excludeLargest := s.largeRepoExclusions(ctx)
	claimed := 0
	for {
		// Check if extra parallelSlots from large-repo collection have pushed
		// us over the configured worker limit. If so, don't start new jobs
		// until the parallel goroutines finish and release their slots.
		extraSlots := int(collector.ParallelSlots.Load())
		if extraSlots > 0 && len(sem)+extraSlots >= s.cfg.Workers {
			if claimed > 0 {
				s.logger.Info("fill cycle complete (parallel slots active)", "claimed", claimed, "active", len(sem), "parallelSlots", extraSlots)
			}
			return
		}
		select {
		case sem <- struct{}{}:
			// Got a worker slot — try to claim a job.
			job, err := s.store.DequeueNext(ctx, s.workerID, excludeLargest)
			if err != nil {
				s.logger.Error("failed to dequeue", "error", err)
				<-sem
				if claimed > 0 {
					s.logger.Info("fill cycle complete (dequeue error)", "claimed", claimed, "active", len(sem))
				}
				return
			}
			if job == nil {
				<-sem // no more work available
				if claimed > 0 {
					s.logger.Info("fill cycle complete (queue empty)", "claimed", claimed, "active", len(sem))
				}
				return
			}
			claimed++
			go func() {
				defer safego.Recover(s.logger, "collection-job")
				defer func() { <-sem }()
				s.runJob(ctx, job)
			}()
		default:
			// All worker slots busy.
			if claimed > 0 {
				s.logger.Info("fill cycle complete (all slots busy)", "claimed", claimed, "active", len(sem))
			}
			return
		}
	}
}

// jobOutcome accumulates results from all collection phases for a single repo.
// It is used internally by runJob to track counts across phases and determine
// the final success/failure status for CompleteJob.
type jobOutcome struct {
	issues       int
	prs          int
	messages     int
	events       int
	releases     int
	contributors int
	commits      int
	success      bool
	errMsg       string
}

func (s *Scheduler) runJob(ctx context.Context, job *db.QueueJob) {
	start := time.Now()

	// Start a heartbeat goroutine that keeps locked_at fresh every 30 seconds.
	// Without this, RecoverStaleLocks (1-hour timeout) steals active jobs from
	// workers collecting large repos (e.g., kubernetes/kubernetes takes 10+ hours).
	// The heartbeat proves the worker is alive — RecoverStaleLocks only reclaims
	// locks where locked_at is older than the timeout.
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	go func() {
		defer safego.Recover(s.logger, "job-heartbeat")
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := s.store.HeartbeatJob(heartbeatCtx, job.RepoID, s.workerID); err != nil {
					s.logger.Warn("heartbeat failed", "repo_id", job.RepoID, "error", err)
				}
			}
		}
	}()

	// Look up the repo to get URL, owner, name, platform.
	repo, err := s.store.GetRepoByID(ctx, job.RepoID)
	if err != nil {
		s.logger.Error("failed to look up repo", "repo_id", job.RepoID, "error", err)
		s.failJob(ctx, job.RepoID, err.Error())
		return
	}

	// v0.22.4 item 7 — observation-only long-jobs watchdog. Polls
	// staging row count for this repo every 30s; if no growth for
	// PhaseWatchdog (default 75m), appends one event to
	// ~/.aveloxis/aveloxis-long-jobs.log + a goroutine dump to
	// ~/.aveloxis/long-jobs/. NEVER cancels this job; the watchdog
	// is purely observational. The defer stop() runs at function
	// return so it survives every job-exit path (success, failure,
	// skip, panic).
	watchdog := &LongJobsWatchdog{
		Logger:    s.logger,
		LogPath:   longJobsLogPath(),
		DumpDir:   longJobsDumpDir(),
		Threshold: s.cfg.Collection.PhaseWatchdogDuration(),
		PollEvery: 30 * time.Second,
		Owner:     repo.Owner,
		Repo:      repo.Name,
		RepoID:    job.RepoID,
		WorkerID:  s.workerID,
		CountFn:   s.store.StagingRowCount,
	}
	stopWatchdog := watchdog.Start(ctx)
	defer stopWatchdog()

	// Prelim phase: check for redirects and duplicates.
	prelim, err := collector.RunPrelim(ctx, s.store, repo, s.logger)
	if err != nil {
		s.logger.Error("prelim failed", "repo_id", job.RepoID, "error", err)
	}
	if prelim != nil && prelim.Skip {
		s.logger.Warn("skipping repo", "repo_id", job.RepoID, "reason", prelim.SkipReason)
		s.skipJob(ctx, job.RepoID, prelim.SkipReason)
		return
	}

	// Generic git repos skip API collection — they only get facade, analysis,
	// scorecard, and SBOM. Commit authors are resolved against both GitHub and
	// GitLab Search APIs to find platform identities.
	var result *collector.CollectResult
	// gapFillErr is hoisted to runJob scope (v0.20.5) so the value
	// produced inside the `if err == nil` gap-fill block survives long
	// enough to be passed into buildOutcome below. Pre-v0.20.5 this was
	// a block-local `gfErr` that got dropped immediately after the WARN
	// log line, leaving last_error NULL and force_full_collect FALSE
	// for every gap-fill failure — exactly the silent-loop class that
	// the v0.18.24 force_full_collect mechanism exists to prevent.
	var gapFillErr error
	// Pass 30 (v0.28.18): a job that fetched listing pages and then FAILED
	// leaves their ETags cached — the retry in this process would be
	// answered 304 on those pages (zero items) while the facade's commits
	// keep the outcome green and last_collected advances past a window
	// that was never stored. On failure, forget this repo's cached ETags.
	var forgetRepoETags func()
	if !repo.Platform.IsGitOnly() {
		client, clientErr := s.selectClient(repo.Platform)
		if clientErr != nil {
			s.logger.Error("unknown platform", "repo_id", job.RepoID, "platform", repo.Platform)
			s.failJob(ctx, job.RepoID, clientErr.Error())
			return
		}
		if f, ok := client.(interface{ ForgetRepoETags(owner, repo string) int }); ok {
			forgetRepoETags = func() {
				if n := f.ForgetRepoETags(repo.Owner, repo.Name); n > 0 {
					s.logger.Info("failed job — forgot the repo's cached listing ETags so the retry re-reads its pages",
						"repo_id", job.RepoID, "owner", repo.Owner, "repo", repo.Name, "etags", n)
				}
			}
		}
		since := s.determineSince(job)
		if since.IsZero() {
			s.logger.Info("full collection (since=zero)", "repo_id", job.RepoID,
				"last_collected", job.LastCollected)
		} else {
			s.logger.Info("incremental collection", "repo_id", job.RepoID,
				"since", since.Format(time.RFC3339), "last_collected", job.LastCollected)
		}
		result, err = s.collectAndProcess(ctx, job.RepoID, repo, client, since)

		// Refresh open items: re-fetch all open issues and PRs to capture
		// status changes (closed, merged), new labels, assignees, reviews, etc.
		// Runs after normal collection so we don't duplicate work for items
		// already updated via the since-based incremental fetch.
		if err == nil {
			refresher := collector.NewOpenItemRefresherWithMode(s.store, client, s.logger, s.cfg.Collection.PRChildMode)
			refresher.RefreshOpenItems(ctx, job.RepoID, repo.Owner, repo.Name)
		}

		// Gap fill: after collectAndProcess, repo_info has fresh metadata counts.
		// Compare gathered vs metadata — if gap >5%, fetch the specific missing
		// items rather than waiting for the next full collection pass.
		if err == nil {
			metaIssues, metaPRs, metaErr := s.store.GetRepoMetaCounts(ctx, job.RepoID)
			if metaErr == nil && (metaIssues > 0 || metaPRs > 0) {
				gf := collector.NewGapFillerWithMode(s.store, client, s.logger, s.cfg.Collection.PRChildMode)
				filled, gfErr := gf.AssessAndFillGaps(ctx, job.RepoID, repo.Owner, repo.Name, metaIssues, metaPRs)
				if gfErr != nil {
					s.logger.Warn("gap fill error", "repo_id", job.RepoID, "error", gfErr)
					// v0.20.5: hoist into runJob scope so buildOutcome
					// records it as outcome.errMsg → last_error in the
					// queue + shouldForceFullRecollect fires.
					gapFillErr = gfErr
				} else if filled > 0 {
					s.logger.Info("gap fill completed", "repo_id", job.RepoID, "filled", filled)
				}
			}
		}
		// v0.18.29: thin-contributor enrichment moved out of runJob into
		// a periodic scheduler-level ticker (see Run's enrichTicker and
		// runEnrichment). With 120 workers each enriching 14000 logins
		// after finishing their tiny repo, the fleet attempted ~1.68M
		// REST calls in parallel windows and exhausted all 73 GitHub
		// keys in ~11 minutes. The periodic-task model runs the
		// enrichment once per cycle, single goroutine, well under the
		// rate-limit budget.
	} else {
		s.logger.Info("git-only repo, skipping API collection", "repo_id", job.RepoID)
	}

	// Phase 3+4: facade then analysis (sequential — analysis needs bare clone).
	facadeResult, analysisResult := s.runFacadeAndAnalysis(ctx, job.RepoID, repo)

	// Phase 5: commit resolution.
	// For generic git repos, attempt resolution on both GitHub and GitLab
	// since we don't know where the contributor identities live.
	s.runCommitResolution(ctx, job.RepoID, repo)

	// v0.19.7: PopulateAffiliations moved out of runJob into a
	// periodic singleton ticker (Run's affiliationsTicker →
	// runAffiliationsPopulation). The per-job invocation produced
	// fan-out contention on UNIQUE (ca_domain) — see the v0.19.7
	// changelog for the production diagnostic that drove the move.

	// Phase 6: SBOM generation.
	s.generateSBOMs(ctx, job.RepoID)

	// Phase 7: Vulnerability scanning via OSV.dev.
	// Uses purls from libyear data to query for known CVEs.
	vulnResult, vulnErr := collector.ScanVulnerabilities(ctx, s.store, job.RepoID, s.logger,
		s.osvCache, s.cfg.Collection.VulnScanTransitiveValue())
	if vulnErr != nil {
		s.logger.Warn("vulnerability scan failed", "repo_id", job.RepoID, "error", vulnErr)
	} else if vulnResult != nil && vulnResult.VulnsFound > 0 {
		s.logger.Info("vulnerabilities found",
			"repo_id", job.RepoID,
			"deps_scanned", vulnResult.TotalDepsScanned,
			"vulns_found", vulnResult.VulnsFound)
	}

	// Determine outcome and complete the job.
	outcome := s.buildOutcome(result, facadeResult, analysisResult, err, gapFillErr)
	duration := time.Since(start)

	// v0.27.139: last_collected anchors at THIS job's start on success
	// (zero on failure — never advanced past data we didn't collect).
	startAnchor := start
	if !outcome.success {
		startAnchor = time.Time{}
	}
	// Pass 31: forget BEFORE CompleteJob — the row flips back to 'queued'
	// there, and a Boost claim in the window would walk the cached pages.
	if !outcome.success && forgetRepoETags != nil {
		forgetRepoETags()
	}
	if err := s.store.CompleteJob(ctx, job.RepoID, outcome.success, startAnchor, s.cfg.Collection.RecollectAfterDuration(),
		outcome.issues, outcome.prs, outcome.messages, outcome.events,
		outcome.releases, outcome.contributors, outcome.commits,
		duration.Milliseconds(), outcome.errMsg,
		s.cfg.Collection.ArchivedRecollectMultiplierValue()); err != nil {
		s.logger.Warn("failed to complete job", "repo_id", job.RepoID, "error", err)
	}

	// Auto-flag repos whose failure class indicates incomplete PR child
	// data. Set AFTER CompleteJob so the flag isn't cleared by the
	// success branch of CompleteJob (which only fires on outcome.success,
	// but keep ordering explicit). The flag is picked up on the repo's
	// next DequeueNext and causes determineSince to return zero for a
	// full re-collection. See v0.18.24 troubleshooting docs.
	if !outcome.success && shouldForceFullRecollect(outcome.errMsg) {
		if err := s.store.SetForceFullCollect(ctx, job.RepoID, true); err != nil {
			s.logger.Warn("failed to set force_full_collect flag", "repo_id", job.RepoID, "error", err)
		} else {
			s.logger.Warn("force_full_recollect set — GraphQL PR batch error class, next cycle will re-collect from since=zero",
				"repo_id", job.RepoID,
				"owner", repo.Owner, "repo", repo.Name,
				"error", outcome.errMsg)
		}
	}

	s.logger.Info("job complete",
		"repo_id", job.RepoID,
		"owner", repo.Owner, "repo", repo.Name,
		"success", outcome.success,
		"duration", duration.Truncate(time.Second),
		"issues", outcome.issues, "prs", outcome.prs,
	)
}

// failJob marks a job as failed with zero counts. Used for early exits
// (repo lookup failure, unknown platform, etc.).
func (s *Scheduler) failJob(ctx context.Context, repoID int64, errMsg string) {
	// v0.27.139: zero startedAt — a failed pass never advances
	// last_collected (due_at still advances for retry pacing).
	if err := s.store.CompleteJob(ctx, repoID, false, time.Time{}, s.cfg.Collection.RecollectAfterDuration(),
		0, 0, 0, 0, 0, 0, 0, 0, errMsg,
		s.cfg.Collection.ArchivedRecollectMultiplierValue()); err != nil {
		s.logger.Warn("failed to record job failure", "repo_id", repoID, "error", err)
	}
}

// skipJob marks a job as successfully completed with zero counts and a reason.
// Used when prelim determines the repo should be skipped (e.g., deleted, duplicate).
func (s *Scheduler) skipJob(ctx context.Context, repoID int64, reason string) {
	// v0.27.139: zero startedAt — a skip collects nothing, so it must
	// not stamp "successfully collected at T" (the cohort-A class:
	// stamped-but-empty passes convert the next round to incremental
	// over history that was never gathered).
	if err := s.store.CompleteJob(ctx, repoID, true, time.Time{}, s.cfg.Collection.RecollectAfterDuration(),
		0, 0, 0, 0, 0, 0, 0, 0, reason,
		s.cfg.Collection.ArchivedRecollectMultiplierValue()); err != nil {
		s.logger.Warn("failed to record job skip", "repo_id", repoID, "error", err)
	}
}

// selectClient returns the platform client for the given platform, or an error
// if the platform is unknown.
func (s *Scheduler) selectClient(p model.Platform) (platform.Client, error) {
	switch p {
	case model.PlatformGitHub:
		return s.ghClient, nil
	case model.PlatformGitLab:
		return s.glClient, nil
	default:
		return nil, fmt.Errorf("unknown platform: %d", p)
	}
}

// determineSince returns the starting point for incremental collection.
// For repos that have never been collected, it returns zero time (full collection).
// For repos previously collected, it returns the stored last_collected —
// which CompleteJob (v0.27.139) anchors at the PREVIOUS ROUND'S START, so
// consecutive rounds tile the timeline with overlap and never a hole.
//
// v0.27.139 (the podman-desktop blind-window incident): the pre-fix
// expression was `time.Now() − recollect_window`, which never read
// last_collected at all. Round N covers up to its own start; round N+1
// began its window at now−D — everything whose FINAL update fell in
// (roundN_start, now−D) was never listed by any round. The blind window
// equals the previous round's duration PLUS however far past due_at the
// fleet backlog delayed the dequeue, and it reopened every cycle
// (verified to the minute on production: missing-item boundaries at
// exactly roundTime−D for two independent rounds). last_collected is the
// only correct lower bound; overlap re-collection is idempotent upserts.
//
// Full-recollect overrides (both return zero time, regardless of LastCollected):
//   - cfg.ForceFullCollection: fleet-wide toggle in aveloxis.json. Used
//     after a systemic bug fix that invalidates collected data.
//   - job.ForceFullCollect: per-repo flag on the queue row. Set
//     automatically by the scheduler when a collection ended with a
//     GraphQL-batch error class that leaves PR child data incomplete
//     (v0.18.24), or manually via `aveloxis recollect <url>`.
func (s *Scheduler) determineSince(job *db.QueueJob) time.Time {
	if s.cfg.Collection.ForceFullCollection {
		return time.Time{} // force full re-collection (fleet-wide)
	}
	if job.ForceFullCollect {
		return time.Time{} // force full re-collection (this repo only)
	}
	if job.LastCollected != nil {
		return *job.LastCollected
	}
	return time.Time{} // zero = full collection
}

// shouldForceFullRecollect returns true when an error message indicates
// the completed job likely left PR child data incomplete (reviews,
// commits, files, comments, assignees, etc. for some subset of PRs). The
// scheduler sets the force_full_collect flag on the repo so the next
// cycle re-collects everything from since=zero, which backfills what the
// failed batch missed.
//
// Pinned to the specific string shapes the GraphQL PR batch path emits —
// intentionally case-sensitive and substring-narrow so unrelated errors
// don't trigger expensive full re-collections. See
// TestShouldForceFullRecollect and TestShouldForceFullRecollect_CaseSensitive
// for the contract.
func shouldForceFullRecollect(errMsg string) bool {
	if errMsg == "" {
		return false
	}
	// All three production shapes share the "graphql PR batch" prefix
	// which the collector and platform layer produce when wrapping the
	// underlying transport/validation/rate failure. Checking this single
	// substring keeps the matcher narrow.
	return strings.Contains(errMsg, "graphql PR batch")
}

// collectAndProcess runs the two-phase staged pipeline: stage raw JSON from
// the API, then process staged data into relational tables with bulk
// contributor resolution.
func (s *Scheduler) collectAndProcess(ctx context.Context, repoID int64, repo *model.Repo, client platform.Client, since time.Time) (*collector.CollectResult, error) {
	sc := collector.NewStagedCollectorWithAllModes(client, s.store, s.logger, s.cfg.Collection.PRChildMode, s.cfg.Collection.ListingMode, s.cfg.Collection.ThreadingMode, s.cfg.Collection.ShardSize).
		WithWorkers(s.cfg.Workers).
		WithIssueChildMode(s.cfg.Collection.IssueChildMode)
	result, err := sc.CollectRepo(ctx, repoID, repo.Owner, repo.Name, since)

	if err == nil {
		proc := collector.NewProcessor(s.store, s.logger)
		if procErr := proc.ProcessRepo(ctx, repoID, int16(repo.Platform)); procErr != nil {
			err = procErr
		}
	}
	return result, err
}

// runFacadeAndAnalysis runs facade (git clone + log) then analysis (deps, libyear,
// scc) sequentially. Analysis depends on the bare clone that facade creates, so
// they cannot run in parallel on the first collection pass for a repo.
func (s *Scheduler) runFacadeAndAnalysis(ctx context.Context, repoID int64, repo *model.Repo) (*collector.FacadeResult, *collector.AnalysisResult) {
	// Phase 3: Facade — creates/updates bare clone and parses git log.
	var facadeResult *collector.FacadeResult
	fc := collector.NewFacadeCollector(s.store, s.logger, s.cfg.Collection.RepoCloneDir)
	// Clone from the repo's OWN stored URL (v0.25.38). The pre-v0.25.38
	// reconstruction via platformHostForModel produced
	// https://unknown/owner/name.git for every GENERIC-GIT repo —
	// breaking facade for the exact platform whose only collection IS
	// facade — and forced github.com/gitlab.com hosts onto
	// enterprise/self-hosted repos. Surfaced by the v0.25.38 runJob
	// lifecycle test on its first execution.
	gitURL := repo.GitURL
	if gitURL == "" {
		gitURL = fmt.Sprintf("https://%s/%s/%s.git",
			platformHostForModel(repo.Platform), repo.Owner, repo.Name)
	}
	result, err := fc.CollectRepo(ctx, repoID, gitURL)
	if err != nil {
		s.logger.Warn("facade collection failed", "repo_id", repoID, "error", err)
		// nil facadeResult is the "facade errored" signal buildOutcome
		// keys on for git-only repos (v0.25.38) — CollectRepo returns a
		// non-nil empty result alongside its error, so normalize here.
		result = nil
	} else if result != nil {
		s.logger.Info("facade complete",
			"repo_id", repoID,
			"commits", result.Commits,
			"commit_messages", result.CommitMessages)
	}
	facadeResult = result

	// GitLab commit_count backfill: GitLab's API commonly reports 0 commits
	// (nil statistics object when the token lacks Reporter+ access, or stale
	// stats cache for freshly-mirrored projects). Now that facade has
	// populated aveloxis_data.commits with the real count, patch the latest
	// repo_info row so the monitor/web "metadata commits" column reflects
	// reality instead of the API-reported zero. GitHub path is unaffected.
	if err == nil && repo.Platform == model.PlatformGitLab {
		if updated, bfErr := s.store.BackfillGitLabCommitCount(ctx, repoID); bfErr != nil {
			s.logger.Warn("gitlab commit_count backfill failed",
				"repo_id", repoID, "error", bfErr)
		} else if updated {
			s.logger.Info("gitlab commit_count backfilled from facade",
				"repo_id", repoID)
		}
	}

	// Phase 4: Analysis — needs the bare clone from facade.
	// RetainClone keeps the temp clone alive for scorecard local execution.
	var analysisResult *collector.AnalysisResult
	ac := collector.NewAnalysisCollector(s.store, s.logger, s.cfg.Collection.RepoCloneDir)
	ac.RetainClone = true
	// v0.27.21 C1: store the full lockfile closure when transitive
	// scanning is on (read at point of use — the v0.25.37 rule).
	ac.TransitiveLockfiles = s.cfg.Collection.VulnScanTransitiveValue()
	ac.DevBuildDeps = s.cfg.Collection.DevBuildDeps
	ac.GitHubActionsDeps = s.cfg.Collection.GitHubActionsDeps
	aResult, aErr := ac.AnalyzeRepo(ctx, repoID)
	if aErr != nil {
		s.logger.Warn("analysis failed", "repo_id", repoID, "error", aErr)
	} else if aResult != nil {
		s.logger.Info("analysis complete",
			"repo_id", repoID,
			"dependencies", aResult.Dependencies,
			"libyear_deps", aResult.LibyearDeps,
			"labor_files", aResult.LaborFiles)
	}
	analysisResult = aResult

	// Phase 4b: OpenSSF Scorecard. Extracted to runScorecardPhase
	// (v0.27.5) — remote-primary for GitHub with a local backstop
	// against the retained temp clone; local-only for GitLab/generic.
	// Determine the local clone path from analysis result.
	localPath := ""
	if analysisResult != nil && analysisResult.ClonePath != "" {
		localPath = analysisResult.ClonePath
	}
	s.runScorecardPhase(ctx, repoID, repo, localPath)

	return facadeResult, analysisResult
}

// runScorecardPhase is Phase 4b of the per-repo pipeline (v0.27.5 —
// extracted from runFacadeAndAnalysis).
//
// Mode order: GitHub repos run REMOTE-primary (--repo, full ~18-check
// set, ~40 API calls measured) with the retained analysis clone as the
// LOCAL backstop on error/timeout (--local, ~11 checks, 0 API calls —
// 11 checks beat none). GitLab and generic-git repos run local only
// (scorecard's GitLab remote support is immature).
//
// Tokens: scorecard receives a comma-separated GITHUB_TOKEN built from
// the pool (collection.scorecard_token_count; 0 = all tokens) and
// round-robins it per request. NO key checkout happens — the
// pre-v0.27.5 GetKey + MarkDepleted pattern is gone: checking one key
// out starved it against 40 collection workers and was the measured
// cause of the multi-DAY remote hangs (scorecard sleeps through the
// single token's rate-limit reset).
//
// The retained temp clone is cleaned up after scorecard finishes,
// regardless of outcome.
func (s *Scheduler) runScorecardPhase(ctx context.Context, repoID int64, repo *model.Repo, analysisClonePath string) {
	repoURL := fmt.Sprintf("https://%s/%s/%s",
		platformHostForModel(repo.Platform), repo.Owner, repo.Name)

	token, instrumentToken := collector.ScorecardTokens(
		s.ghKeys, s.cfg.Collection.ScorecardTokenCountOrDefault())

	_, scErr := collector.RunScorecard(ctx, s.store, repoID, collector.ScorecardOptions{
		RepoURL:         repoURL,
		LocalPath:       analysisClonePath,
		RemotePrimary:   repo.Platform == model.PlatformGitHub,
		Timeout:         s.cfg.Collection.ScorecardTimeout(),
		GithubToken:     token,
		InstrumentToken: instrumentToken,
	}, s.logger)
	if scErr != nil {
		s.logger.Warn("scorecard failed", "repo_id", repoID, "error", scErr)
	}

	// Clean up the retained temp clone now that scorecard is done.
	if analysisClonePath != "" {
		if err := os.RemoveAll(analysisClonePath); err != nil {
			s.logger.Warn("failed to remove retained analysis clone", "path", analysisClonePath, "error", err)
		} else {
			s.logger.Info("removed retained analysis clone after scorecard", "path", analysisClonePath)
		}
	}
}

// runCommitResolution resolves git commit emails to GitHub users.
// Only runs for GitHub repos when API keys are available.
func (s *Scheduler) runCommitResolution(ctx context.Context, repoID int64, repo *model.Repo) {
	if repo.Platform != model.PlatformGitHub || s.ghKeys == nil {
		return
	}

	resolver := collector.NewCommitResolver(s.store, s.ghKeys, s.logger)
	resolveResult, resolveErr := resolver.ResolveCommits(ctx, repoID, repo.Owner, repo.Name)
	if resolveErr != nil {
		s.logger.Warn("commit resolution failed", "repo_id", repoID, "error", resolveErr)
	} else if resolveResult != nil {
		s.logger.Info("commit resolution complete",
			"repo_id", repoID,
			"resolved_api", resolveResult.ResolvedAPI,
			"resolved_noreply", resolveResult.ResolvedNoreply,
			"unresolved", resolveResult.Unresolved)
	}
	// v0.19.7: ResolveEmailsToCanonical removed from the per-job hot
	// path. It selected up to 500 contributors fleet-wide and called
	// GET /users/{login} per row with a 100ms sleep — duplicate work
	// because EnrichContributor (called by the v0.18.29 runEnrichment
	// ticker) populates Canonical from the same endpoint. Keeping it
	// here meant 120 workers each running the same global pass after
	// every job, racing on cntrb_last_enriched_at UPDATEs and burning
	// REST tokens. The function definition stays — operators may call
	// it manually — but the hot-path call site is gone.
}

// buildOutcome evaluates the collection and facade results to determine
// success/failure and extract counts for the job completion record.
//
// gapFillErr (v0.20.5) is folded into the outcome separately from the
// main-collection error so a gap-fill failure produces:
//   - success=false  → shouldForceFullRecollect can fire on the errMsg
//   - errMsg populated with the gap-fill error text → last_error in
//     aveloxis_ops.collection_queue is non-NULL so operators can
//     SQL-query for the affected repos
//
// Before v0.20.5, the gap-fill error was only WARN-logged in runJob
// and dropped before reaching buildOutcome. The result was a silent
// loop: each incremental cycle re-detected the same gap, gap fill
// failed the same way, last_error stayed NULL, force_full_collect
// stayed FALSE.
func (s *Scheduler) buildOutcome(result *collector.CollectResult, facadeResult *collector.FacadeResult, analysisResult *collector.AnalysisResult, collectionErr error, gapFillErr error) jobOutcome {
	out := jobOutcome{success: true}

	if collectionErr != nil {
		out.success = false
		out.errMsg = collectionErr.Error()
	} else if result != nil && len(result.Errors) > 0 {
		out.success = false
		out.errMsg = result.Errors[0].Error()
	}

	// Gap-fill error takes effect only when the main collection
	// succeeded (otherwise the main error message is the more
	// informative one to record). The substring "graphql PR batch"
	// inside gapFillErr.Error() is what shouldForceFullRecollect
	// matches on, so the next cycle re-collects with since=zero and
	// the main collection picks up the historical PRs that gap fill
	// could not.
	if gapFillErr != nil && out.errMsg == "" {
		out.success = false
		out.errMsg = gapFillErr.Error()
	}

	if result != nil {
		out.issues = result.Issues
		out.prs = result.PullRequests
		out.messages = result.Messages
		out.events = result.Events
		out.releases = result.Releases
		out.contributors = result.Contributors
	}

	if facadeResult != nil {
		out.commits = facadeResult.Commits
	}

	// A repo with zero data across all entity types AND zero
	// facade commits likely had an auth failure or is truly empty —
	// mark as failure so it gets retried. v0.20.7 widened the gate
	// to include facade commits: a repo with real git history
	// (out.commits > 0) is provably not empty and provably not
	// auth-failed (facade clones over HTTPS but doesn't go through
	// the API token), so it should NOT be flagged as failure even
	// when API entity counts are all zero. Pre-v0.20.7 this gate
	// wrongly flagged ~100 small-but-real repos like
	// biocorecrg/ggplot2_functions (9 commits, 0 API data) as
	// failures every cycle.
	if result != nil && out.issues == 0 && out.prs == 0 && out.releases == 0 && out.contributors == 0 && out.commits == 0 {
		out.success = false
		if out.errMsg == "" {
			out.errMsg = "no data collected (possible API auth failure or empty repo)"
		}
	}

	// v0.25.38: git-only repos have NO API collection (result == nil), so
	// facade is their only failure signal — and it was previously
	// invisible: the gate above requires result != nil, so a failed
	// facade on a generic-git repo reported SUCCESS with no last_error,
	// every cycle. A nil facadeResult means the clone/log errored; a
	// non-nil result with zero commits is a legitimately empty repo and
	// stays success.
	if result == nil && facadeResult == nil && out.errMsg == "" {
		out.success = false
		out.errMsg = "facade collection failed (git-only repo; see facade warning in log)"
	}

	return out
}

func platformHostForModel(p model.Platform) string {
	switch p {
	case model.PlatformGitHub:
		return "github.com"
	case model.PlatformGitLab:
		return "gitlab.com"
	default:
		return "unknown"
	}
}

// generateSBOMs produces CycloneDX and SPDX SBOMs after collection completes.
// Non-fatal — if SBOM generation fails, collection still succeeds.
func (s *Scheduler) generateSBOMs(ctx context.Context, repoID int64) {
	collector.GenerateAndStoreSBOMs(ctx, s.store, repoID, s.logger)
}

// identifyLeftoverDrainSet returns the set of repo_ids with unprocessed
// staging rows from a previous interrupted run. Used by Run() to feed
// LockReposForDrain before launching the background drain.
func (s *Scheduler) identifyLeftoverDrainSet(ctx context.Context) ([]int64, error) {
	rows, err := s.store.Pool().Query(ctx, `
		SELECT DISTINCT repo_id FROM aveloxis_ops.staging WHERE NOT processed`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var repoIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err == nil {
			repoIDs = append(repoIDs, id)
		}
	}
	return repoIDs, rows.Err()
}

// processLeftoverStaging drains any unprocessed staging rows from a previous
// interrupted run, synchronously. Kept as a fallback path for when the
// drain-park lock fails (so we never lose data — better to block startup
// than to risk PurgeStagedForRepo wiping in-flight rows). The normal path
// is processLeftoverStagingBackground, called as a goroutine from Run().
func (s *Scheduler) processLeftoverStaging(ctx context.Context) {
	repoIDs, err := s.identifyLeftoverDrainSet(ctx)
	if err != nil {
		s.logger.Warn("failed to check for leftover staging rows", "error", err)
		return
	}
	if len(repoIDs) == 0 {
		return
	}

	s.logger.Info("processing leftover staging data from previous run (synchronous fallback)", "repos", len(repoIDs))
	for _, repoID := range repoIDs {
		s.drainOneRepo(ctx, repoID)
	}
}

// processLeftoverStagingBackground is the goroutine entry point for the
// non-blocking drain path. The caller must have already lock-parked
// drainSet via store.LockReposForDrain — those repos are status='collecting'
// with locked_by='<workerID>:drain', invisible to fillWorkerSlots. As each
// repo finishes draining, ReleaseDrainLock returns it to 'queued' so it
// can be picked up for a fresh re-collection without waiting for the
// rest of the drain set to complete.
//
// On context cancel (process shutting down), the loop exits cleanly. Any
// repos still locked stay 'collecting' under the synthetic worker ID;
// the next process startup's RecoverOtherWorkerLocks will release them
// and the drain set will be re-identified and re-parked.
func (s *Scheduler) processLeftoverStagingBackground(ctx context.Context, drainSet []int64) {
	// v0.27.147 (round 26)/v0.27.150 (round 29): heartbeat the WHOLE
	// parked set for the drain's lifetime — the set is lock-parked up
	// front and drained sequentially, so a per-repo beat left the
	// waiting tail's locked_at frozen: one long drain (production has
	// seen ~33h per repo on backlogged staging) let the same process's
	// periodic RecoverStaleLocks (1-hour default) reclaim the tail,
	// which was then drained without a valid lock while routine
	// collection could purge its staging. stop() joins after the loop.
	stopHB := s.store.StartDrainHeartbeat(ctx, s.logger, s.workerID)
	defer stopHB()
	for i, repoID := range drainSet {
		if ctx.Err() != nil {
			s.logger.Info("background drain interrupted by ctx cancel; remaining repos will be re-parked on next startup",
				"remaining", len(drainSet)-i)
			return
		}
		s.drainOneRepo(ctx, repoID)
		if err := s.store.ReleaseDrainLock(ctx, repoID, s.workerID); err != nil {
			s.logger.Warn("failed to release drain lock; repo stays locked until next restart's RecoverOtherWorkerLocks", "repo_id", repoID, "error", err)
		}
	}
	s.logger.Info("background leftover-staging drain complete", "repos", len(drainSet))
}

// drainOneRepo processes one repo's leftover staging rows. Shared by the
// synchronous fallback (processLeftoverStaging) and the background path
// (processLeftoverStagingBackground).
func (s *Scheduler) drainOneRepo(ctx context.Context, repoID int64) {
	repo, err := s.store.GetRepoByID(ctx, repoID)
	if err != nil {
		s.logger.Warn("failed to look up repo for leftover processing", "repo_id", repoID, "error", err)
		return
	}
	proc := collector.NewProcessor(s.store, s.logger)
	if err := proc.ProcessRepo(ctx, repoID, int16(repo.Platform)); err != nil {
		s.logger.Warn("failed to process leftover staging", "repo_id", repoID, "error", err)
		return
	}
	s.logger.Info("processed leftover staging data", "repo_id", repoID)
}

// releaseOurLocks releases all queue locks held by this worker instance,
// returning repos to 'queued' status so they can be picked up immediately
// on restart instead of waiting for stale lock timeout.
func (s *Scheduler) releaseOurLocks(ctx context.Context) {
	tag, err := s.store.Pool().Exec(ctx, `
		UPDATE aveloxis_ops.collection_queue
		SET status = 'queued', locked_by = NULL, locked_at = NULL, due_at = NOW()
		WHERE locked_by = $1 AND status = 'collecting'`, s.workerID)
	if err != nil {
		s.logger.Warn("failed to release locks on shutdown", "error", err)
		return
	}
	if tag.RowsAffected() > 0 {
		s.logger.Info("released queue locks", "count", tag.RowsAffected(), "worker_id", s.workerID)
	}
}

// refreshOrgs re-scans all org/group-type repo groups for new repos and
// checks existing repos for renames. Runs periodically (default every 4h).
func (s *Scheduler) refreshOrgs(ctx context.Context) {
	groups, err := s.store.GetOrgRepoGroups(ctx)
	if err != nil {
		s.logger.Warn("failed to load org repo groups", "error", err)
		return
	}
	if len(groups) == 0 {
		return
	}

	s.logger.Info("refreshing org/group repo lists", "groups", len(groups))

	for _, g := range groups {
		if ctx.Err() != nil {
			return
		}

		var newRepos int
		switch g.Type {
		case "github_org":
			newRepos = s.refreshGitHubOrg(ctx, g)
		case "gitlab_group":
			newRepos = s.refreshGitLabGroup(ctx, g)
		}
		if newRepos > 0 {
			s.logger.Info("new repos discovered in org", "org", g.Name, "new", newRepos)
		}
	}

	// Check existing repos for renames via prelim.
	s.checkForRenames(ctx)
}

func (s *Scheduler) refreshGitHubOrg(ctx context.Context, g db.OrgGroup) int {
	if s.ghKeys == nil {
		return 0
	}
	http := platform.NewHTTPClient("https://api.github.com", s.ghKeys, s.logger, platform.AuthGitHub)

	// Bridge from legacy aveloxis_data.repo_groups to modern
	// aveloxis_ops.user_groups: any user_group whose user_org_requests
	// row points at this org's URL gets every discovered repo linked
	// into aveloxis_ops.user_repos. Hoisted out of the page loop so we
	// pay the lookup once per scan.
	userGroupIDs, ugErr := s.store.GetUserGroupIDsForOrgURL(ctx, g.Website)
	if ugErr != nil {
		s.logger.Warn("failed to look up user_groups for org", "org_url", g.Website, "error", ugErr)
	}

	newCount := 0
	page := 1
	for {
		path := fmt.Sprintf("/orgs/%s/repos?per_page=100&type=all&page=%d", g.Name, page)
		resp, err := http.Get(platform.WithoutETag(ctx), path)
		if err != nil {
			s.logger.Warn("org refresh API error", "org", g.Name, "error", err)
			break
		}
		var items []struct {
			ID      int64  `json:"id"` // v0.27.102 — rename-proof numeric identity
			HTMLURL string `json:"html_url"`
			Name    string `json:"name"`
			Owner   struct {
				Login string `json:"login"`
			} `json:"owner"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			resp.Body.Close()
			break
		}
		resp.Body.Close()

		if len(items) == 0 {
			break
		}
		for _, item := range items {
			// Resolve repo_id — either from the existing catalog row or
			// by inserting a new one. The user_repos linkage step runs
			// in BOTH cases so pre-bridge drift heals on subsequent
			// refresh ticks.
			var repoID int64
			existing, findErr := s.store.FindRepoByURL(ctx, item.HTMLURL)
			if findErr != nil {
				s.logger.Warn("failed to check for existing repo", "url", item.HTMLURL, "error", findErr)
			}
			if existing > 0 {
				repoID = existing
				// v0.27.102: opportunistic forge-ID backfill (fill-empty-
				// only) — see refreshUserOrgs for the rationale.
				if idErr := s.store.SetPlatformRepoIDIfEmpty(ctx, repoID, model.ForgeIDString(item.ID)); idErr != nil {
					s.logger.Warn("failed to backfill platform_repo_id", "repo_id", repoID, "error", idErr)
				}
			} else {
				rid, err := s.store.UpsertRepo(ctx, &model.Repo{
					Platform:   model.PlatformGitHub,
					GitURL:     item.HTMLURL,
					Name:       item.Name,
					Owner:      item.Owner.Login,
					GroupID:    g.ID,
					PlatformID: model.ForgeIDString(item.ID), // v0.27.102
				})
				if err != nil {
					continue
				}
				repoID = rid
				if err := s.store.EnqueueRepo(ctx, repoID, 100); err != nil {
					continue
				}
				s.logger.Info("new repo discovered", "org", g.Name, "repo", item.HTMLURL)
				newCount++
				// §5c repo-side resolution: a mailing-list message may have
				// signaled this repo before it was in the catalog. Backfill
				// any waiting email_message.signaled_repo_id now.
				if n, rerr := s.store.ResolveSignaledRepoForURL(ctx, repoID, item.HTMLURL); rerr == nil && n > 0 {
					s.logger.Info("resolved signaled_repo for new repo", "repo", item.HTMLURL, "messages", n)
				}
			}
			for _, gid := range userGroupIDs {
				if _, err := s.store.AddRepoToGroupByID(ctx, gid, repoID); err != nil {
					s.logger.Warn("failed to link discovered repo into user_repos",
						"group_id", gid, "repo_id", repoID, "error", err)
				}
			}
		}
		page++
	}
	return newCount
}

func (s *Scheduler) refreshGitLabGroup(ctx context.Context, g db.OrgGroup) int {
	// Use the gitlab client's base URL or derive from the website URL.
	glHost := "gitlab.com"
	if u, err := url.Parse(g.Website); err == nil && u.Host != "" {
		glHost = u.Host
	}
	// Need GitLab keys — check if the glClient is available.
	// We'll reuse the ghKeys pool for now; in practice GitLab keys are separate.
	// TODO: pass glKeys to the scheduler for GitLab org refresh.
	http := platform.NewHTTPClient("https://"+glHost+"/api/v4", s.ghKeys, s.logger, platform.AuthGitLab)

	// Same legacy → user_groups bridge as the GitHub path.
	userGroupIDs, ugErr := s.store.GetUserGroupIDsForOrgURL(ctx, g.Website)
	if ugErr != nil {
		s.logger.Warn("failed to look up user_groups for group", "org_url", g.Website, "error", ugErr)
	}

	newCount := 0
	page := 1
	encodedGroup := url.PathEscape(g.Name)
	for {
		path := fmt.Sprintf("/groups/%s/projects?per_page=100&include_subgroups=true&page=%d", encodedGroup, page)
		resp, err := http.Get(platform.WithoutETag(ctx), path)
		if err != nil {
			s.logger.Warn("group refresh API error", "group", g.Name, "error", err)
			break
		}
		var items []struct {
			ID        int64  `json:"id"` // v0.27.102 — rename-proof numeric identity
			WebURL    string `json:"web_url"`
			Name      string `json:"name"`
			Namespace struct {
				FullPath string `json:"full_path"`
			} `json:"namespace"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			resp.Body.Close()
			break
		}
		resp.Body.Close()

		if len(items) == 0 {
			break
		}
		for _, item := range items {
			var repoID int64
			existing, findErr := s.store.FindRepoByURL(ctx, item.WebURL)
			if findErr != nil {
				s.logger.Warn("failed to check for existing repo", "url", item.WebURL, "error", findErr)
			}
			if existing > 0 {
				repoID = existing
				// v0.27.102: opportunistic forge-ID backfill (fill-empty-
				// only) — see refreshUserOrgs for the rationale.
				if idErr := s.store.SetPlatformRepoIDIfEmpty(ctx, repoID, model.ForgeIDString(item.ID)); idErr != nil {
					s.logger.Warn("failed to backfill platform_repo_id", "repo_id", repoID, "error", idErr)
				}
			} else {
				rid, err := s.store.UpsertRepo(ctx, &model.Repo{
					Platform:   model.PlatformGitLab,
					GitURL:     item.WebURL,
					Name:       item.Name,
					Owner:      item.Namespace.FullPath,
					GroupID:    g.ID,
					PlatformID: model.ForgeIDString(item.ID), // v0.27.102
				})
				if err != nil {
					continue
				}
				repoID = rid
				if err := s.store.EnqueueRepo(ctx, repoID, 100); err != nil {
					continue
				}
				s.logger.Info("new repo discovered", "group", g.Name, "repo", item.WebURL)
				newCount++
				if n, rerr := s.store.ResolveSignaledRepoForURL(ctx, repoID, item.WebURL); rerr == nil && n > 0 {
					s.logger.Info("resolved signaled_repo for new repo", "repo", item.WebURL, "messages", n)
				}
			}
			for _, gid := range userGroupIDs {
				if _, err := s.store.AddRepoToGroupByID(ctx, gid, repoID); err != nil {
					s.logger.Warn("failed to link discovered repo into user_repos",
						"group_id", gid, "repo_id", repoID, "error", err)
				}
			}
		}
		page++
	}
	return newCount
}

// checkForRenames runs prelim on a sample of repos to detect renames/transfers.
// Checks repos that haven't been collected recently — they're the most likely
// to have gone stale.
func (s *Scheduler) checkForRenames(ctx context.Context) {
	repos, err := s.store.GetReposForRenameCheck(ctx, 50)
	if err != nil {
		s.logger.Warn("failed to load repos for rename check", "error", err)
		return
	}
	for _, repo := range repos {
		if ctx.Err() != nil {
			return
		}
		prelim, err := collector.RunPrelim(ctx, s.store, &repo, s.logger)
		if err != nil {
			continue
		}
		if prelim != nil && (prelim.Skip || prelim.Redirected) {
			s.logger.Info("rename check result",
				"repo_id", repo.ID, "url", repo.GitURL,
				"skip", prelim.Skip, "redirected", prelim.Redirected,
				"reason", prelim.SkipReason, "new_url", prelim.NewURL)
		}
	}
}

// maybeStartMatviewRebuild starts the weekly rebuild in a goroutine when one
// is owed (matviewPending) and the worker pool has naturally drained below
// the ShouldStartMatviewRebuild threshold. The rebuild itself runs
// concurrently with any remaining in-flight collections — REFRESH
// MATERIALIZED VIEW CONCURRENTLY doesn't block reads, and
// MatviewRebuildActive prevents fillWorkerSlots from claiming new jobs.
func (s *Scheduler) maybeStartMatviewRebuild(ctx context.Context, sem chan struct{}, lastRebuild *time.Time) {
	if !s.matviewPending.Load() {
		return
	}
	// Already running — another poll tick fired while the rebuild goroutine
	// is still in flight. The goroutine will clear both flags on completion.
	if MatviewRebuildActive.Load() {
		return
	}
	if !ShouldStartMatviewRebuild(len(sem), s.cfg.Workers) {
		return
	}
	// Claim the rebuild. CAS guarantees only one goroutine wins.
	if !MatviewRebuildActive.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer safego.Recover(s.logger, "matview-rebuild")
		defer MatviewRebuildActive.Store(false)
		defer s.matviewPending.Store(false)
		s.rebuildMatviews(ctx)
		*lastRebuild = time.Now()
	}()
}

// rebuildMatviews refreshes the materialized views and the dm_ aggregate
// tables. Callers must set MatviewRebuildActive before invoking so that
// fillWorkerSlots gates new job claims for the duration; rebuildMatviews
// itself does not touch the worker semaphore.
//
// Replaces the pre-v0.17.1 implementation that drained every worker slot via
// `for range s.cfg.Workers { sem <- struct{}{} }`. That pattern blocked the
// scheduler's main goroutine for the duration of the longest in-flight
// collection — a single 10+ hour parallel-mode job (meshery, 11K+ PRs)
// froze claims for 9 hours on 2026-04-18.
func (s *Scheduler) rebuildMatviews(ctx context.Context) {
	s.logger.Info("weekly matview rebuild: starting (MatviewRebuildActive=true, new claims paused)",
		"active_workers_at_start", "see monitor banner")

	start := time.Now()
	// A `stop serve` inside the multi-hour (views) or multi-day (dm_)
	// pass is the common shape — shutdown is not a failure (the v0.27.28
	// ClassCanceled rule); the rebuild is owed again on the next
	// rebuild day.
	if err := db.RefreshMaterializedViews(ctx, s.store, s.logger); errors.Is(err, context.Canceled) {
		s.logger.Info("weekly matview rebuild canceled by shutdown — owed again on the next rebuild day", "elapsed", time.Since(start).Truncate(time.Second))
		return
	} else if err != nil {
		s.logger.Error("weekly matview rebuild failed", "error", err)
	} else {
		s.logger.Info("weekly matview rebuild complete", "duration", time.Since(start).Truncate(time.Second))
	}

	// Refresh dm_ aggregate tables (dm_repo_annual/monthly/weekly and
	// dm_repo_group variants). These aggregate commit data by email,
	// affiliation, and time period. v0.27.56: skippable by config —
	// the per-repo loop ran 3+ days on the production fleet
	// (2026-07-27→30) while MatviewRebuildActive held all collection
	// claims paused; operators can now keep the weekly matview step
	// and refresh dm_ tables only via `aveloxis refresh-views --aggregates`.
	if s.cfg.Collection.MatviewRebuildSkipDMAggregates {
		s.logger.Info("dm_ aggregate refresh skipped by config (matview_rebuild_skip_dm_aggregates=true) — dm_ tables update only via `aveloxis refresh-views --aggregates`")
	} else {
		aggStart := time.Now()
		if err := s.store.RefreshAllRepoAggregates(ctx, s.logger); errors.Is(err, db.ErrAggregateRebuildRunning) {
			// An operator `refresh-views --aggregates` overlapping the
			// weekly tick is expected, not a failure; the next tick retries.
			s.logger.Warn("dm_ aggregate refresh skipped — another pass holds the aggregate lock; the next weekly tick retries", "error", err)
		} else if errors.Is(err, context.Canceled) {
			s.logger.Info("dm_ aggregate refresh canceled by shutdown — owed again on the next rebuild day", "elapsed", time.Since(aggStart).Truncate(time.Second))
			return
		} else if err != nil {
			s.logger.Error("dm_ aggregate refresh failed", "error", err)
		} else {
			s.logger.Info("dm_ aggregate refresh complete", "duration", time.Since(aggStart).Truncate(time.Second))
		}
	}

	s.logger.Info("weekly matview rebuild: collection will resume on next poll tick")
}

// maybeScanNewOrgs runs on every poll tick (default 10s). A
// user_org_requests row with last_scanned IS NULL is the
// cross-process signal that an org was just registered — an admin
// added it directly (portal API or web GUI), or an admin approved a
// pending org request (DecideAddRequest inserts the registration).
// Instead of waiting up to 4 hours for orgRefreshTicker, launch an
// immediate scan scoped to the never-scanned orgs so already-tracked
// repos (e.g. a fully-collected org added to a second group) link
// into the new group within seconds of the add/approval (v0.27.52).
//
// Non-admin adds never fire this: their orgs pend in
// collection_add_requests, and no user_org_requests row exists until
// an admin approves (v0.27.20). Rejected-group orgs are excluded by
// the probe itself — the scan's rejected gate deliberately never
// stamps them, so counting them would re-fire the probe every tick.
// A failed enumeration is also safe: MarkOrgRequestScanned stamps
// unconditionally per attempt, so retries fall to the 4h cadence
// instead of looping here.
func (s *Scheduler) maybeScanNewOrgs(ctx context.Context) {
	// Same gate as fillWorkerSlots: while the database is unavailable
	// (nightly Postgres restart), skip silently instead of producing a
	// probe-failed WARN on every 10s poll tick.
	if !s.dbHealthy.Load() {
		return
	}
	pending, err := s.store.HasNeverScannedOrgs(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (the v0.27.28 ClassCanceled rule)
		}
		s.logger.Warn("never-scanned-orgs probe failed", "error", err)
		return
	}
	if !pending {
		return
	}
	// v0.27.83: the demand scan runs under its OWN single-flight flag.
	// Sharing userOrgScanActive with the 4h full pass meant a new
	// registration made mid-pass waited for the entire fleet pass
	// before its quick pickup — a first-experience regression that
	// grows with fleet size. Overlapping the full pass is safe: every
	// write in the scan path is idempotent, and this pass is scoped to
	// never-scanned rows so duplicate enumeration is bounded to orgs
	// registered while a full pass happens to be running.
	s.singleFlight(&s.userOrgDemandActive, "user-org-demand-scan", func() {
		s.logger.Info("new org registrations detected — scanning now instead of waiting for the org refresh ticker")
		s.refreshUserOrgs(ctx, true)
	})
}

// refreshUserOrgs scans user_org_requests for new repos in tracked orgs
// and adds them to the user's group + collection queue. With
// onlyNeverScanned set (the poll-tick demand path) the pass is scoped
// to orgs that have never been enumerated; the 4h ticker pass runs
// unscoped to keep discovering new repos in long-tracked orgs.
func (s *Scheduler) refreshUserOrgs(ctx context.Context, onlyNeverScanned bool) {
	orgs, err := s.store.GetOrgRequests(ctx)
	if err != nil || len(orgs) == 0 {
		return
	}
	if onlyNeverScanned {
		scoped := orgs[:0]
		for _, o := range orgs {
			if o.LastScanned == nil {
				scoped = append(scoped, o)
			}
		}
		orgs = scoped
		if len(orgs) == 0 {
			return
		}
	}

	// v0.27.83 dedup: user_org_requests is unique per (group_id,
	// org_url), so an org tracked by N groups is N rows — and the
	// pre-v0.27.83 loop enumerated the SAME org N times per pass (API
	// pages × N plus per-repo DB round trips × N; the multiplier grows
	// with users × popular orgs). Rows are now resolved and
	// rejected-gated PER ROW first (exact v0.27.20 semantics: a
	// rejected group's row is never linked and never stamped), then
	// grouped by (platform, lowercased org name) — GitHub org names
	// are case-insensitive — so each distinct org is enumerated ONCE,
	// its repos linked into ALL registered groups, and ALL its rows
	// stamped.
	type orgEntry struct {
		requestID int64
		groupID   int64
	}
	type orgScan struct {
		name     string // first-seen casing — used in the API path
		platform string
		entries  []orgEntry
	}
	var order []string // claim order (most-stale org first)
	grouped := map[string]*orgScan{}
	for _, org := range orgs {
		groupID, err := s.store.GetGroupIDForOrgRequest(ctx, org.OrgRequestID)
		if err != nil {
			continue
		}

		// v0.27.20 gate: a 'rejected' group's orgs never scan.
		// Registration is approval-gated in AddOrgToGroup (presence in
		// user_org_requests = approved), so this is the belt for the
		// group-level abuse lever — RejectGroup must stop org-driven
		// enqueue too, not just direct adds.
		if status, serr := s.store.GetGroupStatus(ctx, groupID); serr == nil && status == "rejected" {
			s.logger.Warn("org scan skipped — owning group is rejected",
				"group_id", groupID, "org", org.OrgName)
			continue
		}

		key := org.Platform + "\x00" + strings.ToLower(org.OrgName)
		g, ok := grouped[key]
		if !ok {
			g = &orgScan{name: org.OrgName, platform: org.Platform}
			grouped[key] = g
			order = append(order, key)
		}
		g.entries = append(g.entries, orgEntry{requestID: org.OrgRequestID, groupID: groupID})
	}

	s.logger.Info("scanning user org requests",
		"count", len(orgs), "distinct_orgs", len(order), "only_never_scanned", onlyNeverScanned)
	for _, key := range order {
		g := grouped[key]

		// ForgeID (v0.27.102) is the forge's numeric repo ID from the
		// listing JSON — the rename-proof identity UpsertRepo dedups on.
		var repos []struct{ URL, Owner, Name, ForgeID string }
		switch g.platform {
		case "github":
			if s.ghKeys == nil {
				continue // rows stay unstamped — retried when keys exist
			}
			httpC := platform.NewHTTPClient(s.ghAPIBase, s.ghKeys, s.logger, platform.AuthGitHub)
			// Try /orgs/ first, fall back to /users/ for personal accounts.
			basePaths := []string{
				fmt.Sprintf("/orgs/%s/repos", g.name),
				fmt.Sprintf("/users/%s/repos", g.name),
			}
			for _, basePath := range basePaths {
				page := 1
				found := false
				for {
					path := fmt.Sprintf("%s?per_page=100&type=all&page=%d", basePath, page)
					resp, err := httpC.Get(platform.WithoutETag(ctx), path)
					if err != nil {
						break
					}
					var items []struct {
						ID      int64  `json:"id"` // v0.27.102 — rename-proof numeric identity
						HTMLURL string `json:"html_url"`
						Name    string `json:"name"`
						Owner   struct {
							Login string `json:"login"`
						} `json:"owner"`
					}
					if decErr := json.NewDecoder(resp.Body).Decode(&items); decErr != nil {
						s.logger.Warn("failed to decode org repos response", "path", path, "error", decErr)
					}
					resp.Body.Close()
					if len(items) == 0 {
						break
					}
					found = true
					for _, item := range items {
						repos = append(repos, struct{ URL, Owner, Name, ForgeID string }{item.HTMLURL, item.Owner.Login, item.Name, model.ForgeIDString(item.ID)})
					}
					page++
				}
				if found {
					break
				}
			}
		}

		// Distinct target groups (case-variant registrations of the same
		// org within one group collapse here).
		var groupIDs []int64
		seenGroup := map[int64]bool{}
		for _, e := range g.entries {
			if !seenGroup[e.groupID] {
				seenGroup[e.groupID] = true
				groupIDs = append(groupIDs, e.groupID)
			}
		}

		newCounts := map[int64]int{}
		for _, repo := range repos {
			// Ensure repo exists — ONCE per repo, regardless of how many
			// groups track the org.
			repoID, findErr := s.store.FindRepoByURL(ctx, repo.URL)
			if findErr != nil {
				s.logger.Warn("failed to find repo by URL", "url", repo.URL, "error", findErr)
			}
			if repoID == 0 {
				var err error
				repoID, err = s.store.UpsertRepo(ctx, &model.Repo{
					Platform:   model.PlatformGitHub,
					GitURL:     repo.URL,
					Name:       repo.Name,
					Owner:      repo.Owner,
					PlatformID: repo.ForgeID, // v0.27.102 — enables the rename-heal inside UpsertRepo
				})
				if err != nil {
					continue
				}
				if enqErr := s.store.EnqueueRepo(ctx, repoID, 100); enqErr != nil {
					s.logger.Warn("failed to enqueue repo", "repo_id", repoID, "error", enqErr)
				}
			} else if repo.ForgeID != "" {
				// v0.27.102: opportunistic forge-ID backfill on already-
				// tracked rows. The org-tracked cohort is EXACTLY the
				// population at risk of rename re-discovery, so filling
				// its IDs on every scan pass closes the protection gap
				// now instead of waiting for each repo's Phase 0 cycle.
				// Fill-empty-only; best-effort.
				if idErr := s.store.SetPlatformRepoIDIfEmpty(ctx, repoID, repo.ForgeID); idErr != nil {
					s.logger.Warn("failed to backfill platform_repo_id", "repo_id", repoID, "error", idErr)
				}
			}
			// Link into every registered group (ON CONFLICT DO NOTHING).
			// Count a repo as "new" ONLY when a link row was actually
			// inserted — a nil error alone includes the no-op re-link of
			// every already-tracked repo, which had "found new repos"
			// claiming the org's full repo count every pass forever
			// (9.3M bogus new repos in the Aug 7–16 2026 run).
			for _, gid := range groupIDs {
				inserted, err := s.store.AddRepoToGroupByID(ctx, gid, repoID)
				if err != nil {
					// Same message as refreshGitHubOrg/refreshGitLabGroup so
					// one grep covers all three link paths (v0.27.92).
					s.logger.Warn("failed to link discovered repo into user_repos",
						"repo_id", repoID, "group_id", gid, "error", err)
					continue
				}
				if inserted {
					newCounts[gid]++
				}
			}
		}

		// Stamp EVERY registration row for this org — they all shared
		// the one enumeration.
		for _, e := range g.entries {
			if err := s.store.MarkOrgRequestScanned(ctx, e.requestID); err != nil {
				s.logger.Warn("failed to mark org request scanned", "org_request_id", e.requestID, "error", err)
			}
		}
		for _, gid := range groupIDs {
			if newCounts[gid] > 0 {
				s.logger.Info("user org scan found new repos",
					"org", g.name, "group_id", gid, "new_repos", newCounts[gid])
			}
		}
	}

	// v0.27.93 self-heal: enumeration above links only what the forge
	// listing returns. Tracked repos that entered the catalog through
	// other paths (mailing-list loaders, foundation importers, renames,
	// GitLab orgs — which this scan doesn't enumerate) would otherwise
	// stay unlinked forever (the 2026-08-18 production drift: 9 live
	// repos stranded, incl. gitlab.com/petsc/petsc). One set-based pass
	// per FULL cycle; the 10s demand probe (onlyNeverScanned) stays
	// cheap and skips it.
	if !onlyNeverScanned {
		if linked, err := s.store.ReconcileOrgRepoLinks(ctx); errors.Is(err, context.Canceled) {
			return // shutdown, not a failure
		} else if err != nil {
			s.logger.Warn("org link reconciliation failed", "error", err)
		} else if linked > 0 {
			s.logger.Info("org link reconciliation linked stranded tracked repos",
				"links_inserted", linked)
		}
	}
}

// runBreadth discovers cross-repo activity for contributors via the GitHub Events API.
func (s *Scheduler) runBreadth(ctx context.Context) {
	if s.ghKeys == nil {
		return
	}
	if s.breadthWorker == nil {
		// Run() constructs the worker at startup; nil here means the
		// scheduler was built without keys or outside Run (tests).
		return
	}
	result, err := s.breadthWorker.Run(ctx, s.cfg.Collection.BreadthBatchSizeOrDefault(), s.cfg.Collection.BreadthCooldownDuration())
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return // shutdown, not a failure (the v0.27.28 ClassCanceled rule)
		}
		s.logger.Warn("breadth worker failed", "error", err)
		return
	}
	if result.ContributorsProcessed > 0 {
		s.logger.Info("breadth worker complete",
			"contributors", result.ContributorsProcessed,
			"events_inserted", result.EventsInserted)
	}
}

func (s *Scheduler) recoverStale(ctx context.Context) {
	recovered, err := s.store.RecoverStaleLocks(ctx, s.cfg.StaleLockTimeout)
	if err != nil {
		s.logger.Error("failed to recover stale locks", "error", err)
		return
	}
	if recovered > 0 {
		s.logger.Warn("recovered stale locks", "count", recovered)
	}
}
