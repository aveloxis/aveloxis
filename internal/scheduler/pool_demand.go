// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"github.com/aveloxis/aveloxis/internal/config"
)

// The scheduler's connection DEMAND: how many goroutines can be holding
// a pooled connection at once. v0.29.58 (2026-09-22 log review, finding
// 1): serve sized its pool as workers+15 — a literal that ignored the
// 10 distribution workers, 16 mailing-list workers and every background
// loop, and the health probe (which pings THROUGH the pool) then reported
// a saturated pool as "database unavailable" thirteen times in one run
// while Postgres was up the whole time. Every class is enumerated here,
// derived from configuration, and pinned by pool_demand_test.go against
// the goroutine labels in this package, so a new loop must be classified.

// perSlotConnections is how many connections ONE collection slot can
// hold at once: the staged collector fans out into collect-issues,
// collect-prs and collect-messages (stagedFanOut, each a StagingWriter),
// the job's heartbeat goroutine (job-heartbeat) updates the queue row
// alongside them, and the per-job long-jobs watchdog polls the staging row
// count through the pool.
const (
	stagedFanOut       = 3
	slotHeartbeat      = 1 // job-heartbeat
	slotWatchdog       = 1 // long-jobs-watchdog (review round 4)
	perSlotConnections = stagedFanOut + slotHeartbeat + slotWatchdog
)

// backgroundDBLoops are the SINGLETON goroutines that acquire from
// serve's pool: one connection each at their busiest. The names are the
// safego.Go / goTracked / singleFlight / safego.Recover labels in every
// package that receives the store (scheduler, collector, collector/
// distribution, db); the registry test fails when a label appears in
// those sources that is classified neither here, nor in perWorkerLoops,
// nor in nonDBGoroutines.
var backgroundDBLoops = []string{
	"run-loop",                     // Run's own poll/claim loop (fillWorkerSlots, lock recovery, the key-pool summary, the matview check)
	"leftover-staging-drain",       // processLeftoverStagingBackground
	"drain-heartbeat",              // db.StartDrainHeartbeat: the drain's lease heartbeat, alive for its whole run (review round 5)
	"org-refresh",                  // refreshOrgs (startup + ticker; single-flight since review round 5)
	"repo-metadata-backfill",       // runRepoMetadataBackfill
	"db-health-monitor",            // runDBHealthMonitor (Ping goes through the pool)
	"stall-detector",               // runStallDetector
	"staging-cleanup",              // runStagingCleanup (single-flight since review round 5)
	"vuln-digest",                  // runVulnDigest (single-flight since review round 5)
	"matview-rebuild",              // rebuildMatviews
	"mailing-list-sender-resolve",  // goTracked: sender → contributor resolution
	"mailing-list-sender-backfill", // goTracked: the hourly keyset backfill
	"jira-drain",                   // goTracked: jira staging drain
	"monitor-dashboard",            // the :5555 monitor's handlers read through the same store (one request at a time is the allowance)
	// The singleFlight periodic tasks (review round 4): each runs on its
	// OWN goroutine off a run-loop tick, so any number of them can hold a
	// connection at once, alongside the loop itself.
	"user-org-refresh",
	"user-org-demand-scan",
	"contributor-breadth",
	"activity-classification",
	"activity-history", // the sweep itself; its fan-out is activity-history-worker below
	"contributor-enrichment",
	"search-resolve",
	"gone-recheck",
	"affiliations-population",
}

// scancodeDBLoops are the scancode subsystem's singletons, counted only
// when this process runs scancode workers (review round 5): the
// dispatcher claims through the store alongside the runners; the orphan
// monitor, the lock check and the startup sweep read it.
var scancodeDBLoops = []string{
	"scancode-dispatcher",
	"scancode-orphan-monitor",
	"scancode-lock-check",
	"scancode-startup-sweep",
}

// perWorkerLoops are the goroutine classes whose count comes from
// configuration; PoolDemand multiplies each by its configured width.
var perWorkerLoops = []string{
	"collection-job",          // one per worker slot, perSlotConnections each
	"job-heartbeat",           // counted inside perSlotConnections
	"long-jobs-watchdog",      // counted inside perSlotConnections
	"collect-issues",          // counted inside perSlotConnections (stagedFanOut)
	"collect-prs",             // counted inside perSlotConnections (stagedFanOut)
	"collect-messages",        // counted inside perSlotConnections (stagedFanOut)
	"pr-shard",                // fetches concurrently, writes under the PR phase's mutex: inside collect-prs
	"distribution-worker",     // DistributionTrackingWorkersOrDefault
	"distribution-runner",     // the same workers, as the distribution package labels them
	"distribution-dispatcher", // +1 alongside the runners (review round 5)
	"mailing-list-worker",     // MailingListWorkersOrDefault × systems
	"mailing-list-drain",      // processor workers × systems
	"jira-worker",             // JiraWorkersOrDefault
	"scancode-worker",         // the scheduler's label for the scancode runner set
	"scancode-runner",         // ScancodeWorkersOrDefault when scancode runs here
	"activity-history-worker", // ActivityHistoryConcurrencyValue at once, under its semaphore
	"breadth-fetcher",         // BreadthFetchConcurrencyOrDefault; each renames through the store (review round 5)
}

// nonDBGoroutines carry a label but hold no pooled connection: they wait
// on a channel or a WaitGroup, feed an in-memory channel, or talk only to
// an external HTTP service. Classified so the registry test sees every
// label; never counted (review round 5).
var nonDBGoroutines = []string{
	"scancode-bookkeeping-wait",
	"background-pools-wait",
	"breadth-fetchers-wait",
	"breadth-feeder",
	"osv-detail-fetch",
}

// PoolDemand returns the peak number of pooled connections the scheduler
// can ask for at once, and the breakdown as log attributes (SR-10: the
// EFFECTIVE value with its derivation). workers is the slot count serve
// runs with; mailingListSystems is how many mailing-list systems are
// configured (each spawns its own worker set).
func PoolDemand(cfg *config.Config, workers, mailingListSystems int) (int, []any) {
	c := cfg.Collection
	slots := workers * perSlotConnections
	dist := 0
	if c.DistributionTrackingEnabled {
		dist = c.DistributionTrackingWorkersOrDefault() + 1 // runners + the dispatcher
	}
	ml, mlDrain := 0, 0
	if c.MailingListEnabled {
		ml = c.MailingListWorkersOrDefault() * mailingListSystems
		// The drain loops are spawned PER SYSTEM too (mailinglist_wiring.go
		// clamps the count to at least one) — review round 4.
		mlDrain = max(1, c.MailingListProcessorWorkersOrDefault()) * mailingListSystems
	}
	jira := 0
	if c.JiraEnabled {
		jira = c.JiraWorkersOrDefault()
	}
	// The spawn site's own transform (scheduler.go): 0 disables scancode
	// on this process; anything else goes through ScancodeWorkersOrDefault
	// (review round 5 — a negative used to count 0 here and run 2 there).
	scancode := 0
	if c.ScancodeWorkers != 0 {
		scancode = c.ScancodeWorkersOrDefault() + len(scancodeDBLoops)
	}
	history := c.ActivityHistoryConcurrencyValue()
	breadth := c.BreadthFetchConcurrencyOrDefault()
	bg := len(backgroundDBLoops)
	demand := slots + dist + ml + mlDrain + jira + scancode + history + breadth + bg
	return demand, []any{
		"pool_demand", demand,
		"demand_collection_slots", slots,
		"demand_per_slot", perSlotConnections,
		"demand_distribution", dist,
		"demand_mailing_list_workers", ml,
		"demand_mailing_list_drain", mlDrain,
		"demand_jira_workers", jira,
		"demand_scancode", scancode,
		"demand_activity_history_workers", history,
		"demand_breadth_fetchers", breadth,
		"demand_background_loops", bg,
	}
}
