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

// backgroundDBLoops are the scheduler's SINGLETON goroutines that acquire
// from the pool: one connection each at their busiest. The names are the
// safego.Go / goTracked / safego.Recover labels in this package; the
// registry test fails when a label appears in the sources that is
// classified neither here nor in perWorkerLoops.
var backgroundDBLoops = []string{
	"run-loop",                     // Run's own poll/claim loop (fillWorkerSlots, lock recovery, the key-pool summary, the matview check)
	"leftover-staging-drain",       // processLeftoverStagingBackground
	"org-refresh",                  // refreshOrgs (startup + ticker)
	"repo-metadata-backfill",       // runRepoMetadataBackfill
	"db-health-monitor",            // runDBHealthMonitor (Ping goes through the pool)
	"stall-detector",               // runStallDetector
	"staging-cleanup",              // runStagingCleanup
	"vuln-digest",                  // runVulnDigest
	"matview-rebuild",              // rebuildMatviews
	"mailing-list-sender-resolve",  // goTracked: sender → contributor resolution
	"mailing-list-sender-backfill", // goTracked: the hourly keyset backfill
	"jira-drain",                   // goTracked: jira staging drain
	"scancode-bookkeeping-wait",    // waits on the scancode worker; no statement of its own but holds no less than the label says
	"background-pools-wait",        // shutdown join of the background pools
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

// perWorkerLoops are the goroutine classes whose count comes from
// configuration; PoolDemand multiplies each by its configured width.
var perWorkerLoops = []string{
	"collection-job",     // one per worker slot, perSlotConnections each
	"job-heartbeat",      // counted inside perSlotConnections
	"long-jobs-watchdog", // counted inside perSlotConnections
	"distribution-worker",
	"mailing-list-worker",
	"mailing-list-drain",
	"jira-worker",
	"scancode-worker",
	"activity-history-worker", // ActivityHistoryConcurrencyValue at once, under its semaphore
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
		dist = c.DistributionTrackingWorkersOrDefault()
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
	scancode := c.ScancodeWorkers
	if scancode < 0 {
		scancode = 0
	}
	history := c.ActivityHistoryConcurrencyValue()
	bg := len(backgroundDBLoops)
	demand := slots + dist + ml + mlDrain + jira + scancode + history + bg
	return demand, []any{
		"pool_demand", demand,
		"demand_collection_slots", slots,
		"demand_per_slot", perSlotConnections,
		"demand_distribution_workers", dist,
		"demand_mailing_list_workers", ml,
		"demand_mailing_list_drain", mlDrain,
		"demand_jira_workers", jira,
		"demand_scancode_workers", scancode,
		"demand_activity_history_workers", history,
		"demand_background_loops", bg,
	}
}
