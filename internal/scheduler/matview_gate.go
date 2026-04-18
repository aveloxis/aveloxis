package scheduler

import "sync/atomic"

// MatviewRebuildActive signals that the weekly materialized-view rebuild is
// currently in progress. The scheduler sets this before invoking
// db.RefreshMaterializedViews and clears it when the refresh finishes; while
// true, fillWorkerSlots refuses to claim new jobs and the monitor dashboard
// shows a pause banner.
//
// Why an exported atomic.Bool instead of a method on Scheduler: the monitor
// package lives outside the scheduler's goroutine graph and needs to read
// this state for the dashboard. A package-level atomic keeps the contract
// cycle-free (monitor imports scheduler; scheduler does not import monitor).
var MatviewRebuildActive atomic.Bool

// ShouldStartMatviewRebuild reports whether the current worker-utilization
// level is low enough to safely begin a matview rebuild. The rule is
// "active workers strictly below one-third of total workers".
//
// Chosen for the incident of 2026-04-18: the prior implementation drained
// all workers before starting the rebuild, which blocked the scheduler's
// main goroutine for 9+ hours on a single slow parallel-mode collection.
// The gated approach waits for natural drain (collection throughput always
// falls before the weekly rebuild day ends) and runs the refresh
// CONCURRENTLY so the few remaining jobs can keep working against
// read-safe views.
//
// Edge cases:
//   - totalWorkers <= 0: returns false. With no configured capacity there
//     is no meaningful threshold and starting a rebuild would be premature.
//   - activeWorkers < 0: coerced to 0. Callers pass len(sem) which is
//     non-negative, but defensive coercion prevents a negative sentinel
//     from flipping the comparison.
func ShouldStartMatviewRebuild(activeWorkers, totalWorkers int) bool {
	if totalWorkers <= 0 {
		return false
	}
	if activeWorkers < 0 {
		activeWorkers = 0
	}
	return 3*activeWorkers < totalWorkers
}
