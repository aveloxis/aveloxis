package db

import (
	"os"
	"strings"
	"testing"
)

// TestRealignDueDatesExists — store-level source contract: queue.go must
// expose a RealignDueDates method that recomputes due_at = last_collected +
// recollectAfter for queued rows on scheduler startup. Without it, a change
// to days_until_recollect (e.g., 1 → 7) never takes effect on already-queued
// rows whose due_at was baked in by CompleteJob under the old setting.
func TestRealignDueDatesExists(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) RealignDueDates(") {
		t.Error("queue.go must define RealignDueDates(ctx, recollectAfter) — " +
			"required so a changed days_until_recollect takes effect on restart " +
			"instead of waiting for each repo's next CompleteJob to rebake due_at")
	}
}

// TestRealignDueDatesSQLShape verifies the SQL:
//   - only touches 'queued' rows (never 'collecting' — worker is mid-flight)
//   - only touches rows with a non-null last_collected (never-collected repos
//     keep their initial due_at=NOW() so they collect on first pass)
//   - uses last_collected + interval (not NOW() + interval) so the cooldown
//     is measured from the actual completion, not from startup time
//   - is idempotent (only rewrites rows whose current due_at differs)
func TestRealignDueDatesSQLShape(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) RealignDueDates(")
	if idx < 0 {
		t.Fatal("RealignDueDates not found — see TestRealignDueDatesExists")
	}
	// Grab the function body (roughly).
	fnBody := src[idx:]
	end := strings.Index(fnBody, "\nfunc ")
	if end > 0 {
		fnBody = fnBody[:end]
	}

	if !strings.Contains(fnBody, "status = 'queued'") && !strings.Contains(fnBody, `status='queued'`) {
		t.Error("RealignDueDates must filter status='queued' so it doesn't " +
			"disturb in-flight 'collecting' jobs")
	}
	if !strings.Contains(fnBody, "last_collected IS NOT NULL") {
		t.Error("RealignDueDates must require last_collected IS NOT NULL so " +
			"never-collected repos keep their due_at=NOW() initial value")
	}
	if !strings.Contains(fnBody, "last_collected +") {
		t.Error("RealignDueDates must compute due_at from last_collected + " +
			"interval — using NOW()+interval would grant every repo a fresh " +
			"full cooldown on every restart")
	}
	// Idempotency — the WHERE clause must exclude already-aligned rows so
	// repeated startups don't churn updated_at.
	if !strings.Contains(fnBody, "<>") && !strings.Contains(fnBody, "!=") {
		t.Error("RealignDueDates must skip rows where due_at already equals " +
			"last_collected + interval (idempotency — keeps updated_at stable)")
	}
}

// TestSchedulerCallsRealignDueDatesOnStartup — source contract: the scheduler
// Run loop must invoke RealignDueDates once before the first fillWorkerSlots
// so stale due_at values from a previous days_until_recollect setting are
// corrected before any job is dequeued.
func TestSchedulerCallsRealignDueDatesOnStartup(t *testing.T) {
	data, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(data)

	idx := strings.Index(code, "func (s *Scheduler) Run(")
	if idx < 0 {
		t.Fatal("cannot find Scheduler.Run")
	}
	fnBody := code[idx:]
	// Consider only the startup prelude — everything before the main `for {` loop.
	forIdx := strings.Index(fnBody, "\n\tfor {")
	if forIdx > 0 {
		fnBody = fnBody[:forIdx]
	}

	if !strings.Contains(fnBody, "RealignDueDates") {
		t.Error("Scheduler.Run must call store.RealignDueDates(ctx, s.cfg.RecollectAfter) " +
			"during startup (before entering the main poll loop) so stale due_at " +
			"values from a prior days_until_recollect setting are corrected before " +
			"any job is dequeued")
	}
}

// TestSchedulerRealignsBeforeLeftoverStagingDrain pins the ordering of the
// two startup steps within Scheduler.Run's prelude. RealignDueDates MUST
// run before processLeftoverStaging — otherwise, with a non-empty staging
// backlog on restart, the realignment waits behind the "multi-minute
// drain" (per the comment on processLeftoverStaging) and the monitor
// page shows stale due_at values for the whole drain window. Operators
// see that and conclude the config change didn't take effect. The v0.16.6
// fix is functionally correct either way; the ordering only affects how
// quickly the config change becomes visible.
//
// This test would have failed on v0.18.24's ordering (staging drain first,
// then realign) and passes after the v0.18.26 swap. Regressing the order
// will fail this test before shipping.
func TestSchedulerRealignsBeforeLeftoverStagingDrain(t *testing.T) {
	data, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(data)

	idx := strings.Index(code, "func (s *Scheduler) Run(")
	if idx < 0 {
		t.Fatal("cannot find Scheduler.Run")
	}
	fnBody := code[idx:]
	forIdx := strings.Index(fnBody, "\n\tfor {")
	if forIdx > 0 {
		fnBody = fnBody[:forIdx]
	}

	// Pin the actual call sites, not mentions in comments. Without the
	// open-paren anchor, a comment line like "runs BEFORE
	// processLeftoverStaging" would be indexed first and make the
	// assertion say the wrong thing.
	realignIdx := strings.Index(fnBody, "s.store.RealignDueDates(")
	stagingIdx := strings.Index(fnBody, "s.processLeftoverStaging(")

	if realignIdx < 0 {
		t.Fatal("Scheduler.Run must call s.store.RealignDueDates(...) (see TestSchedulerCallsRealignDueDatesOnStartup)")
	}
	if stagingIdx < 0 {
		t.Fatal("Scheduler.Run must call s.processLeftoverStaging(...) during startup to drain orphaned staging rows")
	}

	if realignIdx > stagingIdx {
		t.Errorf("RealignDueDates is called AFTER processLeftoverStaging in Scheduler.Run "+
			"(realign_pos=%d, staging_pos=%d). Restart a scheduler with a large staging "+
			"backlog and the monitor's Due column keeps showing stale due_at values for "+
			"the whole drain window — operators interpret that as 'days_until_recollect "+
			"change didn't take effect'. Realign first (it's a single UPDATE); then drain.",
			realignIdx, stagingIdx)
	}
}
