// Source-contract tests for v0.18.29 Fix 2: contributor enrichment runs
// as a periodic scheduler task, not per-job inside processJob.
//
// Background: at v0.18.28, scheduler.go:460 calls
// collector.EnrichThinContributors(...) at the end of every processJob
// run. Each call fetches up to EnrichBatchSize=14000 thin logins and
// hits REST GET /users/{login} for each one. With 120 workers each
// finishing their tiny repo and immediately calling enrichment, the
// fleet attempts ~1.68M REST calls in parallel windows and exhausts
// all 73 GitHub keys in ~11 minutes (verified in production logs from
// 2026-05-07T01:37:07 through 01:48:23).
//
// The fix moves enrichment to a single goroutine driven by a periodic
// ticker (default 30 min). One caller, one batch per tick, well under
// the rate-limit budget.

package scheduler

import (
	"strings"
	"testing"
)

// TestEnrichmentNotCalledFromRunJob is the headline contract: the
// per-job enrichment call must be removed from runJob. A regression
// that re-adds it would burn the REST budget again on the next restart.
func TestEnrichmentNotCalledFromRunJob(t *testing.T) {
	src := mustReadSchedulerSource(t)

	// Locate runJob (the per-job entry point).
	idx := strings.Index(src, "func (s *Scheduler) runJob(")
	if idx < 0 {
		t.Fatal("could not locate runJob")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}

	if strings.Contains(body, "EnrichThinContributors") {
		t.Error("runJob must NOT call EnrichThinContributors. With 120 workers each " +
			"calling it after their repo finishes, the fleet attempts ~1.68M REST calls " +
			"in parallel windows and exhausts all GitHub keys in ~11 minutes. " +
			"Enrichment must run as a single periodic scheduler task instead.")
	}
}

// TestEnrichmentTickerInRun pins that scheduler.Run installs a periodic
// ticker driving enrichment.
func TestEnrichmentTickerInRun(t *testing.T) {
	src := mustReadSchedulerSource(t)
	body := extractRunBody(src)
	if body == "" {
		t.Fatal("could not locate Run body")
	}

	// Look for a ticker variable that drives enrichment. We accept either
	// a named ticker (`enrichTicker := time.NewTicker(...)`) or an inline
	// time.Tick. The body must reference both EnrichInterval (config) and
	// EnrichThinContributors (the call).
	if !strings.Contains(body, "EnrichInterval") {
		t.Error("scheduler.Run must reference cfg.EnrichInterval to drive the periodic enrichment ticker")
	}
	if !strings.Contains(src, "EnrichThinContributors") {
		t.Error("scheduler must call collector.EnrichThinContributors from the periodic enrichment goroutine — " +
			"removing it entirely loses contributor profile data")
	}
}

// TestEnrichIntervalConfigField pins the new Config field exists with a
// time.Duration type so it can be plumbed from aveloxis.json.
func TestEnrichIntervalConfigField(t *testing.T) {
	src := mustReadSchedulerSource(t)
	idx := strings.Index(src, "type Config struct {")
	if idx < 0 {
		t.Fatal("could not locate scheduler.Config struct")
	}
	end := strings.Index(src[idx:], "\n}")
	if end < 0 {
		t.Fatal("could not find end of Config struct")
	}
	configDecl := src[idx : idx+end]
	if !strings.Contains(configDecl, "EnrichInterval") {
		t.Error("scheduler.Config must declare EnrichInterval so the cadence is configurable per deployment")
	}
	if !strings.Contains(configDecl, "time.Duration") {
		t.Error("expected time.Duration somewhere in Config — the existing Duration fields are the model")
	}
}

// TestEnrichIntervalDefaultIsSane pins a 30-minute default. Any caller
// that constructs a Scheduler without setting EnrichInterval should get
// a sensible cadence. Faster than 1 minute would re-create rate-limit
// pressure; slower than 1 hour leaves enrichment lagging the fleet.
func TestEnrichIntervalDefaultIsSane(t *testing.T) {
	src := mustReadSchedulerSource(t)
	// Look for a defaulting block in NewWithKeys or New.
	if !strings.Contains(src, "EnrichInterval == 0") {
		t.Error("scheduler.NewWithKeys must default EnrichInterval when it's zero — " +
			"matching the pattern for PollInterval, RecollectAfter, OrgRefreshInterval")
	}
	if !strings.Contains(src, "30 * time.Minute") && !strings.Contains(src, "time.Minute * 30") {
		// If a different default is used, this test will need updating; flag it.
		t.Error("expected 30 * time.Minute as the EnrichInterval default — " +
			"that's the value documented in the v0.18.29 plan")
	}
}
