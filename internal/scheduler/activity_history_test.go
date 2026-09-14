// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// activity_history_test.go — TDD suite for the v0.27.58 daily
// contributor-history sweep worker. Contracts:
//   - GitHub-only fetch via a narrow capability interface (platform.
//     Client NOT widened — no regression surface);
//   - windows are built from the CONFIGURED span
//     (activity_history_window_days → ActivityHistoryWindowDaysOrDefault
//     → HistoryWindows), the config-knobs-end-to-end wiring;
//   - a contributor whose meta fetch 404s (deleted/renamed) is
//     mark-only stamped; a fetch ERROR stamps nothing (retry next
//     tick);
//   - ticker rides Run's select under singleFlight.

import (
	"strings"
	"testing"
)

func TestHistoryFetcherIsNarrowCapabilityInterface(t *testing.T) {
	src := readSchedulerFile(t, "activity_history.go")
	if !strings.Contains(src, "type contributorHistoryFetcher interface") {
		t.Fatal("history fetch must be consumed via a scheduler-side capability interface (platform.Client must not be widened)")
	}
	for _, m := range []string{"FetchContributorHistoryMeta(", "FetchContributorDailyHistory("} {
		if !strings.Contains(src, m) {
			t.Errorf("capability interface must declare %s", m)
		}
	}
	if !strings.Contains(src, ".(contributorHistoryFetcher)") {
		t.Error("runActivityHistory must type-assert s.ghClient and no-op when the capability is absent")
	}
}

func TestHistoryWorkerUsesConfiguredWindowSpan(t *testing.T) {
	src := readSchedulerFile(t, "activity_history.go")
	if !strings.Contains(src, "ActivityHistoryWindowDaysOrDefault()") {
		t.Fatal("the worker must build windows from the configured activity_history_window_days (via the clamping accessor) — the config-knobs-end-to-end contract")
	}
	if !strings.Contains(src, "github.HistoryWindows(") {
		t.Error("windows must come from the shared github.HistoryWindows tiling function")
	}
}

func TestHistoryWorkerFailureContract(t *testing.T) {
	src := readSchedulerFile(t, "activity_history.go")
	if !strings.Contains(src, "MarkHistoryBackfilled") {
		t.Error("contributors with no fetchable account (meta absent) must be mark-only stamped so they leave the NULLS-FIRST claim head")
	}
	if !strings.Contains(src, "StoreContributorActivityHistory") {
		t.Error("fetched history must go through the single-transaction StoreContributorActivityHistory (data + stamp atomic)")
	}
	// Error path: a failed history fetch must NOT stamp — pin the
	// continue-without-mark shape. Anchor on the CALL site
	// (fetcher.Fetch...), not the capability-interface declaration.
	idx := strings.Index(src, "fetcher.FetchContributorDailyHistory(")
	if idx < 0 {
		t.Fatal("cannot find the history fetch call site")
	}
	window := src[idx:]
	if end := strings.Index(window, "StoreContributorActivityHistory"); end > 0 {
		window = window[:end]
	}
	if strings.Contains(window, "MarkHistoryBackfilled") {
		t.Error("a FAILED history fetch must not stamp the contributor — the claim must retry next tick, not silently skip a cooldown period")
	}
}

func TestHistoryTickerWiredIntoRun(t *testing.T) {
	src := readSchedulerFile(t, "scheduler.go")
	if !strings.Contains(src, "historyTicker") {
		t.Fatal("Run must declare a historyTicker for the daily-history sweep")
	}
	if !strings.Contains(src, "runActivityHistory") {
		t.Fatal("Run's select loop must dispatch runActivityHistory")
	}
	if !strings.Contains(src, "s.singleFlight(&s.activityHistActive") {
		t.Error("the ticker must run under singleFlight (a slow sweep must not stack)")
	}
}

// v0.28.3 — the sweep runs a bounded contributor POOL (the
// BreadthWorker pattern; the serial loop measured ~1,170/day against
// a 2.4M pool) with all cadence knobs promoted to config accessors.
func TestActivityHistoryRunsPooled(t *testing.T) {
	src := readSchedulerFile(t, "activity_history.go")
	i := strings.Index(src, "func (s *Scheduler) runActivityHistory(")
	if i < 0 {
		t.Fatal("runActivityHistory missing")
	}
	body := src[i:]
	if j := strings.Index(body, "\nfunc (s *Scheduler) processHistoryContributor("); j > 0 {
		body = body[:j]
	} else {
		t.Fatal("processHistoryContributor (the extracted per-contributor body) missing")
	}
	for _, needle := range []string{
		"sem := make(chan struct{}, concurrency)",
		"var wg sync.WaitGroup",
		"safego.Recover(s.logger",
		// v0.28.10 (Copilot round 7): the outcome commits in a DEFER
		// with failed as the default — a panicking worker previously
		// bypassed the switch entirely, so the cycle log reported
		// fewer outcomes than claimed and zero failures for a panic.
		"outcome := historyFailed",
		"outcome = s.processHistoryContributor(ctx, fetcher, c, windowDays)",
		"ActivityHistoryBatchValue()",
		"ActivityHistoryCooldownValue()",
		"ActivityHistoryConcurrencyValue()",
		"SetHistoryWindowConcurrency",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("runActivityHistory missing %q", needle)
		}
	}
	// The hardcoded cadence constants are GONE (accessors are the
	// single default layer, SR-10).
	full := src
	for _, banned := range []string{"activityHistoryBatch    =", "activityHistoryInterval =", "activityHistoryCooldown ="} {
		if strings.Contains(full, banned) {
			t.Errorf("hardcoded constant %q must be gone — the config accessors own the defaults", banned)
		}
	}
	// The ticker reads the accessor too.
	sched := readSchedulerFile(t, "scheduler.go")
	if !strings.Contains(sched, "time.NewTicker(s.cfg.Collection.ActivityHistoryIntervalValue())") {
		t.Error("historyTicker must use ActivityHistoryIntervalValue()")
	}
}
