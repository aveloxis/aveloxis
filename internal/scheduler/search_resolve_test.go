// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for v0.19.2 Fix #4 scheduler integration:
// the search-resolve background task runs on a periodic ticker,
// taking batches of contributors with email but no gh_user_id and
// resolving them via the GitHub Search API.

package scheduler

import (
	"strings"
	"testing"
)

// TestSearchResolveIntervalConfigField pins the new Config field.
func TestSearchResolveIntervalConfigField(t *testing.T) {
	src := mustReadSchedulerSource(t)
	idx := strings.Index(src, "type Config struct {")
	if idx < 0 {
		t.Fatal("could not locate Config struct")
	}
	end := strings.Index(src[idx:], "\n}")
	configDecl := src[idx : idx+end]
	// v0.25.37: the mirror field is gone; the cadence is read through
	// the CollectionConfig accessor at the point of use.
	if !strings.Contains(configDecl, "Collection *config.CollectionConfig") {
		t.Error("scheduler.Config must carry Collection *config.CollectionConfig")
	}
	if !strings.Contains(src, "s.cfg.Collection.SearchResolveIntervalDuration()") {
		t.Error("the search-resolve ticker must read s.cfg.Collection.SearchResolveIntervalDuration()")
	}
}

// TestSearchResolveIntervalDefaultIsSane pins a 1-hour default.
// Search API is rate-limited to 30/min/token; an hourly tick gives
// plenty of headroom while making meaningful progress on a fleet.
func TestSearchResolveIntervalDefaultIsSane(t *testing.T) {
	src := mustReadSchedulerSource(t)
	// v0.25.37: the 1-hour default lives in the CollectionConfig
	// accessor ONLY. Re-defaulting here is the double-clamp incident
	// class (mailing_list_backfill_months; scancode shutdown grace).
	if strings.Contains(src, "SearchResolveInterval == 0") {
		t.Error("scheduler must NOT re-default SearchResolveInterval — the accessor owns the default")
	}
}

// TestSearchResolveTickerInRun pins that scheduler.Run installs a
// ticker driving the new task.
func TestSearchResolveTickerInRun(t *testing.T) {
	src := mustReadSchedulerSource(t)
	body := extractRunBody(src)
	if body == "" {
		t.Fatal("could not locate Run body")
	}
	if !strings.Contains(body, "SearchResolveInterval") {
		t.Error("scheduler.Run must reference cfg.SearchResolveInterval to drive the periodic search-resolve ticker")
	}
}

// TestRunSearchResolveExists pins the goroutine entry point.
func TestRunSearchResolveExists(t *testing.T) {
	src := mustReadSchedulerSource(t)
	if !strings.Contains(src, "func (s *Scheduler) runSearchResolve(") {
		t.Error("scheduler must define runSearchResolve as the goroutine entry point — invoked by the SearchResolve ticker")
	}
}

// TestRunSearchResolveCallsSearchAndLink pins the function body
// includes both the search call and the LinkContributorToGitHubUser
// store call. Without both, the task would either fail to discover
// matches or fail to apply them.
func TestRunSearchResolveCallsSearchAndLink(t *testing.T) {
	src := mustReadSchedulerSource(t)
	idx := strings.Index(src, "func (s *Scheduler) runSearchResolve(")
	if idx < 0 {
		t.Skip("runSearchResolve not yet defined")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}

	if !strings.Contains(body, "SearchUserByEmail") {
		t.Error("runSearchResolve must call platform.Client.SearchUserByEmail")
	}
	if !strings.Contains(body, "LinkContributorToGitHubUser") {
		t.Error("runSearchResolve must call store.LinkContributorToGitHubUser to apply the resolved gh_user_id")
	}
	if !strings.Contains(body, "MarkContributorSearchAttempted") {
		t.Error("runSearchResolve must call store.MarkContributorSearchAttempted on the no-hit / error path " +
			"so unresolvable rows aren't re-searched every cycle")
	}
}
