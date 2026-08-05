// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.27.52: newly registered orgs (admin direct add via portal/web, or
// admin approval of a pending org request) must be scanned within one
// poll tick instead of waiting up to 4 hours for orgRefreshTicker. The
// signal is the user_org_requests row itself: both registration paths
// insert it with last_scanned NULL, so the scheduler polls
// HasNeverScannedOrgs and launches a scoped scan on demand. No
// cross-process RPC — the DB row is the message.

// stripLineComments removes // comments so doc comments describing a
// pattern can't satisfy (or violate) a source pin (v0.21.5 lesson).
func stripLineComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

func schedulerFunc(t *testing.T, src, name string) string {
	t.Helper()
	start := strings.Index(src, "func (s *Scheduler) "+name+"(")
	if start < 0 {
		t.Fatalf("%s not found in scheduler.go", name)
	}
	rest := src[start:]
	if end := strings.Index(rest[1:], "\nfunc "); end >= 0 {
		rest = rest[:end+1]
	}
	return rest
}

func TestMaybeScanNewOrgsShape(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	body := stripLineComments(schedulerFunc(t, string(src), "maybeScanNewOrgs"))

	if !strings.Contains(body, "HasNeverScannedOrgs(") {
		t.Error("maybeScanNewOrgs must consult store.HasNeverScannedOrgs — the DB row is the demand signal")
	}
	probeIdx := strings.Index(body, "HasNeverScannedOrgs(")
	gateIdx := strings.Index(body, "dbHealthy.Load()")
	if gateIdx < 0 || gateIdx > probeIdx {
		t.Error("maybeScanNewOrgs must check s.dbHealthy.Load() BEFORE probing — " +
			"the same gate fillWorkerSlots uses, so a DB outage doesn't produce a " +
			"probe-failed WARN on every 10s poll tick (the nightly-restart storm class)")
	}
	// v0.27.83: the demand scan has its OWN single-flight flag. Sharing
	// userOrgScanActive with the 4h full pass meant a new registration
	// made mid-pass waited for the ENTIRE fleet pass to finish before
	// its ~10s pickup — the new-user first experience degraded linearly
	// with fleet size. Overlap between the two passes is safe: every
	// write in the scan path is idempotent (UpsertRepo / EnqueueRepo /
	// AddRepoToGroupByID are all ON CONFLICT), and the demand pass is
	// scoped to never-scanned rows so duplicate enumeration is bounded.
	if !strings.Contains(body, "singleFlight(&s.userOrgDemandActive") {
		t.Error("maybeScanNewOrgs must launch through singleFlight(&s.userOrgDemandActive, ...) — " +
			"its own flag, so a long 4h full pass can never starve the ~10s pickup of new registrations")
	}
	if strings.Contains(body, "singleFlight(&s.userOrgScanActive") {
		t.Error("maybeScanNewOrgs must NOT share userOrgScanActive with the full pass — " +
			"that is the pre-v0.27.83 starvation shape (new org adds waited hours behind the fleet pass)")
	}
	if !regexp.MustCompile(`refreshUserOrgs\(ctx,\s*true\)`).MatchString(body) {
		t.Error("maybeScanNewOrgs must run the SCOPED scan (refreshUserOrgs(ctx, true)) — " +
			"a demand trigger should not re-enumerate every long-tracked org")
	}
}

func TestPollLoopTriggersDemandOrgScan(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	body := stripLineComments(schedulerFunc(t, string(src), "Run"))

	pollArm := strings.Index(body, "case <-pollTicker.C:")
	if pollArm < 0 {
		t.Fatal("pollTicker arm not found in Run")
	}
	nextCase := strings.Index(body[pollArm+1:], "case <-")
	arm := body[pollArm:]
	if nextCase >= 0 {
		arm = body[pollArm : pollArm+1+nextCase]
	}
	if !strings.Contains(arm, "maybeScanNewOrgs(") {
		t.Error("Run's pollTicker arm must call maybeScanNewOrgs so new org registrations " +
			"are scanned within one poll tick (default 10s), not up to 4h later")
	}
}

func TestUserOrgRefreshTickerKeepsOwnSingleFlightGuard(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	body := stripLineComments(schedulerFunc(t, string(src), "Run"))

	tickerArm := strings.Index(body, "case <-orgRefreshTicker.C:")
	if tickerArm < 0 {
		t.Fatal("orgRefreshTicker arm not found in Run")
	}
	nextCase := strings.Index(body[tickerArm+1:], "case <-")
	arm := body[tickerArm:]
	if nextCase >= 0 {
		arm = body[tickerArm : tickerArm+1+nextCase]
	}
	if !strings.Contains(arm, "singleFlight(&s.userOrgScanActive") {
		t.Error("the orgRefreshTicker's refreshUserOrgs invocation must route through " +
			"singleFlight(&s.userOrgScanActive, ...) — full passes must never overlap " +
			"each other (the demand scan runs under its own flag since v0.27.83)")
	}
	if !regexp.MustCompile(`refreshUserOrgs\(ctx,\s*false\)`).MatchString(arm) {
		t.Error("the ticker pass must stay UNSCOPED (refreshUserOrgs(ctx, false)) — " +
			"it is what discovers new repos in long-tracked orgs")
	}
}

func TestRefreshUserOrgsScopedFilter(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	body := stripLineComments(schedulerFunc(t, string(src), "refreshUserOrgs"))

	if !strings.Contains(body, "onlyNeverScanned bool") {
		t.Error("refreshUserOrgs must take an onlyNeverScanned bool so the demand path " +
			"can scan just the new registrations")
	}
	if !strings.Contains(body, "LastScanned == nil") {
		t.Error("the scoped branch must filter orgs to LastScanned == nil")
	}
}
