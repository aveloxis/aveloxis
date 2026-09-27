// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRefreshUserOrgsRejectedGateFailsClosed pins worklist follow-up 2 (the
// PR #207 review): the rejected-group gate in refreshUserOrgs read a
// GetGroupStatus ERROR as "not rejected" (`serr == nil && status ==
// "rejected"`), so a transient DB error let a rejected group's org scan and
// enqueue. A lookup error is not "no" (SR-5): the error arm logs at ERROR and
// skips the org before anything is enqueued. The store is a concrete type
// here, so this pins the control flow: between the status lookup and the
// "rejected" comparison there is an error arm that continues.
func TestRefreshUserOrgsRejectedGateFailsClosed(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) refreshUserOrgs("))
	lookup := strings.Index(body, "GetGroupStatus(")
	rejected := strings.Index(body, `"rejected"`)
	if lookup < 0 || rejected < 0 || lookup > rejected {
		t.Fatal("refreshUserOrgs must look the group status up before comparing it with \"rejected\"")
	}
	between := body[lookup:rejected]
	if strings.Contains(between, `== nil &&`) {
		t.Error("refreshUserOrgs reads a GetGroupStatus error as \"not rejected\" (`serr == nil && status == \"rejected\"`); an error must skip the org (SR-5)")
	}
	if !strings.Contains(between, "!= nil") || !strings.Contains(between, ".logger.Error(") || !strings.Contains(between, "continue") {
		t.Error("refreshUserOrgs must handle a GetGroupStatus error before the \"rejected\" comparison: log at ERROR and continue past the org")
	}
}

// TestRefreshUserOrgsGroupLookupIsNotSilent pins the sibling of the gate
// (batch-2 review round 2): GetGroupIDForOrgRequest's error was a bare
// `continue` — no log, and on a cancelled context every org iterated
// silently. Same shape as the fixed arm: shutdown returns, an error is
// logged at ERROR and the org waits for the next tick.
func TestRefreshUserOrgsGroupLookupIsNotSilent(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) refreshUserOrgs("))
	i := strings.Index(body, "GetGroupIDForOrgRequest(")
	if i < 0 {
		t.Fatal("refreshUserOrgs no longer looks the org's group up")
	}
	arm := body[i:]
	if j := strings.Index(arm, "GetGroupStatus("); j > 0 {
		arm = arm[:j]
	}
	if !strings.Contains(arm, "context.Canceled") || !strings.Contains(arm, ".logger.Error(") {
		t.Error("the GetGroupIDForOrgRequest error arm must return on context.Canceled and log any other error at ERROR before continuing")
	}
	// The function's opening lookup (round 3): the same shape.
	opening := body[strings.Index(body, "GetOrgRequests("):i]
	if !strings.Contains(opening, "context.Canceled") || !strings.Contains(opening, ".logger.Error(") {
		t.Error("the GetOrgRequests error arm must return on context.Canceled and log any other error at ERROR — it was the one silent arm left in refreshUserOrgs")
	}
}
