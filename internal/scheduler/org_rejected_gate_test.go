// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestRefreshUserOrgsCarriesRejectedGate pins the v0.27.20 belt on the
// scheduler's org ticker: a 'rejected' group's org registrations never
// scan/enqueue. Registration itself is approval-gated (presence in
// user_org_requests = approved — AddOrgToGroup / DecideAddRequest own
// that), so this gate exists for the group-level abuse lever
// (RejectGroup). The pre-v0.27.20 code had NO status check on ANY
// org-scan path — the ApproveGroup comment claiming otherwise was
// false (audited 2026-07-16).
func TestRefreshUserOrgsCarriesRejectedGate(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	start := strings.Index(s, "func (s *Scheduler) refreshUserOrgs(")
	if start < 0 {
		t.Fatal("refreshUserOrgs not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "GetGroupStatus") || !strings.Contains(body, `"rejected"`) {
		t.Error("refreshUserOrgs must skip orgs whose owning group is 'rejected' — " +
			"without this, RejectGroup cannot stop org-driven enqueue")
	}
	// The gate must appear BEFORE the enqueue call so it actually
	// protects the queue, not just the logging.
	gateIdx := strings.Index(body, `"rejected"`)
	enqIdx := strings.Index(body, "EnqueueRepo")
	if gateIdx >= 0 && enqIdx >= 0 && gateIdx > enqIdx {
		t.Error("the rejected-group gate must run before EnqueueRepo in refreshUserOrgs")
	}
}
