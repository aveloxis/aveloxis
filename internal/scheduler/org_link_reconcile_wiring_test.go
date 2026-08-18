// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestRefreshUserOrgsRunsLinkReconciliation pins the v0.27.93 self-heal
// wiring: after the enumeration loop, the FULL org-scan pass runs
// db.ReconcileOrgRepoLinks so tracked repos that entered the catalog
// through non-org-scan paths (mailing-list loaders, foundation importers,
// renames, GitLab orgs the scanner doesn't enumerate) still land in the
// registering groups within one pass. The 2026-08-18 production drift
// check found 9 live repos stranded this way — including
// gitlab.com/petsc/petsc, which enumeration can NEVER link because
// refreshUserOrgs only enumerates GitHub orgs.
//
// The 10-second demand probe (onlyNeverScanned=true) must NOT run the
// reconciler — it's a fleet-wide statement and the probe is a cheap
// new-registration pickup.
func TestRefreshUserOrgsRunsLinkReconciliation(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	src := string(data)

	start := strings.Index(src, "func (s *Scheduler) refreshUserOrgs(")
	if start < 0 {
		t.Fatal("refreshUserOrgs not found in scheduler.go")
	}
	body := src[start:]
	if end := strings.Index(body[1:], "\nfunc "); end >= 0 {
		body = body[:end+1]
	}

	callIdx := strings.Index(body, "ReconcileOrgRepoLinks(")
	if callIdx < 0 {
		t.Fatal("refreshUserOrgs must call ReconcileOrgRepoLinks — without " +
			"the reconciliation pass, repos created by non-org-scan paths " +
			"under tracked orgs stay unlinked forever")
	}
	// The call must be gated off the demand probe. Pin the gate variable
	// appearing in the 200 chars before the call.
	window := body[max(0, callIdx-200):callIdx]
	if !strings.Contains(window, "onlyNeverScanned") {
		t.Error("ReconcileOrgRepoLinks must be gated on the full pass " +
			"(!onlyNeverScanned) — the 10s demand probe must stay cheap")
	}
}
