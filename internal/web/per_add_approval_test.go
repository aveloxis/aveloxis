// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract pins for the v0.27.20 per-add approval surface on
// the web process (summary/15, Option A).

package web

import (
	"os"
	"strings"
	"testing"
)

func readWebSource(t *testing.T, name string) string {
	t.Helper()
	src, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(src)
}

// TestHandleAddRepoBatchesThroughAddReposToGroup pins that the bulk
// paste is ONE AddReposToGroup call (one approval unit per submit)
// carrying the configured auto-approve limit, not a per-URL loop over
// the legacy single-add.
func TestHandleAddRepoBatchesThroughAddReposToGroup(t *testing.T) {
	s := readWebSource(t, "server.go")
	start := strings.Index(s, "func (s *Server) handleAddRepo(")
	if start < 0 {
		t.Fatal("handleAddRepo not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "s.store.AddReposToGroup(") {
		t.Error("handleAddRepo must route the whole paste through ONE AddReposToGroup call")
	}
	if !strings.Contains(body, "AutoApproveAddLimitValue()") {
		t.Error("handleAddRepo must pass the configured web.auto_approve_add_limit")
	}
	if strings.Contains(body, "s.store.AddRepoToGroup(") {
		t.Error("handleAddRepo must not fall back to per-URL AddRepoToGroup calls — " +
			"that would split one paste into N approval units")
	}
}

// TestHandleAddOrgHonorsPendingOutcome pins that the immediate org
// scan fires ONLY when the registration actually happened (admin
// path) — a non-admin's pending request must not scan anything.
func TestHandleAddOrgHonorsPendingOutcome(t *testing.T) {
	s := readWebSource(t, "server.go")
	start := strings.Index(s, "func (s *Server) handleAddOrg(")
	if start < 0 {
		t.Fatal("handleAddOrg not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "out.Registered") {
		t.Error("handleAddOrg must branch on OrgAddOutcome.Registered — scanning an " +
			"unregistered (pending) org would bypass the per-add approval rule")
	}
}

// TestScanOrgReposCarriesRejectedGate pins the v0.27.20 belt: a
// rejected group's orgs never scan. The pre-v0.27.20 code had NO
// status check here at all (the ApproveGroup comment claiming
// otherwise was false — audited 2026-07-16).
func TestScanOrgReposCarriesRejectedGate(t *testing.T) {
	s := readWebSource(t, "server.go")
	start := strings.Index(s, "func (s *Server) scanOrgRepos(")
	if start < 0 {
		t.Fatal("scanOrgRepos not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "GetGroupStatus") || !strings.Contains(body, `"rejected"`) {
		t.Error("scanOrgRepos must skip scanning when the owning group is 'rejected' — " +
			"the group-level abuse lever must stop org-driven enqueue")
	}
}

// TestAddRequestAdminRoutesRegistered pins the web admin decision
// routes + their requireAdmin gating.
func TestAddRequestAdminRoutesRegistered(t *testing.T) {
	s := readWebSource(t, "server.go")
	for _, needle := range []string{
		`mux.HandleFunc("POST /admin/add-requests/{id}/approve", s.requireAdmin(s.handleApproveAddRequest))`,
		`mux.HandleFunc("POST /admin/add-requests/{id}/reject", s.requireAdmin(s.handleRejectAddRequest))`,
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("server.go must register (admin-gated): %s", needle)
		}
	}
}

// TestApprovalProcessingRunsInBackground pins that a large approval
// never blocks the admin's request: item processing and the approved
// org scan run under safego goroutines from the shared decision flow.
func TestApprovalProcessingRunsInBackground(t *testing.T) {
	s := readWebSource(t, "admin.go")
	start := strings.Index(s, "func (s *Server) decideAddRequest(")
	if start < 0 {
		t.Fatal("decideAddRequest not found in admin.go")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	for _, needle := range []string{"safego.Go", "ProcessApprovedAddRequest", "scanOrgRepos", "SendAddRequestDecided"} {
		if !strings.Contains(body, needle) {
			t.Errorf("decideAddRequest missing %q — approval must process items/scan the org "+
				"in the background and notify the requester", needle)
		}
	}
}
