// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestScanOrgReposRejectedGateFailsClosed is the web twin of the scheduler's
// pin for worklist follow-up 2: scanOrgRepos read a GetGroupStatus ERROR as
// "not rejected" and scanned. The error arm logs at ERROR and returns before
// the scan starts (SR-5).
func TestScanOrgReposRejectedGateFailsClosed(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/web/server.go"), "func (s *Server) scanOrgRepos("))
	lookup := strings.Index(body, "GetGroupStatus(")
	rejected := strings.Index(body, `"rejected"`)
	if lookup < 0 || rejected < 0 || lookup > rejected {
		t.Fatal("scanOrgRepos must look the group status up before comparing it with \"rejected\"")
	}
	between := body[lookup:rejected]
	if strings.Contains(between, `== nil &&`) {
		t.Error("scanOrgRepos reads a GetGroupStatus error as \"not rejected\"; an error must stop the scan (SR-5)")
	}
	if !strings.Contains(between, "!= nil") || !strings.Contains(between, ".logger.Error(") || !strings.Contains(between, "return") {
		t.Error("scanOrgRepos must handle a GetGroupStatus error before the \"rejected\" comparison: log at ERROR and return")
	}
}

// TestLoginLogsAFailedAdminLookup pins worklist follow-up 6 at the login
// (batch-2 review round 2: the site the ratchet's line rule cannot see —
// a bare `isAdmin, err := ...` with no arm passes it). Between the lookup
// and createSession there is an error arm that logs at ERROR; the session
// is created either way, as a non-admin on the error.
func TestLoginLogsAFailedAdminLookup(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/web/server.go"), "func (s *Server) completeOAuthLogin("))
	lookup := strings.Index(body, "IsUserAdmin(")
	create := strings.Index(body, "createSession(")
	if lookup < 0 || create < 0 || lookup > create {
		t.Fatal("completeOAuthLogin must look the admin flag up before creating the session")
	}
	between := body[lookup:create]
	if !strings.Contains(between, "err != nil") || !strings.Contains(between, ".logger.Error(") {
		t.Error("completeOAuthLogin must log a failed admin-flag lookup at ERROR before creating the (non-admin) session")
	}
}
