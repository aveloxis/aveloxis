// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.102 — the web-side org scan captures the forge numeric repo ID
// (same contract as the scheduler scan sites; see
// internal/scheduler/org_scan_forge_id_test.go for the incident story).
package web

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestScanOrgReposCapturesForgeRepoID(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	i := strings.Index(s, "func (s *Server) scanOrgRepos")
	if i < 0 {
		t.Fatal("scanOrgRepos not found")
	}
	body := s[i:]
	if j := strings.Index(body, "\nfunc "); j > 0 {
		body = body[:j]
	}
	if !strings.Contains(body, "`json:\"id\"`") {
		t.Error("scanOrgRepos must decode the listing's numeric `id` field")
	}
	if !strings.Contains(body, "PlatformID:") {
		t.Error("scanOrgRepos must pass the forge ID into UpsertRepo via model.Repo.PlatformID")
	}
	// v0.29.63: the Seen form, with the listing's created_at, so a recorded
	// forge-ID change is dated (the plain form was accepted here and in the
	// scheduler pin, which let refreshUserOrgs skip the date).
	code := srctest.StripGoComments(body)
	if strings.Contains(code, "SetPlatformRepoIDIfEmpty(") {
		t.Error("scanOrgRepos calls the plain SetPlatformRepoIDIfEmpty — use SetPlatformRepoIDIfEmptySeen with the listing's created_at")
	}
	if !strings.Contains(code, "`json:\"created_at\"`") {
		t.Error("scanOrgRepos must decode the listing's created_at")
	}
	for _, call := range regexp.MustCompile(`SetPlatformRepoIDIfEmptySeen\(.*`).FindAllString(code, -1) {
		if !strings.Contains(call, "CreatedAt") {
			t.Errorf("%s: %q — the listing's CreatedAt must be the date argument", "scanOrgRepos", call)
		}
	}
	if !strings.Contains(code, "SetPlatformRepoIDIfEmptySeen(") {
		t.Error("scanOrgRepos must backfill the forge ID onto already-tracked rows (found branch)")
	}
}
