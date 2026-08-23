// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.102 — org scans capture the forge's numeric repo ID.
//
// The 2026-08-19 rename-dup audit: 12 data-bearing duplicate rows were
// minted by org scans re-discovering renamed/transferred repos under
// their NEW URLs. The listing JSON the scans already decode carries the
// numeric `id` — the only rename-proof identity — and every scan site
// must (a) decode it, (b) pass it into UpsertRepo (which now heals
// renames instead of duplicating), and (c) opportunistically backfill
// it onto already-tracked rows so the at-risk cohort gains protection
// without waiting for Phase 0 collection cycles.
package scheduler

import (
	"os"
	"strings"
	"testing"
)

func TestOrgScansCaptureForgeRepoID(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)

	for _, fn := range []string{
		"func (s *Scheduler) refreshGitHubOrg",
		"func (s *Scheduler) refreshGitLabGroup",
		"func (s *Scheduler) refreshUserOrgs",
	} {
		i := strings.Index(s, fn)
		if i < 0 {
			t.Fatalf("%s not found", fn)
		}
		body := s[i:]
		if j := strings.Index(body, "\nfunc "); j > 0 {
			body = body[:j]
		}
		if !strings.Contains(body, "`json:\"id\"`") {
			t.Errorf("%s must decode the listing's numeric `id` field", fn)
		}
		if !strings.Contains(body, "PlatformID:") {
			t.Errorf("%s must pass the forge ID into UpsertRepo via model.Repo.PlatformID", fn)
		}
		if !strings.Contains(body, "SetPlatformRepoIDIfEmpty(") {
			t.Errorf("%s must backfill the forge ID onto already-tracked rows (found branch)", fn)
		}
	}
}
