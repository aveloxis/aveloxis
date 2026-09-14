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
		// v0.30.0: the GitLab group listing lives on the instance's client
		// (gitlab.Client.ListGroupProjects decodes the id, pinned below);
		// the scheduler consumes GroupProject.ForgeID.
		decodes := "`json:\"id\"`"
		if fn == "func (s *Scheduler) refreshGitLabGroup" {
			decodes = "item.ForgeID"
		}
		i := strings.Index(s, fn)
		if i < 0 {
			t.Fatalf("%s not found", fn)
		}
		body := s[i:]
		if j := strings.Index(body, "\nfunc "); j > 0 {
			body = body[:j]
		}
		if !strings.Contains(body, decodes) {
			t.Errorf("%s must decode (or consume) the listing's numeric `id` field", fn)
		}
		if !strings.Contains(body, "PlatformID:") {
			t.Errorf("%s must pass the forge ID into UpsertRepo via model.Repo.PlatformID", fn)
		}
		if !strings.Contains(body, "SetPlatformRepoIDIfEmpty(") {
			t.Errorf("%s must backfill the forge ID onto already-tracked rows (found branch)", fn)
		}
	}
}

func TestGitLabGroupListingDecodesForgeRepoID(t *testing.T) {
	src, err := os.ReadFile("../platform/gitlab/group_projects.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "`json:\"id\"`") || !strings.Contains(string(src), "ForgeID: model.ForgeIDString(it.ID)") {
		t.Error("gitlab.Client.ListGroupProjects must decode the numeric project id into GroupProject.ForgeID")
	}
}
