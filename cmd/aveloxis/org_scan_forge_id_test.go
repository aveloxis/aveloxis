// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.103 — the add-repo org-expansion path captures the forge numeric
// repo ID. The v0.27.102 rename-dedup fix landed at the scheduler and web
// scan sites but MISSED this one (found by the 2026-08-19 fill audit's
// decoded-but-dropped sweep): listGitHubOrgRepos decoded the listing JSON
// and dropped `id`, so `aveloxis add-repo <orgURL>` could still mint
// rename duplicates. Same contract as
// internal/scheduler/org_scan_forge_id_test.go.
package main

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestAddRepoOrgExpansionCapturesForgeRepoID(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)

	// orgRepo carries the forge ID from lister to consumer.
	orgRepoDecl := srctest.TypeBody(t, s, "orgRepo")
	if !strings.Contains(orgRepoDecl, "ForgeID") {
		t.Error("orgRepo must carry ForgeID so the listers' decoded id reaches UpsertRepo")
	}

	for _, fn := range []string{"func listGitHubOrgRepos", "func listGitLabGroupRepos"} {
		body := srctest.FuncBody(t, s, fn)
		if !strings.Contains(body, "`json:\"id\"`") {
			t.Errorf("%s must decode the listing's numeric `id` field", fn)
		}
		if !strings.Contains(body, "ForgeID:") {
			t.Errorf("%s must populate orgRepo.ForgeID (via model.ForgeIDString)", fn)
		}
	}

	// The consumer passes it into UpsertRepo — which hosts both the
	// rename-heal (untracked URL + forge-ID hit) and the found-branch
	// backfill (prefer-nonempty ON CONFLICT).
	body := srctest.FuncBody(t, s, "func addOneRepoWithGroup(")
	if !strings.Contains(body, "PlatformID:") {
		t.Error("addOneRepoWithGroup must pass the forge ID into UpsertRepo via model.Repo.PlatformID")
	}
}
