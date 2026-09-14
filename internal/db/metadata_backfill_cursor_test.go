// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.30.0 review of B1–B5 (finding 11): ReposNeedingMetadataBackfill pages
// by repo_id, so rows that cannot be refreshed (generic git, a GitLab
// instance without keys, a forge 404) do not make the serve-startup loop
// re-read one page forever.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package db

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestReposNeedingMetadataBackfillPagesByCursor(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avmdcursor"
	cleanupCaseRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupCaseRepos(ctx, t, store, slug) })
	var ids []int64
	for _, n := range []string{"a", "b"} {
		id, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGenericGit, GitURL: "https://git.example.invalid/" + slug + "/" + n, Owner: slug, Name: n})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	seen := map[int64]bool{}
	after := ids[0] - 1
	for page := 0; page < 3; page++ {
		got, err := store.ReposNeedingMetadataBackfill(ctx, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			break
		}
		if got[0].RepoID <= after {
			t.Fatalf("page %d returned repo %d at or before the cursor %d", page, got[0].RepoID, after)
		}
		seen[got[0].RepoID] = true
		after = got[0].RepoID
	}
	if !seen[ids[0]] || !seen[ids[1]] {
		t.Errorf("paging by cursor visited %v, want both seeded repos %v", seen, ids)
	}
}
