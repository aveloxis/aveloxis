// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.33: FindReviewDBID must be repo-scoped. The pre-v0.25.33
// signature took only the platform review ID and looked it up globally
// — on a case-variant duplicate pair the same platform_review_id exists
// under BOTH repo_ids and the lookup returned an arbitrary copy,
// creating cross-repo bridge rows (winner review_comments pointing at
// loser reviews). Those cross-links are what broke the first production
// dedup-repos run. Scoping the lookup by repo_id makes it impossible to
// create new ones; `aveloxis dedup-repos` remaps the historical ones.

package db

import (
	"strings"
	"testing"
)

func TestFindReviewDBIDIsRepoScoped(t *testing.T) {
	body := extractFunctionBody(t, "postgres.go", "FindReviewDBID")

	if !strings.Contains(body, "repoID") {
		t.Fatal("FindReviewDBID must take a repoID parameter — the global lookup is " +
			"ambiguous whenever the same repository exists under two repo_ids and " +
			"silently creates cross-repo review bridge rows.")
	}
	if !strings.Contains(normWS(body), "repo_id = $") {
		t.Error("FindReviewDBID's SQL must filter on repo_id — the comment's parent " +
			"review MUST belong to the same repo as the comment.")
	}
}
