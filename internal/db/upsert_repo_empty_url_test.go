// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.30.0: a repository is addressed by its stored repo_git (the facade
// clones it, scorecard scores it) and nothing rebuilds a URL from the
// platform id any more, so a row with an empty repo_git could never be
// collected. The owning layer refuses to write one (SR-18): repos.repo_git
// is NOT NULL UNIQUE, which still admits exactly one empty string.
func TestUpsertRepoRefusesEmptyURL(t *testing.T) {
	s := &PostgresStore{} // the refusal must come before any database use
	for _, u := range []string{"", "   ", "/", ".git", " /.git "} {
		id, err := s.UpsertRepo(context.Background(), &model.Repo{Platform: model.PlatformGitHub, GitURL: u, Owner: "o", Name: "r"})
		if err == nil || id != 0 {
			t.Errorf("UpsertRepo(GitURL=%q) = (%d, %v), want a refusal", u, id, err)
		}
	}
}
