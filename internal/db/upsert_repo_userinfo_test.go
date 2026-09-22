// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57, Copilot review 5261384568. The store is the layer that owns
// repo_git (SR-18): whatever path a URL arrives by — GUI, portal API, CLI,
// a foundation loader, an approval — a URL carrying credentials is refused
// HERE, before any statement runs, so no writer can store one. A pure test:
// the refusal precedes the first use of the pool, which this store lacks.
func TestUpsertRepoRefusesAURLWithUserinfo(t *testing.T) {
	s := &PostgresStore{}
	for _, u := range []string{
		"https://user:token@github.com/owner/name",
		"https://token@gitlab.com/group/project",
		"https://user@git.example.invalid/owner/name.git",
		// schemeless web pastes reach UpsertRepo raw through the portal API
		// (review 5267408933); SCP is accepted (below)
		"user:token@github.com/owner/name",
		"token@github.com/owner/name",
	} {
		_, err := s.UpsertRepo(context.Background(), &model.Repo{
			Platform: model.PlatformGitHub, GitURL: u, Owner: "owner", Name: "name",
		})
		if !errors.Is(err, platform.ErrURLUserinfo) {
			t.Errorf("UpsertRepo(%q) = %v, want platform.ErrURLUserinfo", u, err)
		}
	}
	// An SCP clone URL is not credentials: it reaches the pool (nil here),
	// which is the proof it passed the refusal.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("UpsertRepo(SCP URL) never reached the pool — it was refused as credentials")
			}
		}()
		_, _ = s.UpsertRepo(context.Background(), &model.Repo{Platform: model.PlatformGenericGit, GitURL: "git@github.com:owner/name.git", Owner: "owner", Name: "name"})
	}()
}
