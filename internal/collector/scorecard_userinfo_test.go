// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.29.57, Copilot review 5261384568: RunScorecard is the boundary to a
// subprocess that receives --repo on its command line and logs it. A repo
// URL carrying userinfo is refused before anything else happens — before
// the install check, so the contract holds whether or not scorecard is on
// PATH, and before any token is lent or any clone touched. The store refuses
// such a URL on write too (TestUpsertRepoRefusesAURLWithUserinfo); this arm
// is for rows that predate the refusal.
func TestRunScorecardRefusesARepoURLWithUserinfo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, u := range []string{
		"https://user:token@github.com/owner/name",
		"https://token@github.com/owner/name",
	} {
		res, err := RunScorecard(context.Background(), nil, 1, ScorecardOptions{
			RepoURL: u, RemotePrimary: true, GithubToken: "ghp_x", LocalPath: t.TempDir(),
		}, logger)
		if !errors.Is(err, platform.ErrURLUserinfo) {
			t.Errorf("RunScorecard(%q) = (%v, %v), want platform.ErrURLUserinfo", u, res, err)
		}
		if res != nil {
			t.Errorf("RunScorecard(%q) returned a result alongside the refusal", u)
		}
	}
}
