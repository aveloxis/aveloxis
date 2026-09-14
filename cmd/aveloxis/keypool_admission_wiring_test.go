// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestKeyPoolAdmissionWiring pins the ONE pool constructor's admission
// wiring (2026-09-12, review round): the GitHub pool takes all three
// knobs through their accessors (the single default layer, SR-10); the
// GitLab pool takes the pool-wide ceiling as a backstop and NO per-key
// ceiling — that number is derived from GitHub's per-key secondary
// limits, and applying it to a one-token GitLab fleet capped GitLab
// collection at four concurrent requests across the whole worker pool.
func TestKeyPoolAdmissionWiring(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/main.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func loadKeys("))
	for _, needle := range []string{
		"cfg.Collection.GitHubMaxInflightValue()",
		"cfg.Collection.GitHubMaxInflightPerKeyValue()",
		"cfg.Collection.GitHubBudgetForegroundReservePctValue()",
		"gh.SetAdmission(maxInflight, maxPerKey, reservePct)",
		"const glPerKey = 0",
		"gl.SetAdmission(maxInflight, glPerKey, reservePct)",
		`"max_inflight_per_key", maxPerKey`,
		`"gitlab_max_inflight_per_key", glPerKey`,
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("loadKeys must contain %q — the knobs flow through their accessors to the GitHub pool, and the GitLab pool gets the pool-wide ceiling only", needle)
		}
	}
	if strings.Contains(body, "gl.SetAdmission(maxInflight, maxPerKey") {
		t.Error("the GitHub-derived per-key ceiling must not be applied to the GitLab pool")
	}
	// The per-key value the GitLab call and its log line share is ONE
	// const (L10 pass on the review-round fixes): the call and the log
	// cannot drift apart, and the const's value is pinned to zero above.
}
