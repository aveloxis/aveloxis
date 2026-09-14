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
	src := srctest.Read(t, "cmd/aveloxis/forge_clients.go")
	gh := srctest.StripGoComments(srctest.FuncBody(t, src, "func loadGitHubKeyPool("))
	for _, needle := range []string{
		"cfg.Collection.GitHubMaxInflightValue()",
		"cfg.Collection.GitHubMaxInflightPerKeyValue()",
		"cfg.Collection.GitHubBudgetForegroundReservePctValue()",
		"pool.SetAdmission(maxInflight, maxPerKey, reservePct)",
		`"max_inflight_per_key", maxPerKey`,
	} {
		if !strings.Contains(gh, needle) {
			t.Errorf("loadGitHubKeyPool must contain %q — the knobs flow through their accessors to the GitHub pool", needle)
		}
	}
	// v0.30.0: one pool per GitLab instance, each with the pool-wide
	// ceiling and no per-key ceiling.
	gl := srctest.StripGoComments(srctest.FuncBody(t, src, "func buildForgeClients("))
	for _, needle := range []string{
		"cfg.Collection.GitHubMaxInflightValue()",
		"cfg.Collection.GitHubBudgetForegroundReservePctValue()",
		"const glPerKey = 0",
		"pool.SetAdmission(maxInflight, glPerKey, reservePct)",
		`"max_inflight_per_key", glPerKey`,
	} {
		if !strings.Contains(gl, needle) {
			t.Errorf("buildForgeClients must contain %q — every GitLab instance's pool gets the pool-wide ceiling only", needle)
		}
	}
	if strings.Contains(gl, "GitHubMaxInflightPerKeyValue") || strings.Contains(gl, "SetAdmission(maxInflight, maxPerKey") {
		t.Error("the GitHub-derived per-key ceiling must not be applied to a GitLab pool")
	}
	// The per-key value the GitLab call and its log line share is ONE
	// const (L10 pass on the review-round fixes): the call and the log
	// cannot drift apart, and the const's value is pinned to zero above.
}
