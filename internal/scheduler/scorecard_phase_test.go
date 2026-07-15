// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 — scorecard phase extraction + remote-primary contract.
// Source-contract pins on runScorecardPhase: the Phase 4b block is
// extracted out of runFacadeAndAnalysis, mode order is remote-primary
// for GitHub, tokens come from the pool without any per-key checkout,
// and the per-attempt timeout knob is threaded through.

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// schedulerFuncBody extracts the body of a named function from
// scheduler.go with // line comments stripped — the v0.21.5 lesson:
// negative pins must not false-match tokens that appear only in
// explanatory comments (runScorecardPhase's doc comment names the
// removed GetKey/MarkDepleted pattern on purpose).
func schedulerFuncBody(t *testing.T, funcName string) string {
	t.Helper()
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	marker := "func " + funcName
	idx := strings.Index(src, marker)
	if idx < 0 {
		t.Fatalf("cannot find %q in scheduler.go", marker)
	}
	body := src[idx:]
	if next := strings.Index(body[len(marker):], "\nfunc "); next > 0 {
		body = body[:len(marker)+next]
	}
	var sb strings.Builder
	for _, line := range strings.Split(body, "\n") {
		if c := strings.Index(line, "//"); c >= 0 {
			line = line[:c]
		}
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	return sb.String()
}

// TestRunScorecardPhaseExtracted pins the v0.27.5 refactor: Phase 4b
// lives in its own method and runFacadeAndAnalysis delegates to it
// instead of inlining the scorecard block.
func TestRunScorecardPhaseExtracted(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *Scheduler) runScorecardPhase(") {
		t.Fatal("scheduler.go must declare runScorecardPhase — the extracted Phase 4b")
	}

	facade := schedulerFuncBody(t, "(s *Scheduler) runFacadeAndAnalysis(")
	if !strings.Contains(facade, "s.runScorecardPhase(") {
		t.Error("runFacadeAndAnalysis must delegate Phase 4b to s.runScorecardPhase")
	}
	if strings.Contains(facade, "collector.RunScorecard(") {
		t.Error("runFacadeAndAnalysis must no longer inline the scorecard invocation — that lives in runScorecardPhase")
	}
}

// TestScorecardPhaseNoKeyCheckout is the v0.27.5 negative tripwire:
// the scorecard phase must NOT check a key out of the pool. The
// pre-v0.27.5 GetKey + MarkDepleted pattern handed scorecard ONE token
// shared with 40 collection workers — the measured cause of the
// multi-DAY remote hangs (scorecard sleeps through that token's
// rate-limit reset). Tokens now flow as a comma-separated multi-token
// GITHUB_TOKEN built by collector.ScorecardTokens from
// KeyPool.AllTokens, with no checkout and no depletion penalty.
func TestScorecardPhaseNoKeyCheckout(t *testing.T) {
	body := schedulerFuncBody(t, "(s *Scheduler) runScorecardPhase(")

	if strings.Contains(body, "GetKey(") {
		t.Error("runScorecardPhase must not call GetKey — scorecard gets the pooled " +
			"multi-token GITHUB_TOKEN (ScorecardTokens), never a single checked-out key")
	}
	if strings.Contains(body, "MarkDepleted(") {
		t.Error("runScorecardPhase must not call MarkDepleted — no key is checked out, " +
			"so there is nothing to deplete")
	}
	if !strings.Contains(body, "collector.ScorecardTokens(") {
		t.Error("runScorecardPhase must build GITHUB_TOKEN via collector.ScorecardTokens")
	}
	if !strings.Contains(body, "ScorecardTokenCountOrDefault()") {
		t.Error("runScorecardPhase must respect collection.scorecard_token_count via the accessor")
	}
}

// TestScorecardPhaseRemotePrimaryForGitHub pins the mode-order
// contract: GitHub repos (platform 1) get RemotePrimary; GitLab and
// generic-git stay local-only. Also pins the per-attempt timeout knob
// and the clone cleanup surviving the extraction.
func TestScorecardPhaseRemotePrimaryForGitHub(t *testing.T) {
	body := schedulerFuncBody(t, "(s *Scheduler) runScorecardPhase(")

	if !strings.Contains(body, "repo.Platform == model.PlatformGitHub") {
		t.Error("runScorecardPhase must set RemotePrimary from `repo.Platform == model.PlatformGitHub` — " +
			"GitLab/generic repos are local-only (scorecard's GitLab remote support is immature)")
	}
	if !strings.Contains(body, "RemotePrimary:") {
		t.Error("runScorecardPhase must pass RemotePrimary in ScorecardOptions")
	}
	if !strings.Contains(body, "ScorecardTimeout()") {
		t.Error("runScorecardPhase must thread collection.scorecard_timeout_minutes through the ScorecardTimeout accessor")
	}
	if !strings.Contains(body, "RemoveAll(") {
		t.Error("runScorecardPhase must clean up the retained analysis clone after scorecard finishes")
	}
}
