// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector"
)

// v0.20.7 (Fix E): the "no data collected" heuristic in
// buildOutcome was ignoring facadeResult.Commits when deciding
// whether to mark a repo as failure. Result: ~100 small-but-real
// repos in the May 9–12 production log got success=false with
// errMsg="no data collected (possible API auth failure or empty
// repo)" even though facade had successfully walked their git
// history. Examples: biocorecrg/ggplot2_functions (9 commits,
// 5 source files), microsoft/WARA-RelQ (2 commits, 2 files).
// These repos were re-collected from since=zero every cycle,
// burning API quota and never escaping the false-failure state.

// TestBuildOutcome_RepoWithCommitsButNoAPIData pins the
// behavioral contract: a repo with non-zero facade commits and
// zero API entity counts must mark success=true. The heuristic
// docstring intent ("auth failure OR truly empty") is preserved
// — facade succeeding with N commits proves it's neither.
func TestBuildOutcome_RepoWithCommitsButNoAPIData(t *testing.T) {
	var s Scheduler

	// Mirror the production shape: API returns no data (small repo
	// with no issues/PRs/contributors visible to the API) but
	// facade walks 9 real commits.
	result := &collector.CollectResult{
		Issues:       0,
		PullRequests: 0,
		Messages:     0,
		Events:       0,
		Releases:     0,
		Contributors: 0,
	}
	facade := &collector.FacadeResult{Commits: 9}

	outcome := s.buildOutcome(result, facade, nil, nil, nil)

	if !outcome.success {
		t.Errorf("buildOutcome must mark success=true when facade.Commits > 0, even if all API entity counts are zero — pre-v0.20.7 the heuristic flagged ~100 small-but-real repos like biocorecrg/ggplot2_functions (9 commits, 0 API data) as failures and re-collected them daily from since=zero. Got success=%v errMsg=%q", outcome.success, outcome.errMsg)
	}
	if outcome.errMsg != "" {
		t.Errorf("buildOutcome with facade.Commits > 0 and no errors must leave errMsg empty (avoids polluting last_error). Got errMsg=%q", outcome.errMsg)
	}
}

// TestBuildOutcome_TrulyEmptyRepoStaysFailure pins the
// negative: a repo with zero data EVERYWHERE (facade also zero,
// nil, or no commits) still marks success=false. The heuristic
// originally exists to catch silent auth failures and "this
// repo is broken" cases. We want Fix E to relax the heuristic
// only when facade proves the repo is real, not to disable it.
func TestBuildOutcome_TrulyEmptyRepoStaysFailure(t *testing.T) {
	var s Scheduler

	result := &collector.CollectResult{}
	// No facade result (nil) — the most common shape for a repo
	// where neither API nor facade produced data.
	outcome := s.buildOutcome(result, nil, nil, nil, nil)

	if outcome.success {
		t.Error("buildOutcome with zero API data AND no facade commits must mark success=false — that's the genuine 'truly empty or auth failure' case the heuristic exists to catch")
	}
	if outcome.errMsg == "" {
		t.Error("buildOutcome must populate errMsg with the 'no data collected' message so operators can SQL-query last_error for affected repos")
	}

	// Also pin the zero-commits FacadeResult case (facade ran but
	// found no commits — e.g. git log exit 128 from an empty bare
	// clone). buildOutcome can't distinguish a zero-commit facade
	// result from a nil one, and shouldn't try.
	outcome2 := s.buildOutcome(result, &collector.FacadeResult{Commits: 0}, nil, nil, nil)
	if outcome2.success {
		t.Error("buildOutcome with facade.Commits == 0 and zero API data must mark success=false — zero commits is the same signal as no facade at all")
	}
}

// TestBuildOutcome_HeuristicChecksFacadeCommits is the source-
// contract pin: the "no data collected" check must reference
// facadeResult or out.commits. The exact form may evolve (one
// could check facadeResult != nil && facadeResult.Commits > 0,
// or after the assignment out.commits > 0), but the substring
// "commits" inside the heuristic block is the load-bearing
// signal that the gate was widened.
func TestBuildOutcome_HeuristicChecksFacadeCommits(t *testing.T) {
	data, err := readSchedulerSource()
	if err != nil {
		t.Fatal(err)
	}

	// Find the "no data collected" gate by anchoring on its
	// distinctive error message.
	gateIdx := strings.Index(data, `"no data collected`)
	if gateIdx < 0 {
		t.Fatal(`cannot find "no data collected" heuristic in scheduler.go`)
	}
	// Scan backward to the outer `if` statement whose body contains
	// the gate. LastIndex of "if " picks the inner `if out.errMsg
	// == ""` first; we need the one that gates the whole block. Walk
	// back through both `if` tokens by anchoring on the
	// `out.releases == 0` substring that uniquely identifies the
	// outer condition.
	condIdx := strings.LastIndex(data[:gateIdx], "out.releases == 0")
	if condIdx < 0 {
		t.Fatal("cannot find outer condition of the 'no data collected' gate (substring 'out.releases == 0' missing)")
	}
	// Capture from the start of the outer if to the gate body so
	// the condition itself is in scope for the substring check.
	ifStart := strings.LastIndex(data[:condIdx], "if ")
	if ifStart < 0 {
		t.Fatal("cannot find 'if ' before the outer condition")
	}
	block := data[ifStart:gateIdx]

	if !strings.Contains(block, "commits") && !strings.Contains(block, "Commits") {
		t.Errorf("the 'no data collected' heuristic gate must reference facade commits so repos with real git history aren't flagged as failures. Block was:\n%s", block)
	}
}

// readSchedulerSource reads scheduler.go for source-contract
// substring matching used by Fix E pins.
func readSchedulerSource() (string, error) {
	b, err := os.ReadFile("scheduler.go")
	return string(b), err
}
