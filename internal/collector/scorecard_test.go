// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestScorecardResultHasExpectedFields verifies the ScorecardResult struct.
func TestScorecardResultHasExpectedFields(t *testing.T) {
	r := ScorecardResult{
		OverallScore: 7.5,
		Checks: []ScorecardCheck{
			{Name: "Code-Review", Score: 8, Reason: "Found 10/12 approved PRs"},
			{Name: "Maintained", Score: 10, Reason: "30 commits in last 90 days"},
		},
	}
	if r.OverallScore != 7.5 {
		t.Errorf("OverallScore = %f, want 7.5", r.OverallScore)
	}
	if len(r.Checks) != 2 {
		t.Fatalf("Checks length = %d, want 2", len(r.Checks))
	}
	if r.Checks[0].Name != "Code-Review" {
		t.Errorf("Checks[0].Name = %q, want %q", r.Checks[0].Name, "Code-Review")
	}
}

// TestRepoInfoModelHasCommunityFields verifies the model includes community profile fields.
func TestRepoInfoModelHasCommunityFields(t *testing.T) {
	info := repoInfoCommunityFields{
		ChangelogFile:     "CHANGELOG.md",
		ContributingFile:  "CONTRIBUTING.md",
		LicenseFile:       "LICENSE",
		CodeOfConductFile: "CODE_OF_CONDUCT.md",
		SecurityIssueFile: "SECURITY.md",
		SecurityAuditFile: "",
	}
	if info.ChangelogFile != "CHANGELOG.md" {
		t.Errorf("ChangelogFile = %q, want %q", info.ChangelogFile, "CHANGELOG.md")
	}
}

// repoInfoCommunityFields is a compile-time check that these fields exist.
// In the actual implementation, these fields are on model.RepoInfo.
type repoInfoCommunityFields struct {
	ChangelogFile     string
	ContributingFile  string
	LicenseFile       string
	CodeOfConductFile string
	SecurityIssueFile string
	SecurityAuditFile string
}

// TestRunScorecardAcceptsLocalPath verifies RunScorecard has a localPath parameter.
// When localPath is provided, scorecard should use --local instead of --repo.
func TestRunScorecardAcceptsLocalPath(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// RunScorecard must accept a localPath parameter.
	if !strings.Contains(code, "localPath string") {
		t.Error("RunScorecard must accept a localPath parameter for local scorecard execution")
	}
}

// TestRunScorecardUsesLocalFlag verifies that when localPath is non-empty,
// the scorecard command uses --local instead of --repo.
func TestRunScorecardUsesLocalFlag(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must contain logic for --local flag.
	if !strings.Contains(code, `"--local"`) {
		t.Error("RunScorecard must use --local flag when localPath is provided")
	}
}

// TestRunScorecardSetsRemoteForLocalMode verifies that when running in local
// mode, the code sets the git remote origin to the actual repo URL so scorecard
// can resolve the remote for API-dependent checks.
func TestRunScorecardSetsRemoteForLocalMode(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// When using --local, the temp clone's origin points to the bare repo,
	// not to GitHub. We must fix this so scorecard can make API calls.
	if !strings.Contains(code, "set-url") {
		t.Error("RunScorecard must set git remote origin to actual URL for local mode")
	}
}

// TestRunScorecardFallsBackToRepoFlag verifies that when localPath is empty,
// RunScorecard falls back to the original --repo behavior.
func TestRunScorecardFallsBackToRepoFlag(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must still contain --repo for fallback.
	if !strings.Contains(code, `"--repo"`) {
		t.Error("RunScorecard must fall back to --repo when localPath is empty")
	}
}

// TestAnalysisResultHasClonePath verifies AnalysisResult has a ClonePath field
// that the caller can use to pass the clone to scorecard before cleanup.
func TestAnalysisResultHasClonePath(t *testing.T) {
	r := AnalysisResult{
		Dependencies: 10,
		ClonePath:    "/tmp/some-clone",
	}
	if r.ClonePath != "/tmp/some-clone" {
		t.Errorf("ClonePath = %q, want /tmp/some-clone", r.ClonePath)
	}
}

// TestAnalysisCollectorRetainClone verifies AnalysisCollector has a RetainClone
// field that prevents automatic cleanup of the temporary clone.
func TestAnalysisCollectorRetainClone(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "RetainClone") {
		t.Error("AnalysisCollector must have a RetainClone field to retain the clone for scorecard")
	}

	// When RetainClone is true, ClonePath must be set in the result.
	if !strings.Contains(code, "ClonePath") {
		t.Error("analysis.go must populate ClonePath when RetainClone is true")
	}
}

// TestSchedulerRunsScorecardLocally verifies the scheduler passes the local
// clone path to RunScorecard and cleans up afterward.
func TestSchedulerRunsScorecardLocally(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Scheduler should set RetainClone on the analysis collector.
	if !strings.Contains(code, "RetainClone") {
		t.Error("scheduler must set RetainClone = true on AnalysisCollector")
	}

	// Scheduler should pass ClonePath to RunScorecard.
	if !strings.Contains(code, "ClonePath") {
		t.Error("scheduler must pass analysis result's ClonePath to RunScorecard")
	}

	// Scheduler must clean up the clone after scorecard finishes.
	if !strings.Contains(code, "RemoveAll") {
		t.Error("scheduler must clean up the retained clone after scorecard completes")
	}
}

// TestSchedulerReducedDepletionForLocalScorecard verifies the MarkDepleted
// penalty is reduced for local scorecard (fewer API calls than remote mode).
func TestSchedulerReducedDepletionForLocalScorecard(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// MarkDepleted(usedKey, 500) was the old value for remote scorecard.
	// Local scorecard makes far fewer API calls (~20-50 vs ~150-300),
	// so the depletion penalty should be reduced.
	if strings.Contains(code, "MarkDepleted(usedKey, 500)") {
		t.Error("MarkDepleted penalty should be reduced from 500 for local scorecard (fewer API calls)")
	}
}

// TestSchedulerScorecardSemaphoreBoundsSubprocesses — REWRITTEN
// 2026-09-12 to the superseding contract. The v0.21-era pin banned a
// scorecard semaphore on the premise that local scorecard is mostly
// disk I/O; since v0.27.5 the phase is REMOTE-primary (~40 GitHub API
// calls per run) and the chaoss.tv analysis measured 71,519 runs per
// 71,566 jobs — every cycle — sharing the key pool with 70 workers and
// the history sweep. A subprocess cannot hold a KeyPool lease, so the
// pool's in-flight ceilings cannot see it; the scheduler bounds the
// subprocess count instead (collection.scorecard_max_concurrent), and
// the slot is taken BEFORE the tokens are borrowed so a queued phase
// never holds tokens it is not using. Comment-stripped.
func TestSchedulerScorecardSemaphoreBoundsSubprocesses(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := srctest.StripGoComments(string(src))

	if !strings.Contains(code, "scorecardSem chan struct{}") {
		t.Error("Scheduler must carry `scorecardSem chan struct{}` — the subprocess ceiling for a phase the key pool's leases cannot see")
	}
	if !strings.Contains(code, "scorecardSem: make(chan struct{}, cfg.Collection.ScorecardMaxConcurrentValue())") {
		t.Error("NewWithKeys must size scorecardSem from cfg.Collection.ScorecardMaxConcurrentValue() — the accessor is the single default layer (SR-10)")
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, string(src), "func (s *Scheduler) runScorecardPhase("))
	acquire := strings.Index(body, "case s.scorecardSem <- struct{}{}:")
	release := strings.Index(body, "defer func() { <-s.scorecardSem }()")
	tokens := strings.Index(body, "collector.ScorecardTokens(")
	if acquire < 0 || release < 0 || tokens < 0 {
		t.Fatalf("runScorecardPhase must take a scorecardSem slot in a select (ctx-aware), defer its release, and borrow tokens: acquire=%d release=%d tokens=%d", acquire, release, tokens)
	}
	if !(acquire < release && release < tokens) {
		t.Errorf("order must be slot → deferred release → tokens (acquire=%d release=%d tokens=%d): tokens borrowed while queued for a slot are held by a phase that is not running", acquire, release, tokens)
	}
	if !strings.Contains(body[acquire:tokens], "case <-ctx.Done():") {
		t.Error("the slot wait must have a ctx.Done arm — a `stop serve` while queued for a scorecard slot must not hang the job")
	}
	if !strings.Contains(body, "defer releaseTokens()") {
		t.Error("the borrowed tokens must be returned to the pool (defer releaseTokens()) — LendTokens is accounted; a lost release over-reports the key as lent forever")
	}
}
