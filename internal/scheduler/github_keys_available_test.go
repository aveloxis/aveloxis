// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGitHubKeysAvailable pins worklist items 40 and 21: loadKeys builds a
// non-nil, EMPTY GitHub pool when only GitLab keys are configured, so every
// `s.ghKeys == nil` / `s.ghClient == nil` gate let the GitHub-only tickers
// run — breadth then stamped whole batches on the pool's "no API keys"
// error (item 40), the enrichment, search and sender tickers logged an
// unanswered WARN per tick, and the distribution worker struck GitHub repos
// toward the sideline (item 21). One predicate answers "can a GitHub call
// be made at all": a pool with at least one non-invalidated key.
func TestGitHubKeysAvailable(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	if (&Scheduler{}).githubKeysAvailable() {
		t.Error("a nil pool reads as available")
	}
	if (&Scheduler{ghKeys: platform.NewKeyPool(nil, lg)}).githubKeysAvailable() {
		t.Error("an empty pool (GitLab-only keys) reads as available")
	}
	pool := platform.NewKeyPool([]string{"ghp_probe"}, lg)
	s := &Scheduler{ghKeys: pool}
	if !s.githubKeysAvailable() {
		t.Error("a pool with a key reads as unavailable")
	}
	keys, _ := pool.Snapshot()
	if len(keys) != 1 {
		t.Fatalf("%d keys", len(keys))
	}
	// Invalidate the only key through the pool's own path.
	key, release, err := pool.Acquire(t.Context(), platform.ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	release()
	pool.InvalidateKey(key)
	if s.githubKeysAvailable() {
		t.Error("a pool whose every key was invalidated reads as available")
	}
}

// TestGitLabKeysAvailable (PR #218 review E15): the GitLab twin reads the
// GitLab pool, never the GitHub one, with the same nil / empty / keyed
// answers.
func TestGitLabKeysAvailable(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	github := platform.NewKeyPool([]string{"ghp_probe"}, lg)
	if (&Scheduler{ghKeys: github}).gitlabKeysAvailable() {
		t.Error("a nil GitLab pool reads as available (or the GitHub pool was read)")
	}
	if (&Scheduler{ghKeys: github, glKeys: platform.NewKeyPool(nil, lg)}).gitlabKeysAvailable() {
		t.Error("an empty GitLab pool reads as available")
	}
	if !(&Scheduler{glKeys: platform.NewKeyPool([]string{"glpat-probe"}, lg)}).gitlabKeysAvailable() {
		t.Error("a GitLab pool with a key reads as unavailable")
	}
}

// TestGitHubOnlyTasksGateOnUsableKeys is the consumer sweep for the same
// state (the empty-pool class took seven review passes one site at a time
// before): every scheduler entry point that reaches the GitHub client or
// pool names githubKeysAvailable(), not a nil check. It is a source sweep
// (a token off the control path would pass it); the predicate's own
// behaviour is TestGitHubKeysAvailable, and the sender resolver's gate sits
// on its API tail, not its loop (review round 1). The denominator is the
// functions examined, so a new GitHub-only ticker without the gate fails.
func TestGitHubOnlyTasksGateOnUsableKeys(t *testing.T) {
	sites := map[string][]string{
		"internal/scheduler/scheduler.go":               {"func (s *Scheduler) runBreadth(", "func (s *Scheduler) runSearchResolve(", "func (s *Scheduler) runEnrichment(", "func (s *Scheduler) refreshGitHubOrg(", "func (s *Scheduler) refreshUserOrgs("},
		"internal/scheduler/mailinglist_wiring.go":      {"func (s *Scheduler) senderResolvePass("},
		"internal/scheduler/activity_classification.go": {"func (s *Scheduler) runActivityClassification("},
		"internal/scheduler/activity_history.go":        {"func (s *Scheduler) runActivityHistory("},
		"internal/scheduler/distribution_wiring.go":     {"func (s *Scheduler) spawnDistributionWorker("},
		"internal/scheduler/keypool_summary.go":         {"func (s *Scheduler) logIdleGitHubTasks("},
		"internal/scheduler/repo_metadata_backfill.go":  {"func (s *Scheduler) runRepoMetadataBackfill("},
		// v0.29.70 whole-branch review: the block-notice fetch (runJob's 451
		// sideline and the gone recheck both reach it through here).
		"internal/scheduler/forge_notice.go": {"func (s *Scheduler) captureBlockNotice("},
	}
	examined := 0
	for file, sigs := range sites {
		src := srctest.Read(t, file)
		for _, sig := range sigs {
			body := srctest.StripGoComments(srctest.FuncBody(t, src, sig))
			examined++
			if !strings.Contains(body, "githubKeysAvailable()") {
				t.Errorf("%s in %s reaches GitHub without gating on githubKeysAvailable() — an empty GitHub pool (GitLab-only keys) is not nil", sig, file)
			}
		}
	}
	srctest.MinCount(t, "GitHub-only entry points", examined, 12)
}

// TestGitHubGatesKeepTheirDecidedShapes pins the three shapes review round 1
// of items 40/21 reversed, so they cannot drift back: (1) the sender
// resolver's gate sits on its API TAIL, not on its loop — two of its three
// stages need no key and it is what links DB-resolvable senders; (2)
// enrichment never falls through to the GitLab client — GetThinContributorLogins
// has no platform filter, so GitHub logins would be looked up on GitLab and a
// same-named GitLab user's profile merged onto them (SR-6; item 62); (3) the
// distribution worker keeps its GitHub source — a registries-only scan
// COMPLETES and wipes the repository's GitHub-sourced rows for the cadence,
// where a failed scan strikes and keeps the snapshot (v0.29.55 decision (a)).
func TestGitHubGatesKeepTheirDecidedShapes(t *testing.T) {
	wiring := srctest.Read(t, "internal/scheduler/mailinglist_wiring.go")
	loop := srctest.StripGoComments(srctest.FuncBody(t, wiring, "func (s *Scheduler) runMailingListSenderResolve("))
	if !strings.Contains(loop, "time.NewTicker(") || !strings.Contains(loop, "s.senderResolvePass(ctx)") || strings.Contains(loop, "githubKeysAvailable()") {
		t.Error("runMailingListSenderResolve must run senderResolvePass from its ticker and gate nothing itself: the DB stages resolve senders without a key")
	}
	sender := srctest.StripGoComments(srctest.FuncBody(t, wiring, "func (s *Scheduler) senderResolvePass("))
	if !strings.Contains(sender, "githubKeysAvailable()") {
		t.Error("senderResolvePass must gate its API tail per pass (inside the loop by construction)")
	}
	if !strings.Contains(sender, "apiClient = s.ghClient") {
		t.Error("runMailingListSenderResolve must pass the GitHub client to ResolveEmailToIdentity only when a usable key exists (a nil client skips the API tail)")
	}
	// The positive shapes (review round 2: a negative token was escapable by
	// selectClient(PlatformGitLab) or a second variable): enrichment's ONLY
	// client binding is the GitHub client, and the scanner receives the
	// type-asserted GitHub client itself, with no nil assignment anywhere in
	// the wiring function.
	enrich := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) runEnrichment("))
	if strings.Count(enrich, "client :=") != 1 || !strings.Contains(enrich, "client := s.ghClient") || strings.Contains(enrich, "client =") || strings.Contains(enrich, "glClient") || strings.Contains(enrich, "selectClient(") {
		t.Error("runEnrichment must bind exactly `client := s.ghClient` and never the GitLab client: GetThinContributorLogins has no platform filter (SR-6; worklist item 62)")
	}
	// The distribution worker's GitHub source is asserted at RUNTIME on the
	// options the worker receives (TestDistributionWorkerReceivesTheGitHubSource,
	// through the newDistributionWorker seam) and on the options seam
	// (TestDistributionScannerKeepsTheGitHubSource); the seam's production
	// default and the ban on a direct constructor call are
	// TestDistributionWorkerSeamIsTheOnlyConstructor (review rounds 3–6, after
	// every source pin here was escaped by a respelling).
}
