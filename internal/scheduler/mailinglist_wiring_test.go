// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestSchedulerConfigHasMailingListFields — Config must carry the v0.25.7
// mailing-list knobs so main.go can plumb them through.
func TestSchedulerConfigHasMailingListFields(t *testing.T) {
	src := readFile(t, "scheduler.go")
	for _, f := range []string{
		"MailingListEnabled", "MailingListWorkers", "MailingListCadence",
		"MailingListBackfillMonths", "MailingListPoliteEmail", "MailingListMirrorHandling",
	} {
		if !strings.Contains(src, f) {
			t.Errorf("scheduler.Config must declare %s", f)
		}
	}
}

// TestRunGatesMailingListWorker — the worker is spawned only when enabled,
// mirroring the distribution gate.
func TestRunGatesMailingListWorker(t *testing.T) {
	src := readFile(t, "scheduler.go")
	if !strings.Contains(src, "if s.cfg.MailingListEnabled {") {
		t.Error("Run must gate the mailing-list worker on s.cfg.MailingListEnabled")
	}
	if !strings.Contains(src, "s.spawnMailingListWorker(ctx)") {
		t.Error("Run must call s.spawnMailingListWorker(ctx) when enabled")
	}
}

// TestSpawnMailingListWorkerExists — the wiring helper builds the
// apache_ponymail backend + shared Pacer/Breaker and runs N runners.
func TestSpawnMailingListWorkerExists(t *testing.T) {
	src := readFile(t, "mailinglist_wiring.go")
	for _, needle := range []string{
		"func (s *Scheduler) spawnMailingListWorker(",
		"mailinglist.LoadSystems()",
		"mailinglist.NewPonyMail(",
		"collector.NewMailingListWorker(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("mailinglist_wiring.go must contain %q", needle)
		}
	}
}

// TestMainPlumbsMailingListConfig — the CLI maps CollectionConfig →
// scheduler.Config for the mailing-list knobs.
func TestMainPlumbsMailingListConfig(t *testing.T) {
	data, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// Whitespace-tolerant: gofmt re-aligns the struct literal when the longest
	// key changes, so pin the field reference, not the colon-to-value spacing.
	for _, needle := range []string{
		"MailingListEnabled:",
		"cfg.Collection.MailingListEnabled",
		"cfg.Collection.MailingListCadenceDuration()",
		"cfg.Collection.MailingListMirrorHandlingOrDefault()",
		"cfg.Collection.MailingListProcessorWorkersOrDefault()",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("main.go must plumb %q", needle)
		}
	}
}

// TestSignaledRepoResolutionWiredIntoOrgScan — §5c repo-side: a newly
// discovered repo must trigger signaled_repo_id backfill.
func TestSignaledRepoResolutionWiredIntoOrgScan(t *testing.T) {
	src := readFile(t, "scheduler.go")
	if c := strings.Count(src, "ResolveSignaledRepoForURL("); c < 2 {
		t.Errorf("both refreshGitHubOrg and refreshGitLabGroup must call ResolveSignaledRepoForURL on new repos (found %d call sites)", c)
	}
}

// TestSenderBackfillTickerScheduled — §5d: the sender-identity backfill must
// run on a periodic ticker so coverage improves over time.
func TestSenderBackfillTickerScheduled(t *testing.T) {
	src := readFile(t, "mailinglist_wiring.go")
	if !strings.Contains(src, "runMailingListSenderBackfill") ||
		!strings.Contains(src, "BackfillMailingListSenderIDs(") {
		t.Error("spawnMailingListWorker must start a periodic runMailingListSenderBackfill calling BackfillMailingListSenderIDs")
	}
}

// TestMultiSystemBackendWiring — Phase 3: the spawn must support both the
// Pony Mail and public-inbox backends, spawning a pool per system.
func TestMultiSystemBackendWiring(t *testing.T) {
	src := readFile(t, "mailinglist_wiring.go")
	if !strings.Contains(src, "mailinglist.NewPonyMail(") || !strings.Contains(src, "mailinglist.NewPublicInbox(") {
		t.Error("mailingListBackendFor must build both apache_ponymail (PonyMail) and lore_public_inbox (PublicInbox) backends")
	}
	if !strings.Contains(src, "for _, sys := range systems") {
		t.Error("spawnMailingListWorker must iterate system definitions (one pool per system), not hardcode apache")
	}
}

func readFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
