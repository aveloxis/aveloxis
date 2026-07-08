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
	// v0.25.37: the mirror fields are gone — the wiring reads every
	// mailing-list knob through cfg.Collection accessors.
	src := readFile(t, "mailinglist_wiring.go") + readFile(t, "scheduler.go")
	for _, f := range []string{
		"s.cfg.Collection.MailingListEnabled",
		"s.cfg.Collection.MailingListWorkersOrDefault()",
		"s.cfg.Collection.MailingListCadenceDuration()",
		"s.cfg.Collection.MailingListBackfillMonthsOrDefault()",
		"s.cfg.Collection.MailingListPoliteEmail",
		"s.cfg.Collection.MailingListMirrorHandlingOrDefault()",
	} {
		if !strings.Contains(src, f) {
			t.Errorf("mailing-list wiring must read %s (accessor = single default point)", f)
		}
	}
}

// TestRunGatesMailingListWorker — the worker is spawned only when enabled,
// mirroring the distribution gate.
func TestRunGatesMailingListWorker(t *testing.T) {
	src := readFile(t, "scheduler.go")
	if !strings.Contains(src, "if s.cfg.Collection.MailingListEnabled {") {
		t.Error("Run must gate the mailing-list worker on s.cfg.Collection.MailingListEnabled")
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
	// v0.25.37: main.go no longer plumbs per-knob fields — it hands the
	// whole collection block to the scheduler, and the wiring reads the
	// accessors at the point of use. A single needle suffices: if the
	// Collection pointer is passed, every knob (present and future)
	// arrives without wiring.
	if !strings.Contains(src, "Collection: &cfg.Collection") {
		t.Error("main.go must pass Collection: &cfg.Collection into scheduler.Config " +
			"(v0.25.37 single-wiring-point contract)")
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
