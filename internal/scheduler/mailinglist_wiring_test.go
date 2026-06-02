// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestSchedulerConfigHasMailingListFields — Config must carry the v0.26.0
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
	for _, needle := range []string{
		"MailingListEnabled:        cfg.Collection.MailingListEnabled",
		"cfg.Collection.MailingListCadenceDuration()",
		"cfg.Collection.MailingListMirrorHandlingOrDefault()",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("main.go must plumb %q", needle)
		}
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
