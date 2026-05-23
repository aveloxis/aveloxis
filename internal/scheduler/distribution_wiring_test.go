// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// v0.24.0 — source-contract tests pinning the scheduler wiring for
// the DistributionWorker.

func TestSchedulerConfigHasDistributionFields(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		"DistributionTrackingEnabled",
		"DistributionTrackingInterval",
		"DistributionTrackingWorkers",
		"DistributionTrackingStartInterval",
		"DistributionTrackingPoliteEmail",
		"DistributionTrackingUserAgent",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("scheduler.Config must declare %s for v0.24.0 DistributionWorker wiring", needle)
		}
	}
}

func TestSchedulerRunSpawnsWorkerWhenEnabled(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin: gate on DistributionTrackingEnabled, then call
	// spawnDistributionWorker. Off-by-default contract.
	if !strings.Contains(src, "if s.cfg.DistributionTrackingEnabled") {
		t.Error("scheduler.Run must guard distribution worker spawn on DistributionTrackingEnabled (off-by-default contract)")
	}
	if !strings.Contains(src, "spawnDistributionWorker") {
		t.Error("scheduler.Run must call spawnDistributionWorker when the gate passes")
	}
}

func TestSpawnDistributionWorkerExists(t *testing.T) {
	data, err := os.ReadFile("distribution_wiring.go")
	if err != nil {
		t.Fatalf("internal/scheduler/distribution_wiring.go must exist (Phase G deliverable): %v", err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *Scheduler) spawnDistributionWorker") {
		t.Error("distribution_wiring.go must declare Scheduler.spawnDistributionWorker")
	}
}

func TestSpawnDistributionWorkerComposesAllSources(t *testing.T) {
	data, err := os.ReadFile("distribution_wiring.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin: the wiring imports + constructs all three external clients
	// AND passes them to NewCompositeScanner.
	for _, needle := range []string{
		"depsdev.New",
		"ecosystems.New",
		"distribution.NewCompositeScanner",
		"distribution.NewWorker",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("distribution_wiring.go must include %q to compose the full scanner", needle)
		}
	}
}
