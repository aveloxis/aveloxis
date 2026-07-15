// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// v0.21.0 — TestSchedulerRunStartsScancodeWorker pins the wiring
// between scheduler.Config's new scancode fields and the
// collector.ScancodeWorker. Without this test, a refactor that
// silently dropped the goroutine spawn would mean ScancodeWorker
// never runs and the cadence gate never elapses, but the operator
// would only notice when months of scancode data went stale on the
// dashboard.

func readSchedulerSourceForScancodeTest(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestSchedulerConfigHasScancodeFields(t *testing.T) {
	// v0.27.6: the per-knob accessor calls moved from the scheduler's
	// spawn site into the shared collector.ScancodeOptionsFromConfig
	// mapping (ONE mapping for both spawn sites — serve and the
	// dedicated `aveloxis scancode-worker` command). Pin every knob
	// there, and pin that the scheduler routes through the mapping
	// (plus the explicit-0 disable gate for the dedicated-host
	// recipe).
	optSrc, err := os.ReadFile("../collector/scancode_options.go")
	if err != nil {
		t.Fatal(err)
	}
	required := []string{
		"ScancodeWorkersOrDefault()",
		"ScancodeStartInterval()",
		"ScancodeCadence()",
		"ScancodeCloneDirOrDefault()",
		"ScancodeShutdownGrace()",
		"ScancodeRunTimeout()",
		"ScancodeRunTimeoutCap()",
		"ScancodeMaxInMemoryOrDefault()",
		"ScancodeTimeoutCapStrikesOrDefault()",
		"ScancodeIgnoreGlobsOrDefault()",
	}
	for _, accessor := range required {
		if !strings.Contains(string(optSrc), accessor) {
			t.Errorf("collector.ScancodeOptionsFromConfig must read %s. Without it, the corresponding aveloxis.json knob silently no-ops on BOTH spawn sites.", accessor)
		}
	}

	src := readSchedulerSourceForScancodeTest(t)
	if !strings.Contains(src, "ScancodeOptionsFromConfig(") {
		t.Error("scheduler.go must build the worker's options via collector.ScancodeOptionsFromConfig — a hand-rolled literal here can drift from the `aveloxis scancode-worker` spawn site")
	}
	if !strings.Contains(src, "ScancodeWorkers == 0") {
		t.Error("scheduler.go must gate the spawn on an EXPLICIT scancode_workers: 0 (the dedicated-scancode-host disable; docs/guide/dedicated-scancode-host.md)")
	}
}

func TestSchedulerRunStartsScancodeWorker(t *testing.T) {
	src := readSchedulerSourceForScancodeTest(t)
	if !strings.Contains(src, "NewScancodeWorker(") {
		t.Error("scheduler.go must instantiate collector.NewScancodeWorker. Without this call, the new worker pool never runs and scancode data goes permanently stale.")
	}
	// The worker must be launched as a goroutine — otherwise its
	// blocking Run() would stop the scheduler's Run() from
	// returning.
	if !strings.Contains(src, "go ") || !strings.Contains(src, ".Run(ctx)") {
		t.Error("scheduler.go must launch the ScancodeWorker as a goroutine (go sw.Run(ctx) or similar)")
	}
}

func TestMainWiresScancodeConfig(t *testing.T) {
	// v0.25.37: main.go passes the whole collection block; the scancode
	// knobs are read through cfg.Collection accessors inside the
	// scheduler (see TestSchedulerSpawnsScancodeWorker's needles). The
	// per-knob main.go plumb lines are gone by design.
	data, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "Collection: &cfg.Collection") {
		t.Error("main.go must pass Collection: &cfg.Collection into scheduler.Config — " +
			"without it every aveloxis.json collection knob silently no-ops")
	}
}
