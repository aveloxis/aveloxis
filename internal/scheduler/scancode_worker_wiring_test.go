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
	src := readSchedulerSourceForScancodeTest(t)
	required := []string{
		"ScancodeWorkers",
		"ScancodeStartInterval",
		"ScancodeCadence",
		"ScancodeCloneDir",
		"ScancodeShutdownGrace",
	}
	for _, field := range required {
		if !strings.Contains(src, field) {
			t.Errorf("scheduler.Config must declare %s. Without it, the v0.21.0 scancode worker has no way to receive its config from aveloxis.json via cmd/aveloxis/main.go.", field)
		}
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
