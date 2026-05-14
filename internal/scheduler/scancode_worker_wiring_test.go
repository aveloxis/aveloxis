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
	// cmd/aveloxis/main.go's runServe must populate
	// scheduler.Config's scancode fields from
	// cfg.Collection.Scancode*. Without this, aveloxis.json edits
	// of scancode_workers / scancode_cadence_days / etc. silently
	// no-op.
	data, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, needle := range []string{
		"ScancodeWorkersOrDefault",
		"ScancodeStartInterval",
		"ScancodeCadence",
		"ScancodeCloneDirOrDefault",
		"ScancodeShutdownGrace",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("cmd/aveloxis/main.go must use cfg.Collection.%s when populating scheduler.Config — without this, the aveloxis.json scancode_* keys silently no-op", needle)
		}
	}
}
