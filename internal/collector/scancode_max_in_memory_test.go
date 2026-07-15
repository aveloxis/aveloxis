// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.25.2 — scancode --max-in-memory is plumbed through from
// CollectionConfig instead of the pre-v0.25.2 hardcoded "5000".
// On a terabyte-RAM production host the default cap forces
// scancode to spill scan state to disk much earlier than necessary;
// raising it speeds up monorepo scans without affecting low-memory
// dev hosts that keep the default.

func TestScancodeWorkerCarriesMaxInMemoryField(t *testing.T) {
	srcBytes, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	// Mirror the runTimeoutBase / runTimeoutCap fields. Whitespace-
	// tolerant match so future gofmt alignment doesn't break the
	// pin.
	re := regexp.MustCompile(`maxInMemory\s+int`)
	if !re.MatchString(src) {
		t.Error("ScancodeWorker must declare a `maxInMemory int` field — without it the v0.25.2 knob can't reach runOne")
	}
}

func TestNewScancodeWorkerAcceptsMaxInMemory(t *testing.T) {
	srcBytes, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	// v0.27.6: the constructor takes ScancodeWorkerOptions (the
	// 10-positional-parameter signature is gone — the second spawn
	// site made it an accident magnet). The knob must survive as an
	// options field, and the constructor must consume the struct.
	re := regexp.MustCompile(`MaxInMemory\s+int`)
	if !re.MatchString(src) {
		t.Error("ScancodeWorkerOptions must declare a MaxInMemory int field so both spawn sites can wire it from the config")
	}
	ctorRe := regexp.MustCompile(`func NewScancodeWorker\([^)]*opts ScancodeWorkerOptions\)`)
	if !ctorRe.MatchString(src) {
		t.Error("NewScancodeWorker must take a ScancodeWorkerOptions struct (v0.27.6) — a regrown positional-parameter list is the shape this test exists to prevent")
	}
	if !strings.Contains(src, "opts.MaxInMemory") {
		t.Error("NewScancodeWorker must consume opts.MaxInMemory (with the non-positive clamp) — otherwise the v0.25.2 knob silently no-ops")
	}
}

func TestRunOnePassesMaxInMemoryFromField(t *testing.T) {
	workerBytes, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	policyBytes, err := os.ReadFile("scancode_policy.go")
	if err != nil {
		t.Fatal(err)
	}
	worker := string(workerBytes)
	policy := string(policyBytes)

	// Negative pin: the pre-v0.25.2 hardcoded literal must be gone.
	// Otherwise the config knob does nothing.
	for name, src := range map[string]string{"scancode_worker.go": worker, "scancode_policy.go": policy} {
		if strings.Contains(src, `"--max-in-memory", "5000"`) {
			t.Errorf("%s still contains the hardcoded \"--max-in-memory\", \"5000\" pair — v0.25.2 must source the value from w.maxInMemory, not a literal. The config knob does nothing as long as the literal is here.", name)
		}
	}

	// Positive pin: the flag is built by the scancodeArgs helper
	// (v0.27.6 — the arg list moved to scancode_policy.go so the
	// --ignore glob handling is behaviorally testable) and the worker
	// feeds it w.maxInMemory.
	if !strings.Contains(policy, `"--max-in-memory"`) {
		t.Error("scancodeArgs (scancode_policy.go) must still pass --max-in-memory to scancode")
	}
	if !strings.Contains(worker, "w.maxInMemory") {
		t.Error("executeScan must reference w.maxInMemory when building the scancode args — otherwise the value isn't operator-configurable")
	}
}

func TestSchedulerConfigHasScancodeMaxInMemory(t *testing.T) {
	// v0.27.6: the accessor call moved from the scheduler spawn site
	// into the shared collector.ScancodeOptionsFromConfig mapping
	// (one mapping for both spawn sites). Pin the accessor there AND
	// that the scheduler routes through the mapping.
	optBytes, err := os.ReadFile("scancode_options.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(optBytes), "ScancodeMaxInMemoryOrDefault()") {
		t.Error("ScancodeOptionsFromConfig must read c.ScancodeMaxInMemoryOrDefault()")
	}
	schedBytes, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(schedBytes), "ScancodeOptionsFromConfig(") {
		t.Error("the scheduler's scancode spawn must build options via collector.ScancodeOptionsFromConfig — a hand-rolled options literal there can drift from the `aveloxis scancode-worker` spawn site")
	}
}

func TestMainWiresScancodeMaxInMemory(t *testing.T) {
	srcBytes, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	// v0.25.37: main.go hands the whole collection block to the
	// scheduler; the accessor is consumed at the spawn site instead.
	if !strings.Contains(src, "Collection: &cfg.Collection") {
		t.Error("cmd/aveloxis/main.go must pass Collection: &cfg.Collection into scheduler.Config")
	}
}
