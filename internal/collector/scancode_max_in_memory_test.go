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
	// Constructor signature must accept the new parameter. Pin via
	// whitespace-tolerant regex on the function signature region so
	// gofmt column re-alignment doesn't break this.
	re := regexp.MustCompile(`func NewScancodeWorker\([^)]+maxInMemory\s+int`)
	if !re.MatchString(src) {
		t.Error("NewScancodeWorker constructor must take a maxInMemory int parameter so cmd/aveloxis/main.go can wire it from the config")
	}
}

func TestRunOnePassesMaxInMemoryFromField(t *testing.T) {
	srcBytes, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)

	// Negative pin: the pre-v0.25.2 hardcoded literal must be gone.
	// Otherwise the config knob does nothing.
	if strings.Contains(src, `"--max-in-memory", "5000"`) {
		t.Error("scancode_worker.go still contains the hardcoded \"--max-in-memory\", \"5000\" pair — v0.25.2 must source the value from w.maxInMemory, not a literal. The config knob does nothing as long as the literal is here.")
	}

	// Positive pin: the flag is built from the worker field. Match
	// the `--max-in-memory` flag adjacent to a strconv.Itoa or
	// fmt.Sprintf using w.maxInMemory.
	if !strings.Contains(src, `"--max-in-memory"`) {
		t.Error("scancode_worker.go must still pass --max-in-memory to scancode")
	}
	if !strings.Contains(src, "w.maxInMemory") {
		t.Error("runOne must reference w.maxInMemory when building the --max-in-memory flag — otherwise the value isn't operator-configurable")
	}
}

func TestSchedulerConfigHasScancodeMaxInMemory(t *testing.T) {
	srcBytes, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	if !strings.Contains(src, "ScancodeMaxInMemory") {
		t.Error("scheduler.Config must declare ScancodeMaxInMemory so the value flows from main.go through to NewScancodeWorker")
	}
}

func TestMainWiresScancodeMaxInMemory(t *testing.T) {
	srcBytes, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	if !strings.Contains(src, "ScancodeMaxInMemory:") {
		t.Error("cmd/aveloxis/main.go must populate scheduler.Config.ScancodeMaxInMemory from cfg.Collection.ScancodeMaxInMemoryOrDefault()")
	}
	if !strings.Contains(src, "ScancodeMaxInMemoryOrDefault()") {
		t.Error("cmd/aveloxis/main.go must call cfg.Collection.ScancodeMaxInMemoryOrDefault() — the accessor handles the legacy-config zero-value fallback")
	}
}
