// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestContributorsProgressLogging — v0.22.4 item 3.
//
// The ListContributors loop in StagedCollector.collectMetadataAndContributors
// can take hours on huge fleets (microsoft/vscode was empirically stuck for
// 7+ hours in this phase). Pre-v0.22.4 the loop was silent between the
// "collecting contributors" start and the "contributors staged" end —
// indistinguishable from a hang.
//
// Pin: a progress log line "contributors progress" with owner/repo gated by
// a modulo of result.Contributors (or an equivalent local counter).
func TestContributorsProgressLogging(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the ListContributors loop.
	loopStart := strings.Index(code, "sc.client.ListContributors(")
	if loopStart < 0 {
		t.Fatal("could not find sc.client.ListContributors( call in staged.go")
	}
	winStart := loopStart - 200
	if winStart < 0 {
		winStart = 0
	}
	loopEnd := loopStart + 1500
	if loopEnd > len(code) {
		loopEnd = len(code)
	}
	window := code[winStart:loopEnd]

	if !strings.Contains(window, `"contributors progress"`) {
		t.Error("staged.go: ListContributors loop must log " +
			"`contributors progress` with owner/repo so a multi-hour silent " +
			"iteration becomes visible — vscode's empirical 7h stall on " +
			"2026-05-16 had no observable progress signal during this phase")
	}

	// Modulo gate so cadence is bounded. Contributors lists are smaller
	// than review comments, so we accept any reasonable cadence.
	moduloRE := regexp.MustCompile(`%\s*\d{1,4}\s*==\s*0`)
	if !moduloRE.MatchString(window) {
		t.Error("staged.go: contributors progress log must be gated by " +
			"a modulo (e.g. count%100 == 0)")
	}

	// owner/repo on the progress line.
	progIdx := strings.Index(window, `"contributors progress"`)
	if progIdx >= 0 {
		callEnd := strings.Index(window[progIdx:], ")")
		if callEnd > 0 {
			call := window[progIdx : progIdx+callEnd]
			if !strings.Contains(call, `"owner"`) {
				t.Error("contributors progress log must include slog key \"owner\"")
			}
			if !strings.Contains(call, `"repo"`) {
				t.Error("contributors progress log must include slog key \"repo\"")
			}
		}
	}
}
