// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestReviewCommentsProgressLogging — v0.22.4 item 2.
//
// The ListReviewComments loop in collectMessages can run for hours on
// large repos (zephyr: ~210K review comments at ~40K/hr ≈ 5h). Pre-
// v0.22.4 the loop produced zero log output for its entire duration,
// indistinguishable from a hang.
//
// Pin two requirements:
//
//  1. A dedicated local counter for review comments (NOT result.Messages,
//     which also counts issue comments + PR conversation comments and
//     would conflate the signals).
//  2. A progress log line "review comments progress" with owner/repo
//     gated by a modulo of the counter so the cadence is bounded.
func TestReviewCommentsProgressLogging(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the ListReviewComments loop. Expand backward 200 chars so
	// a `reviewCount := 0` declaration immediately above the for-range
	// is captured by the window (Go's range loop has no init clause).
	loopStart := strings.Index(code, "sc.client.ListReviewComments(")
	if loopStart < 0 {
		t.Fatal("could not find sc.client.ListReviewComments( call in staged.go")
	}
	winStart := loopStart - 200
	if winStart < 0 {
		winStart = 0
	}
	loopEnd := loopStart + 2000
	if loopEnd > len(code) {
		loopEnd = len(code)
	}
	window := code[winStart:loopEnd]

	// 1. Dedicated local counter — anything that looks like
	//    `reviewCount`, `reviewComments`, `rcCount`. We require it
	//    is declared (not just incremented) inside the window so a
	//    refactor that drops the declaration but leaves the
	//    increment fails the test.
	counterRE := regexp.MustCompile(`\b(reviewCount|reviewComments|rcCount)\s*:?=`)
	if !counterRE.MatchString(window) {
		t.Error("staged.go: ListReviewComments loop must use a dedicated " +
			"local counter (reviewCount/reviewComments/rcCount) so progress " +
			"is bounded by review-comments-staged-this-cycle, not the " +
			"conflated result.Messages count")
	}

	// 2. Progress log line.
	if !strings.Contains(window, `"review comments progress"`) {
		t.Error("staged.go: ListReviewComments loop must log " +
			"`review comments progress` with owner/repo so a multi-hour " +
			"silent iteration becomes visible")
	}

	// 3. Modulo gate so cadence is bounded.
	moduloRE := regexp.MustCompile(`%\s*\d{2,5}\s*==\s*0`)
	if !moduloRE.MatchString(window) {
		t.Error("staged.go: review comments progress log must be gated " +
			"by a modulo (e.g. count%1000 == 0) so it doesn't fire per-row")
	}

	// 4. owner/repo on the progress line itself (not just in scope).
	progIdx := strings.Index(window, `"review comments progress"`)
	if progIdx >= 0 {
		// Slice the log call window: from msg to its closing paren.
		callEnd := strings.Index(window[progIdx:], ")")
		if callEnd > 0 {
			call := window[progIdx : progIdx+callEnd]
			if !strings.Contains(call, `"owner"`) {
				t.Error("review comments progress log must include " +
					"slog key \"owner\"")
			}
			if !strings.Contains(call, `"repo"`) {
				t.Error("review comments progress log must include " +
					"slog key \"repo\"")
			}
		}
	}
}
