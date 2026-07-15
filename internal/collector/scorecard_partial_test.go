// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestScorecardParsesPartialResults verifies that the scorecard invoke
// half parses the JSON output even when scorecard exits with status 1
// (some checks failed). Scorecard produces valid JSON with scores for
// successful checks plus error details for failed ones. Treating exit 1
// as a total failure discards all the good data.
//
// v0.27.5: the subprocess handling moved from RunScorecard into
// invokeScorecard (the invoke/persist split); this pin moved with it —
// same contract, new home.
func TestScorecardParsesPartialResults(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func invokeScorecard(")
	if idx < 0 {
		t.Fatal("cannot find invokeScorecard function (the v0.27.5 invoke half)")
	}
	// Scan only invokeScorecard's body, bounded by the next top-level
	// `func ` definition after it. The pre-v0.23.7 version capped
	// at a fixed byte offset (3000), which was fragile — adding
	// v0.23.7 cleanup comments pushed the `runErr` token past the
	// cap and broke the pin even though the contract was still
	// satisfied. The fix-attempt of bumping to 6000 then caught
	// the `cmd.Run()` in the `setRemoteOrigin` helper just below,
	// a false-positive. Bounding by the next `func ` keyword is
	// resilient to both.
	fnBody := code[idx:]
	if nextFn := strings.Index(fnBody[len("func invokeScorecard("):], "\nfunc "); nextFn > 0 {
		fnBody = fnBody[:len("func invokeScorecard(")+nextFn]
	}

	// The old pattern was:
	//   if err := cmd.Run(); err != nil { return nil, fmt.Errorf("scorecard failed...") }
	// This discards the JSON output that scorecard writes to stdout even on exit 1.
	//
	// The correct pattern: capture cmd.Run() error, then attempt JSON parse.
	// Only return error if JSON parse fails AND cmd.Run() failed.
	if strings.Contains(fnBody, `if err := cmd.Run(); err != nil`) {
		t.Error("invokeScorecard must not return error immediately on non-zero exit — " +
			"scorecard produces valid JSON with partial results even when " +
			"individual checks fail (exit 1). Capture error, then parse stdout.")
	}
	// Must capture the run error into a variable and continue to JSON parsing.
	if !strings.Contains(fnBody, "runErr") && !strings.Contains(fnBody, "cmdErr") {
		t.Error("invokeScorecard should capture the cmd.Wait() error into a named " +
			"variable (e.g., runErr) and attempt JSON parse regardless")
	}
}
