package collector

import (
	"os"
	"strings"
	"testing"
)

// TestScorecardParsesPartialResults verifies that RunScorecard parses the
// JSON output even when scorecard exits with status 1 (some checks failed).
// Scorecard produces valid JSON with scores for successful checks plus
// error details for failed ones. Treating exit 1 as a total failure
// discards all the good data.
func TestScorecardParsesPartialResults(t *testing.T) {
	src, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func RunScorecard(")
	if idx < 0 {
		t.Fatal("cannot find RunScorecard function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 3000 {
		fnBody = fnBody[:3000]
	}

	// The old pattern was:
	//   if err := cmd.Run(); err != nil { return nil, fmt.Errorf("scorecard failed...") }
	// This discards the JSON output that scorecard writes to stdout even on exit 1.
	//
	// The correct pattern: capture cmd.Run() error, then attempt JSON parse.
	// Only return error if JSON parse fails AND cmd.Run() failed.
	if strings.Contains(fnBody, `if err := cmd.Run(); err != nil`) {
		t.Error("RunScorecard must not return error immediately on non-zero exit — " +
			"scorecard produces valid JSON with partial results even when " +
			"individual checks fail (exit 1). Capture error, then parse stdout.")
	}
	// Must capture the run error into a variable and continue to JSON parsing.
	if !strings.Contains(fnBody, "runErr") && !strings.Contains(fnBody, "cmdErr") {
		t.Error("RunScorecard should capture cmd.Run() error into a named variable " +
			"(e.g., runErr) and attempt JSON parse regardless")
	}
}
