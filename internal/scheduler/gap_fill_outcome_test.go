package scheduler

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector"
)

// makeSuccessfulCollectResult returns a CollectResult that mimics a
// healthy main-collection outcome — non-zero data so the "no data
// collected" heuristic in buildOutcome doesn't fire and mask the
// gap-fill signal we're trying to surface.
func makeSuccessfulCollectResult() *collector.CollectResult {
	return &collector.CollectResult{
		Issues:       12,
		PullRequests: 45,
		Messages:     100,
		Events:       30,
		Releases:     2,
		Contributors: 8,
		Errors:       nil,
	}
}

// makeGapFillError returns an error shaped like the production logs:
// the prefix "PR gap fill error" wraps "gap fill PR batch" which wraps
// "graphql PR batch". shouldForceFullRecollect matches on the
// "graphql PR batch" substring so any of these wrappers triggers it.
func makeGapFillError() error {
	return errors.New("PR gap fill error: gap fill PR batch: graphql PR batch: graphql: exhausted 10 retries for https://api.github.com/graphql")
}

// callBuildOutcome is the test indirection that lets us update the
// argument order in one place if the implementation evolves. New args
// are appended to the end so legacy tests can stop at the prior arity.
func callBuildOutcome(s *Scheduler, result *collector.CollectResult, facadeResult *collector.FacadeResult, analysisResult *collector.AnalysisResult, collectionErr error, gapFillErr error) jobOutcome {
	return s.buildOutcome(result, facadeResult, analysisResult, collectionErr, gapFillErr)
}

// v0.20.5: Gap fill errors were previously logged at WARN and dropped on
// the floor — they never reached outcome.errMsg, so shouldForceFullRecollect
// could not fire and last_error stayed NULL in collection_queue. The result:
// repos with PR gaps got stuck in a perpetual loop where each incremental
// cycle re-detected the same gap, gap fill failed the same way, and the
// queue carried no SQL-queryable signal. These tests pin the new
// plumbing.

// TestBuildOutcomeFoldsGapFillError pins the behavioral contract: when a
// gap-fill error is passed in and the main collection error is nil, the
// outcome must reflect it as a failure with the gap-fill error message
// captured. Otherwise shouldForceFullRecollect — which matches on
// "graphql PR batch" substring — never fires for the gap-fill failure
// class.
func TestBuildOutcomeFoldsGapFillError(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// buildOutcome's signature must accept a gap-fill error parameter so
	// the call site in runJob can pass gfErr through. Naming may vary;
	// the parameter must be of type `error`. Pin via the function
	// signature substring.
	sigRE := regexp.MustCompile(`func\s+\(s\s+\*Scheduler\)\s+buildOutcome\([^)]*\)\s+jobOutcome`)
	m := sigRE.FindString(src)
	if m == "" {
		t.Fatal("could not find buildOutcome method signature")
	}
	// Count comma-separated parameters. The pre-v0.20.5 signature had 4
	// parameters (result, facadeResult, analysisResult, collectionErr).
	// The new signature must have at least 5.
	commas := strings.Count(m, ",")
	if commas < 4 {
		t.Errorf("buildOutcome signature %q has %d commas (=%d params) — must accept an additional gap-fill error parameter so runJob can fold the gfErr into the outcome",
			m, commas, commas+1)
	}
}

// TestRunJobFoldsGapFillErrorIntoOutcome pins that runJob captures the
// gap-fill error and passes it into buildOutcome. Without this wiring,
// the buildOutcome signature change is dead — the gap-fill error stays
// on the floor.
func TestRunJobFoldsGapFillErrorIntoOutcome(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find the runJob function body so we don't false-match in unrelated
	// functions. Anchor on the function declaration and the closing brace
	// of the next function definition.
	runJobStart := strings.Index(src, "func (s *Scheduler) runJob(")
	if runJobStart < 0 {
		t.Fatal("could not find runJob function")
	}
	// Find the next function definition after runJob to bound the search.
	nextFuncRE := regexp.MustCompile(`\nfunc `)
	tail := src[runJobStart+1:]
	loc := nextFuncRE.FindStringIndex(tail)
	if loc == nil {
		t.Fatal("could not find end of runJob body")
	}
	runJobBody := src[runJobStart : runJobStart+1+loc[0]]

	// Pin: runJob must declare a variable to capture gfErr at a scope
	// that survives the `if err == nil` block, and pass it to
	// buildOutcome at the call site. Two signals together.
	gfErrPersisted := strings.Contains(runJobBody, "gapFillErr") ||
		strings.Contains(runJobBody, "gapErr") ||
		strings.Contains(runJobBody, "lastGapFillErr")
	if !gfErrPersisted {
		t.Error("runJob must declare a persistent variable (e.g. gapFillErr) to hold the gap-fill error so it can be passed to buildOutcome — otherwise gfErr remains scoped to the if-block and gets dropped")
	}

	// buildOutcome must be called with one more arg than the pre-v0.20.5
	// signature. Match the call site and count commas.
	callRE := regexp.MustCompile(`s\.buildOutcome\(([^)]+)\)`)
	cm := callRE.FindStringSubmatch(runJobBody)
	if cm == nil {
		t.Fatal("could not find s.buildOutcome call in runJob")
	}
	commas := strings.Count(cm[1], ",")
	if commas < 4 {
		t.Errorf("s.buildOutcome call in runJob has %d commas (=%d args) — must pass the gap-fill error as an additional argument", commas, commas+1)
	}
}

// TestBuildOutcomeBehavior_GapFillErrorPopulatesErrMsg is the behavioral
// test: given a successful main collection result and a non-nil gap-fill
// error, buildOutcome must produce outcome.success=false and
// outcome.errMsg containing the gap-fill error message (which always
// starts with "PR gap fill error:" or "gap fill PR batch:" in
// production). This is what makes shouldForceFullRecollect fire.
func TestBuildOutcomeBehavior_GapFillErrorPopulatesErrMsg(t *testing.T) {
	// This test exercises the real buildOutcome method via the package's
	// jobOutcome internal type. The Scheduler instance doesn't need any
	// dependencies wired up — buildOutcome is a pure function over its
	// arguments.
	var s Scheduler

	// Build a successful collection result (no errors, non-zero counts so
	// the "no data collected" heuristic doesn't fire and mask the gap-fill
	// signal we're testing). The shape mirrors what the main collection
	// produces for a healthy repo with PRs and issues.
	successResult := makeSuccessfulCollectResult()
	gapErr := makeGapFillError()

	// Invoke buildOutcome with the new 5-arg signature. The test relies
	// on the implementation accepting a gap-fill-error parameter as the
	// last argument. Tests that pre-date Fix A continue to compile by
	// passing nil for this argument.
	outcome := callBuildOutcome(&s, successResult, nil, nil, nil, gapErr)

	if outcome.success {
		t.Error("buildOutcome must mark success=false when the gap-fill error is non-nil — without this, shouldForceFullRecollect (which is gated on !outcome.success) cannot trigger force_full_collect")
	}
	if !strings.Contains(outcome.errMsg, "gap fill") && !strings.Contains(outcome.errMsg, "graphql PR batch") {
		t.Errorf("buildOutcome must populate errMsg with the gap-fill error text so shouldForceFullRecollect can substring-match it. Got errMsg=%q", outcome.errMsg)
	}
}

// TestBuildOutcomeBehavior_NilGapFillErrorIsHarmless pins that passing nil
// for the gap-fill error doesn't change the prior behavior. Successful
// collections must still produce success=true.
func TestBuildOutcomeBehavior_NilGapFillErrorIsHarmless(t *testing.T) {
	var s Scheduler
	successResult := makeSuccessfulCollectResult()

	outcome := callBuildOutcome(&s, successResult, nil, nil, nil, nil)

	if !outcome.success {
		t.Errorf("buildOutcome with nil gap-fill error and a clean result must leave success=true, got success=%v errMsg=%q", outcome.success, outcome.errMsg)
	}
}
