package collector

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// TestGapFillFetchErrorsAreClassified verifies that the per-item fetch in
// fillIssueGaps and fillPRGaps does not silently `continue` on every error.
// The original v0.16.11 fix flushed the staging batch but the loops still
// swallowed errors at Debug level — meaning a 500-PR gap fill that hit a
// rate limit on item #3 would log "PR not found or error" 497 more times
// and complete with `filled=2` as if everything else really were missing.
//
// Required: each fetch error must run through isOptionalEndpointSkip first.
// Skippable conditions (404, 410, 403-private) keep `continue`; anything
// else must abort the loop so the caller can retry on the next cycle.
func TestGapFillFetchErrorsAreClassified(t *testing.T) {
	src, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, fn := range []string{"fillIssueGaps", "fillPRGaps"} {
		idx := strings.Index(code, "func (gf *GapFiller) "+fn+"(")
		if idx < 0 {
			t.Fatalf("cannot find %s in gap_fill.go", fn)
		}
		body := code[idx:]
		// Limit scope to the function body — assume next "\nfunc " starts
		// the next function.
		if next := strings.Index(body[1:], "\nfunc "); next > 0 {
			body = body[:next+1]
		}

		// Locate the fetch call (FetchIssueByNumber / FetchPRByNumber) and
		// the err handler that follows it.
		fetchSig := "FetchIssueByNumber"
		if fn == "fillPRGaps" {
			fetchSig = "FetchPRByNumber"
		}
		fetchIdx := strings.Index(body, fetchSig)
		if fetchIdx < 0 {
			t.Errorf("%s should call %s", fn, fetchSig)
			continue
		}
		// Inspect the next ~400 chars after the fetch — should contain the
		// classifier and a non-trivial error path that bubbles unexpected
		// errors instead of silently continuing.
		window := body[fetchIdx:]
		if len(window) > 600 {
			window = window[:600]
		}

		if !strings.Contains(window, "isOptionalEndpointSkip") {
			t.Errorf("%s must classify per-item fetch errors with " +
				"isOptionalEndpointSkip — silently `continue`-ing on every " +
				"error hides rate limits and turns a partial outage into a " +
				"permanent silent gap. Found: %s", fn, window)
		}

		// Soft check: the error path should escalate non-skippable errors
		// beyond Debug level (Warn/Error). Debug-only logging is what hid
		// this in production for weeks.
		if strings.Count(window, `gf.logger.Debug("PR not found`) > 0 ||
			strings.Count(window, `gf.logger.Debug("issue not found`) > 0 {
			// The original Debug log can stay for the genuine 404 path
			// inside the isOptionalEndpointSkip branch, but only there.
			// If the catch-all is still Debug, flag it.
			if !strings.Contains(window, "Warn") && !strings.Contains(window, "Error") {
				t.Errorf("%s: non-skippable fetch errors must log at WARN/ERROR " +
					"so on-call sees rate-limit pressure; current log site is " +
					"Debug-only. Found: %s", fn, window)
			}
		}
	}
}

// TestGapFillNonOptionalErrorsBubbleUp verifies the data flow: a fetch error
// that is NOT isOptionalEndpointSkip-eligible (e.g. ErrForbidden masquerading
// as a real auth failure, or a wrapped network error) must abort the loop
// and propagate via the function return so the scheduler can mark the job
// failed. Otherwise gap fill silently "succeeds" on a poisoned token.
//
// We assert this by source-grepping for the bubble-up pattern. A pure unit
// test would need to mock the platform.Client interface (12+ methods),
// which is out of proportion for a one-line guard.
func TestGapFillNonOptionalErrorsBubbleUp(t *testing.T) {
	src, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, fn := range []string{"fillIssueGaps", "fillPRGaps"} {
		idx := strings.Index(code, "func (gf *GapFiller) "+fn+"(")
		if idx < 0 {
			t.Fatalf("cannot find %s in gap_fill.go", fn)
		}
		body := code[idx:]
		if next := strings.Index(body[1:], "\nfunc "); next > 0 {
			body = body[:next+1]
		}

		// The handler for non-optional errors should `return` (with the
		// error), not just `break` or `continue`. Returning surfaces the
		// failure to the staged-collection caller; break/continue just
		// hides it under another layer of "completed successfully".
		fetchSig := "FetchIssueByNumber"
		if fn == "fillPRGaps" {
			fetchSig = "FetchPRByNumber"
		}
		fetchIdx := strings.Index(body, fetchSig)
		// The non-optional error branch may include a flush-on-partial step,
		// so allow a generous window. We're looking for the bubble-up
		// `return filled, ... err`-shaped statement that tells the caller
		// the gap fill failed mid-stream.
		window := body[fetchIdx:]
		if len(window) > 2000 {
			window = window[:2000]
		}

		// Look for the bubble-up shape: `return filled, ...err`. Must be a
		// real return that surfaces the error, not the success-path return
		// at the bottom of the function.
		if !strings.Contains(window, "return filled, fmt.Errorf") &&
			!strings.Contains(window, "return filled, err") {
			t.Errorf("%s: non-optional fetch errors must `return filled, err` " +
				"(bubble up), not `continue`. Found: %s", fn, window)
		}
	}
}

// TestIsOptionalEndpointSkipCoversForbidden is a pin: the classifier must
// recognize platform.ErrForbidden, not just ErrNotFound. Gap fill on a
// repo whose token loses scope mid-cycle would otherwise treat every
// 403-private as a permanent gap.
func TestIsOptionalEndpointSkipCoversForbidden(t *testing.T) {
	wrapped := errors.Join(errors.New("contextual"), platform.ErrForbidden)
	if !isOptionalEndpointSkip(wrapped) {
		t.Error("isOptionalEndpointSkip must recognize wrapped platform.ErrForbidden — " +
			"a token-scope 403 on a single item must skip cleanly, not bail the loop")
	}
}
