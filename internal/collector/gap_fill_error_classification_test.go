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

	// Since v0.18.1 the PR fetch lives in fetchPRsForGap (a helper
	// invoked by fillPRGaps). Issues still fetch inline in fillIssueGaps.
	// For each top-level function we allow the classifier to live either
	// in the function itself or in a helper it calls — as long as the
	// classifier runs somewhere in the fetch path, the non-skip error
	// won't silently `continue`.
	for _, fn := range []string{"fillIssueGaps", "fillPRGaps"} {
		idx := strings.Index(code, "func (gf *GapFiller) "+fn+"(")
		if idx < 0 {
			t.Fatalf("cannot find %s in gap_fill.go", fn)
		}
		body := code[idx:]
		if next := strings.Index(body[1:], "\nfunc "); next > 0 {
			body = body[:next+1]
		}

		// For fillIssueGaps we expect the fetch AND classifier inline.
		// For fillPRGaps (post-refactor) the fetch lives in a helper; we
		// search the helper too.
		haystack := body
		if fn == "fillPRGaps" {
			if hIdx := strings.Index(code, "func (gf *GapFiller) fetchPRsForGap("); hIdx >= 0 {
				helperBody := code[hIdx:]
				if next := strings.Index(helperBody[1:], "\nfunc "); next > 0 {
					helperBody = helperBody[:next+1]
				}
				haystack = body + "\n" + helperBody
			}
		}

		fetchSig := "FetchIssueByNumber"
		if fn == "fillPRGaps" {
			fetchSig = "FetchPRByNumber"
		}
		if !strings.Contains(haystack, fetchSig) {
			t.Errorf("%s (or its helper) should call %s", fn, fetchSig)
			continue
		}
		if !strings.Contains(haystack, "isOptionalEndpointSkip") {
			t.Errorf("%s (or its helper) must classify per-item fetch errors with " +
				"isOptionalEndpointSkip — silently `continue`-ing on every " +
				"error hides rate limits and turns a partial outage into a " +
				"permanent silent gap.", fn)
		}
		// Non-skippable errors should surface at WARN, not Debug only.
		if !strings.Contains(haystack, "Warn") && !strings.Contains(haystack, "Error") {
			t.Errorf("%s (or its helper): non-skippable fetch errors must log at WARN/ERROR " +
				"so on-call sees rate-limit pressure", fn)
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

	// Since v0.18.1 the PR fetch logic was split: fillPRGaps delegates
	// to fetchPRsForGap (new helper) which contains the FetchPRByNumber
	// call and the bubble-up return. For issues, fillIssueGaps still
	// owns the fetch directly. We grep each function's likely-owning
	// region for the bubble-up pattern; either pattern (direct in fn,
	// or return-through-helper) is acceptable.
	for _, fn := range []string{"fillIssueGaps", "fillPRGaps"} {
		idx := strings.Index(code, "func (gf *GapFiller) "+fn+"(")
		if idx < 0 {
			t.Fatalf("cannot find %s in gap_fill.go", fn)
		}
		body := code[idx:]
		if next := strings.Index(body[1:], "\nfunc "); next > 0 {
			body = body[:next+1]
		}

		// Either (a) the function itself has a `return filled, fmt.Errorf`
		// bubble-up, or (b) it delegates to a helper that returns the
		// non-skippable error to it — which it then returns (as
		// `return filled, <err>`). Both shapes count.
		inline := strings.Contains(body, "return filled, fmt.Errorf") ||
			strings.Contains(body, "return filled, err") ||
			strings.Contains(body, "return filled, nonFatalErr")
		if !inline {
			t.Errorf("%s: non-optional fetch errors must bubble up via `return filled, err/nonFatalErr/fmt.Errorf(...)`. Function body:\n%s", fn, body)
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
