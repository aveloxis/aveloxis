// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.20.9 (Fix B): pre-fix `fetchPRsForGap` discarded the
// (potentially non-empty) partial batch returned alongside a
// non-skippable error from `FetchPRBatch`. With v0.20.8's
// subdivision behavior, that partial-result return is now load-
// bearing: a 10-PR batch that splits to 5+5 where the second 5
// fails returns the FIRST 5 as `(out, err)`. Pre-Fix-B,
// `fetchPRsForGap` then returned `(nil, err)` and `fillPRGaps`
// (line 276-285) bailed before staging anything — the work the
// subdivision recovered was thrown away.
//
// Fix B: have `fetchPRsForGap` return `(partial, err)` and
// `fillPRGaps` iterate the partial envelopes and stage them
// before returning the error. The error still bubbles up so Fix
// A's force_full_collect logic fires and last_error is recorded
// — we just stop discarding the successful sub-batches.

// TestFetchPRsForGap_ReturnsPartialEnvelopesOnError is a
// source-contract pin: the GraphQL branch of `fetchPRsForGap`
// must not return `nil` for the envelopes slice when
// `FetchPRBatch` returned a non-nil error. The presence of
// `len(batch) > 0` or a similar partial-preservation check is
// the load-bearing signal.
func TestFetchPRsForGap_ReturnsPartialEnvelopesOnError(t *testing.T) {
	data, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate the GraphQL branch of fetchPRsForGap by anchoring
	// on the distinctive WARN message.
	anchorIdx := strings.Index(src, `"gap fill aborting on non-skippable PR batch error"`)
	if anchorIdx < 0 {
		t.Fatal(`cannot find GraphQL branch anchor "gap fill aborting on non-skippable PR batch error"`)
	}
	// Scan backward to the function start and forward to the
	// branch's return.
	branchStart := strings.LastIndex(src[:anchorIdx], "if gf.prChildMode")
	if branchStart < 0 {
		t.Fatal("cannot find start of GraphQL branch in fetchPRsForGap")
	}
	branchEnd := anchorIdx + strings.Index(src[anchorIdx:], "\n\t}")
	if branchEnd <= anchorIdx {
		t.Fatal("cannot find end of GraphQL branch in fetchPRsForGap")
	}
	branchBody := src[branchStart:branchEnd]

	// Three load-bearing signals:
	//   1. We acknowledge the batch may have partial content
	//      (substring "len(batch)" or similar).
	//   2. We do NOT return `nil, err` on the error path.
	//   3. The successful path's mapping loop is reused (or the
	//      partial result is constructed via the same envelope
	//      shape).
	if !strings.Contains(branchBody, "len(batch)") {
		t.Error("fetchPRsForGap's GraphQL branch must inspect len(batch) on the error path so partial successful sub-batches are not discarded — v0.20.8 subdivision can return (partial, err) and pre-Fix-B threw the partial away")
	}
	// A simple regression pin against `return nil, err` in the
	// error path. Implementations may use a named variable, an
	// intermediate `out` slice, or build envelopes from `batch`
	// before returning — but `nil` is what we explicitly do not
	// want.
	errorReturnIdx := strings.Index(branchBody, `return nil, fmt.Errorf("gap fill PR batch:`)
	if errorReturnIdx >= 0 {
		t.Error("fetchPRsForGap's GraphQL error path returns `nil, ...` — must return the partial envelopes from batch so they can be staged before the error bubbles. Look for `return out, fmt.Errorf(...)` or similar pattern that preserves the partial result.")
	}
}

// TestFillPRGaps_StagesPartialBeforeReturningError pins the
// caller-side complement: fillPRGaps must iterate the envelopes
// slice and stage what's there before propagating the error.
func TestFillPRGaps_StagesPartialBeforeReturningError(t *testing.T) {
	data, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Anchor on the fillPRGaps function definition.
	idx := strings.Index(src, "func (gf *GapFiller) fillPRGaps(")
	if idx < 0 {
		t.Fatal("cannot find fillPRGaps")
	}
	// Find the matching closing brace by looking for the next
	// "\nfunc " (signals the next function).
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of fillPRGaps")
	}
	body := tail[:1+endRel]

	// Locate the fetchPRsForGap call site and check that the
	// error handling does NOT short-circuit to return without
	// processing envelopes. Anchor on the existing
	// nonFatalErr := pattern and look for the staging loop
	// AFTER the error variable is captured.
	feIdx := strings.Index(body, "fetchPRsForGap(")
	if feIdx < 0 {
		t.Fatal("cannot find fetchPRsForGap call in fillPRGaps")
	}
	afterFe := body[feIdx:]

	// The new behavior: after fetchPRsForGap returns, the
	// envelope-staging loop must always run — not be gated
	// behind `nonFatalErr == nil`. Pin via the presence of the
	// staging loop and the absence of an early-return-on-error
	// before it.
	stagingLoopIdx := strings.Index(afterFe, "for _, envelope := range envelopes")
	if stagingLoopIdx < 0 {
		t.Fatal("cannot find envelope-staging loop in fillPRGaps")
	}
	gap := afterFe[:stagingLoopIdx]
	// If the gap between fetchPRsForGap and the staging loop
	// contains an unconditional `return filled, nonFatalErr`,
	// the partial envelopes are still being discarded.
	if strings.Contains(gap, "return filled, nonFatalErr") &&
		!strings.Contains(gap, "// will return below") {
		t.Error("fillPRGaps must NOT return between fetchPRsForGap and the envelope-staging loop — Fix B requires staging the partial successful envelopes first, then returning the error after the staging loop completes (so force_full_collect still fires for the failure, but the partial work isn't wasted).")
	}
}
