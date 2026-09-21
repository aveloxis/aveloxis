// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGuideTickerExampleClassifiesCancellation (v0.29.57 review): the
// contributor guide's runMyThing is the ticker task new code is copied
// from, so it must pass the same shutdown rule as the real ones
// (TestEveryTickerTaskClassifiesCancellation). A draft of it logged three
// ctx-bound store errors without the check, so a copy would have logged
// every `stop serve` as a failure.
func TestGuideTickerExampleClassifiesCancellation(t *testing.T) {
	doc := srctest.Read(t, "docs/contributing/adding-a-collection-phase.md")
	const sig = "func (s *Scheduler) runMyThing("
	if n := strings.Count(doc, sig); n != 1 {
		t.Fatalf("the guide has %d runMyThing examples, want 1 — the check reads exactly one", n)
	}
	start := strings.Index(doc, sig)
	end := strings.Index(doc[start:], "\n}\n")
	if end < 0 {
		t.Fatal("the guide's runMyThing example does not end")
	}
	// Nested closes are indented; the column-0 "}" is the function's. Pinned
	// (Copilot review 5267408933 claimed a nested close truncated it).
	if seg := srctest.StripGoComments(doc[start : start+end+2]); strings.Count(seg, "{") != strings.Count(seg, "}") {
		t.Fatalf("the extracted runMyThing is not brace-balanced (%d open, %d close) — the example was truncated at a nested close", strings.Count(seg, "{"), strings.Count(seg, "}"))
	}
	body := srctest.StripGoComments(doc[start : start+end+2])
	violations, exempt := cancelViolations(body)
	for _, v := range violations {
		t.Errorf("the guide's runMyThing logs a ctx-bound error without the shutdown check: %s", v)
	}
	// Guard the denominator: the analyzer reads only `s.logger.…` calls with
	// an "error", err attribute and exempts a producer that is not
	// ctx-bound, so an example logging through another receiver or spelled
	// `"err", err` would pass unexamined. Every `.Warn(`/`.Error(` call here,
	// on any receiver, must be one it reads; every producer in the example is
	// ctx-bound. Not counted: INFO (the real tasks log progress and the
	// classified shutdown itself, `"cause", err`, at INFO) and the
	// `WarnContext` / `Log(ctx, level, …)` forms, which the analyzer does
	// not read either.
	warned := 0
	// A call with an argument: `err.Error()` is not a log.
	for _, m := range regexp.MustCompile(`\.(?:Warn|Error)\(\s*[^\s)]`).FindAllStringIndex(body, -1) {
		warned++
		open := m[0] + strings.Index(body[m[0]:m[1]], "(")
		read := strings.HasSuffix(body[:m[0]], "s.logger") && errorAttr(callArgs(body, open)) != ""
		if !read {
			t.Errorf("a .Warn(/.Error( call in the example is not one the analyzer reads (an s.logger call with an \"error\", err attribute): %q", body[max(0, m[0]-10):min(len(body), m[0]+70)])
		}
	}
	if warned == 0 {
		t.Fatal("the example has no .Warn(/.Error( call — the check examined nothing")
	}
	if len(exempt) != 0 {
		t.Errorf("the analyzer exempted %v — every producer in the example is ctx-bound", exempt)
	}
}
