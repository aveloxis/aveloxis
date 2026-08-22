// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 1c (v0.27.37): AssessAndFillGaps must RETURN fill
// errors. Pre-fix, fillIssueGaps/fillPRGaps errors were Warn'd and
// dropped, so the entire v0.20.5 force_full_collect recovery pipeline
// — wired through runJob → buildOutcome → shouldForceFullRecollect —
// was unreachable for the exact failure class it was built for
// (graphql retry exhaustion inside a fill). Repos with historical gaps
// looped incompletely forever. The existing v0.20.5 tests pin the
// scheduler side only; this pins the missing half.

package collector

import (
	"os"
	"strings"
	"testing"
)

func TestAssessAndFillGapsReturnsFillErrors(t *testing.T) {
	src, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// v0.27.140: the body moved into the threshold variant (the
	// v0.27.139 healer's seam); the bare name is now a delegate.
	start := strings.Index(s, "func (gf *GapFiller) AssessAndFillGapsWithThreshold(")
	if start < 0 {
		t.Fatal("AssessAndFillGapsWithThreshold not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "errors.Join") {
		t.Error("AssessAndFillGaps must aggregate fill errors (errors.Join) and return them — partial fills are already staged (v0.20.9), so returning the error only re-arms the v0.20.5 recovery")
	}
	for _, swallow := range []string{
		`gf.logger.Warn("issue gap fill error", "error", err)
			}
			totalFilled += filled`,
		`gf.logger.Warn("PR gap fill error", "error", err)
			}
			totalFilled += filled`,
	} {
		if strings.Contains(body, swallow) {
			t.Error("fill errors must be collected, not warn-and-dropped (the pre-v0.27.37 swallow shape)")
		}
	}
}
