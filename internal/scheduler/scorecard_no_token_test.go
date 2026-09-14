// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestScorecardPhaseNamesTheEmptyLoanSkip — v0.29.10. RunScorecard now
// refuses a remote-primary run with no lent token and no clone
// (collector.ErrScorecardNoToken; the behavior is driven in the collector's
// scorecard_empty_loan_test.go). The phase must classify that refusal in its
// OWN branch, before the generic failure WARN — RunScorecard already logged
// the cause, and a second "scorecard failed" line would file an expected
// storm-time skip as a scorecard malfunction — and the phase line must carry
// the reason so a log review can count these skips.
func TestScorecardPhaseNamesTheEmptyLoanSkip(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) runScorecardPhase("))

	const noToken = "case errors.Is(scErr, collector.ErrScorecardNoToken):"
	const generic = "case scErr != nil:"
	const warn = `s.logger.Warn("scorecard failed"`
	if strings.Count(body, noToken) != 1 || strings.Count(body, generic) != 1 || strings.Count(body, warn) != 1 {
		t.Fatalf("runScorecardPhase must classify with exactly one %q arm, one %q arm and one %s call", noToken, generic, warn)
	}
	nt, g, w := strings.Index(body, noToken), strings.Index(body, generic), strings.Index(body, warn)
	if !(nt < g && g < w) {
		t.Error("the ErrScorecardNoToken arm must come BEFORE the generic failure arm, and the generic WARN must sit in that later arm")
	}
	// The reason itself is computed by scorecardSkipReason (behavior below);
	// the phase line must call it on this phase's error.
	if strings.Count(body, `"skip_reason", scorecardSkipReason(scErr),`) != 1 {
		t.Error(`the "scorecard phase complete" line must carry "skip_reason", scorecardSkipReason(scErr)`)
	}
}

func TestScorecardSkipReason(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"ran", nil, ""},
		{"no usable token and no clone", collector.ErrScorecardNoToken, "no_usable_token"},
		{"wrapped", fmt.Errorf("phase: %w", collector.ErrScorecardNoToken), "no_usable_token"},
		{"a real failure is not a skip", errors.New("scorecard exited 1"), ""},
		{"shutdown is not a skip", context.Canceled, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := scorecardSkipReason(tc.err); got != tc.want {
				t.Errorf("scorecardSkipReason(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
