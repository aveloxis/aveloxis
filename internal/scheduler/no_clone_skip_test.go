// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// no_clone_skip_test.go — v0.29.56 wiring pin: when the facade could not
// fetch the repository at all, analysis and scorecard are skipped. Both
// read the clone that does not exist; scorecard's remote mode spent a run
// per repo to be told the repository is unreachable (8 repos in two hours
// of the 2026-09-17 log).

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestNoCloneSkipsAnalysisAndScorecard(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"),
		"func (s *Scheduler) runFacadeAndAnalysis(")
	guard := strings.Index(body, "collector.HasBareClone(")
	analyze := strings.Index(body, "ac.AnalyzeRepo(")
	scorecard := strings.Index(body, "s.runScorecardPhase(")
	if guard < 0 || analyze < 0 || scorecard < 0 {
		t.Fatal("cannot find the clone guard, the analysis call or the scorecard phase")
	}
	if !(guard < analyze && guard < scorecard) {
		t.Error("the missing-clone guard must come before analysis and the scorecard phase")
	}
	after := body[guard:analyze]
	if !strings.Contains(after, "return facadeResult, nil") {
		t.Error("the guard must return before analysis when there is no clone")
	}
}
