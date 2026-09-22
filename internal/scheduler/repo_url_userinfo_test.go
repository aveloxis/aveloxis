// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.57 fix-review round 2: the facade and scorecard refusals sat
// downstream of prelim's HEAD request (which sends userinfo as basic auth)
// and of seven log sites that wrote the stored URL verbatim. The refusal
// belongs where the row is loaded: runJob refuses before RunPrelim and fails
// the job; the two periodic loops that probe stored URLs (the rename check
// and the gone recheck) refuse before their probe. Ordering pins: both
// indexes must exist and the refusal must come first.
func TestStoredRepoURLIsRefusedBeforeAnyProbe(t *testing.T) {
	src := srctest.Read(t, "internal/scheduler/scheduler.go")
	gone := srctest.Read(t, "internal/scheduler/gone_recheck.go")
	for _, tc := range []struct {
		name, body, probe, arm string
	}{
		{"runJob", srctest.FuncBody(t, src, "func (s *Scheduler) runJob("), "collector.RunPrelim(ctx, s.store, repo, s.logger)", "s.failJob(ctx, job.RepoID, uerr.Error())"},
		{"checkForRenames", srctest.FuncBody(t, src, "func (s *Scheduler) checkForRenames("), "collector.RunPrelim(ctx, s.store, &repo, s.logger)", "continue"},
		{"runGoneRecheck", srctest.FuncBody(t, gone, "func (s *Scheduler) runGoneRecheck("), "goneProbe(ctx, c.GitURL)", "stampChecked(c)"},
	} {
		body := srctest.StripGoComments(tc.body)
		refuse := strings.Index(body, "platform.RefuseURLUserinfo(")
		probe := strings.Index(body, tc.probe)
		if refuse < 0 || probe < 0 || refuse > probe {
			t.Errorf("%s: the userinfo refusal (at %d) must come before the probe %q (at %d)", tc.name, refuse, tc.probe, probe)
			continue
		}
		arm := body[refuse:probe]
		if !strings.Contains(arm, "platform.RedactURLUserinfo(") || !strings.Contains(arm, "s.logger.Error(") {
			t.Errorf("%s: the refusal arm must log at ERROR with the redacted URL", tc.name)
		}
		if !strings.Contains(arm, tc.arm) {
			t.Errorf("%s: the refusal arm must end the work with %q", tc.name, tc.arm)
		}
	}
	// No log site in these two files writes a stored URL verbatim.
	for name, s := range map[string]string{"scheduler.go": src, "gone_recheck.go": gone} {
		stripped := srctest.StripGoComments(s)
		for _, raw := range []string{`"url", repo.GitURL`, `"url", c.GitURL`} {
			if strings.Contains(stripped, raw) {
				t.Errorf("%s logs %s verbatim — route it through platform.RedactURLUserinfo", name, raw)
			}
		}
	}
	prelim := srctest.StripGoComments(srctest.Read(t, "internal/collector/prelim.go"))
	if strings.Contains(prelim, `"url", repo.GitURL`) {
		t.Error("prelim.go logs repo.GitURL verbatim — route it through platform.RedactURLUserinfo")
	}
}
