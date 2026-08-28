// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pass 30 (v0.28.18): a FAILED job forgets its repo's cached listing
// ETags — the retry must re-read pages the failed job fetched but never
// stored; the hook runs on the failure path AFTER CompleteJob, and both
// forge clients expose ForgetRepoETags over the repo's path prefix.
func TestFailedJobForgetsTheRepoETags(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) runJob("))
	call := strings.Index(body, "forgetRepoETags()")
	complete := strings.Index(body, "s.store.CompleteJob(")
	outcome := strings.Index(body, "outcome := s.buildOutcome(")
	if call < 0 || complete < 0 || outcome < 0 || call > complete || call < outcome {
		t.Fatalf("runJob must call forgetRepoETags() after the outcome is known and BEFORE CompleteJob (the row flips to queued there; a Boost claim in the window would walk the cached pages): outcome=%d call=%d complete=%d", outcome, call, complete)
	}
	if !strings.Contains(srctest.NormalizeWS(body[outcome:complete]), "if !outcome.success && forgetRepoETags != nil { forgetRepoETags() }") {
		t.Error("the hook must be gated on !outcome.success")
	}
	if !strings.Contains(body, "ForgetRepoETags(owner, repo string) int") {
		t.Error("runJob must discover the hook via the optional ForgetRepoETags interface on the platform client")
	}
	for _, c := range []struct{ file, prefix string }{
		{"internal/platform/github/client.go", `fmt.Sprintf("/repos/%s/%s/", owner, repo)`},
		{"internal/platform/gitlab/client.go", `fmt.Sprintf("/projects/%s/", projectPath(owner, repo))`},
	} {
		fb := srctest.FuncBody(t, srctest.Read(t, c.file), "func (c *Client) ForgetRepoETags(")
		if !strings.Contains(fb, "ForgetETagsWithPrefix("+c.prefix+")") {
			t.Errorf("%s: ForgetRepoETags must forget the repo's own path prefix %s", c.file, c.prefix)
		}
	}
}
