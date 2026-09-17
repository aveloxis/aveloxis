// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSearchResolveStampsOnlyDefinitiveFailures: review round 1 on v0.29.55
// (the class sweep of the enrichment fix). runSearchResolve stamped
// MarkContributorSearchAttempted — a cooldown — on ANY SearchUserByEmail
// error, so a rate-limited search (the probe: ten refusals on one key, then
// "exhausted 10 retries … transient") hid the email from search resolution
// with nothing learned (SR-5). The error arm must consult
// platform.IsDefinitiveAnswer and end the iteration before the stamp.
func TestSearchResolveStampsOnlyDefinitiveFailures(t *testing.T) {
	src := srctest.Read(t, "internal/scheduler/scheduler.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Scheduler) runSearchResolve("))
	once := func(hay, needle string) int {
		t.Helper()
		if n := strings.Count(hay, needle); n != 1 {
			t.Fatalf("%q appears %d times, want exactly 1", needle, n)
		}
		return strings.Index(hay, needle)
	}
	arm := body[once(body, "s.ghClient.SearchUserByEmail(ctx, c.Email)"):]
	arm = arm[:once(arm, "if login == \"\" || ghUserID == 0 {")]
	gate := once(arm, "if !platform.IsDefinitiveAnswer(err) {")
	mark := once(arm, "s.store.MarkContributorSearchAttempted(ctx, c.CntrbID)")
	cont := strings.Index(arm[gate:], "continue")
	if cont < 0 || gate+cont > mark {
		t.Fatal("the non-definitive search failure does not end the iteration before MarkContributorSearchAttempted")
	}
}
