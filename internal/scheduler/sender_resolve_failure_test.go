// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSenderResolveStampsOnlyDefinitiveFailures (review round 2 on v0.29.55,
// the unswept sibling of the search-resolve fix): runMailingListSenderResolve
// stamped MarkSenderResolveAttempt — a 30-day cooldown — on ANY
// ResolveEmailToIdentity error, so a rate-limited or transient /search/ call,
// or an empty key pool, hid the sender and left their messages unattributed
// with nothing learned (SR-5). The error arm must consult
// platform.IsDefinitiveAnswer and end the iteration before the stamp.
func TestSenderResolveStampsOnlyDefinitiveFailures(t *testing.T) {
	src := srctest.Read(t, "internal/scheduler/mailinglist_wiring.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Scheduler) runMailingListSenderResolve("))
	once := func(hay, needle string) int {
		t.Helper()
		if n := strings.Count(hay, needle); n != 1 {
			t.Fatalf("%q appears %d times, want exactly 1", needle, n)
		}
		return strings.Index(hay, needle)
	}
	arm := body[once(body, "if rerr != nil {"):]
	arm = arm[:once(arm, "if login == \"\" {")]
	gate := once(arm, "if !platform.IsDefinitiveAnswer(rerr) {")
	mark := once(arm, "s.store.MarkSenderResolveAttempt(")
	cont := strings.Index(arm[gate:], "continue")
	if cont < 0 || gate+cont > mark {
		t.Fatal("the non-definitive resolve failure does not end the iteration before MarkSenderResolveAttempt")
	}
}
