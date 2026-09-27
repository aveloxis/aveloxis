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
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Scheduler) senderResolvePass("))
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

// TestSenderResolveDoesNotStampOnStoreFailure pins worklist item 18: the
// sender resolver stamped its 30-day cooldown when CreateEmailOnlyContributor
// or LinkMailingListSender failed with a DB error — nothing was saved, and
// the sender was hidden for 30 days anyway (SR-5). A store failure ends the
// iteration without a stamp (the next tick retries).
func TestSenderResolveDoesNotStampOnStoreFailure(t *testing.T) {
	src := srctest.Read(t, "internal/scheduler/mailinglist_wiring.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Scheduler) senderResolvePass("))
	for _, arm := range []string{"if cerr != nil {", "if lerr != nil {"} {
		i := strings.Index(body, arm)
		if i < 0 {
			t.Fatalf("%q not found", arm)
		}
		rest := body[i:]
		end := strings.Index(rest, "continue")
		if end < 0 {
			t.Fatalf("%q does not end its iteration", arm)
		}
		if strings.Contains(rest[:end], "MarkSenderResolveAttempt(") {
			t.Errorf("%s stamps the sender-resolve cooldown on a STORE failure; nothing was saved, so nothing may be stamped", arm)
		}
	}
}

// TestSenderResolveNoIdentityIsNotLinked pins review round 7 of items 16–19:
// LinkMailingListSender's "no stable identity" skip (a login with no
// contributor row and no forge id) is a typed error, and the wiring records
// the 30-day cooldown for it — never the terminal resolved stamp, never a
// linked count. Through v0.29.67 the skip came back as a nil error and the
// sender was stamped resolved with the login and no alias.
func TestSenderResolveNoIdentityIsNotLinked(t *testing.T) {
	src := srctest.Read(t, "internal/scheduler/mailinglist_wiring.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Scheduler) senderResolvePass("))
	arm := "if errors.Is(lerr, db.ErrNoStableIdentity) {"
	i := strings.Index(body, arm)
	if i < 0 {
		t.Fatalf("%q not found: the skip must be classified before the store-failure arm", arm)
	}
	if j := strings.Index(body, "if lerr != nil {"); j >= 0 && j < i {
		t.Error("the no-identity arm must come before the store-failure arm, or the skip is counted as a store failure")
	}
	rest := body[i:]
	end := strings.Index(rest, "continue")
	if end < 0 {
		t.Fatal("the no-identity arm does not end its iteration")
	}
	if !strings.Contains(rest[:end], "MarkSenderResolveAttempt(ctx, c.SenderEmail, false, \"\", \"\")") {
		t.Error("the no-identity arm must record the 30-day cooldown (resolved=false, no login): the login may gain a row later")
	}
	if strings.Contains(rest[:end], "linked++") || strings.Contains(rest[:end], ", true,") {
		t.Error("the no-identity arm must not count the sender linked or stamp it resolved: nothing was written")
	}
}
