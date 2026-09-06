// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestSenderResolveTickerWiredAndUsesSharedChain pins Phase 2 (summary/12 §5):
// the runMailingListSenderResolve ticker exists, is spawned from
// spawnMailingListWorker, and uses the SHARED resolver + the link/stamp
// helpers (not a bespoke per-ticker chain). A future refactor that swaps in a
// private resolver, drops the bot-terminal stamp, or forgets to wire the
// goroutine fails here.
func TestSenderResolveTickerWiredAndUsesSharedChain(t *testing.T) {
	data, err := os.ReadFile("mailinglist_wiring.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// pass 38: launched through the tracked-pool helper (safego.Go inside)
	// so the shutdown arm waits for it before the pool closes.
	if !strings.Contains(src, `s.goTracked("mailing-list-sender-resolve", func() { s.runMailingListSenderResolve(ctx) })`) {
		t.Error("spawnMailingListWorker must spawn the runMailingListSenderResolve ticker goroutine")
	}

	body := extractFuncBody(t, src, "func (s *Scheduler) runMailingListSenderResolve(")
	for _, needle := range []string{
		"GetMailingListSenderResolveCandidates(", // candidate selection (>= threshold, cooldown)
		"collector.ResolveEmailToIdentity(",      // the SHARED chain, not a bespoke one
		"collector.IsAutomationEmail(",           // bots stamped terminal
		"LinkMailingListSender(",                 // link/create on a hit
		"MarkSenderResolveAttempt(",              // cooldown / outcome stamp
		"mailingListSenderResolveMinMessages",    // the >=6 threshold constant
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("runMailingListSenderResolve must reference %q", needle)
		}
	}
	// Bots must be stamped resolved=true (terminal), so they leave the pool.
	if !strings.Contains(body, "MarkSenderResolveAttempt(ctx, c.SenderEmail, true, \"bot\"") {
		t.Error("bot senders must be stamped resolved=true (terminal) so they don't re-enter the candidate pool every cooldown")
	}
}

// TestSenderResolvePhase4CreatesEmailOnlyForHumans pins Phase 4: on an API
// miss, a DIRECT-HUMAN sender (HumanClass, non-bot) gets an email-only
// contributor; bot-relayed senders do not.
func TestSenderResolvePhase4CreatesEmailOnlyForHumans(t *testing.T) {
	data, err := os.ReadFile("mailinglist_wiring.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(data), "func (s *Scheduler) runMailingListSenderResolve(")
	for _, needle := range []string{
		"c.HumanClass", // gate on direct-human
		"!collector.IsAutomationEmail(c.SenderEmail)",                     // never a bot
		"CreateEmailOnlyContributor(",                                     // the Phase 4 create
		`MarkSenderResolveAttempt(ctx, c.SenderEmail, true, "email-only"`, // terminal stamp
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("runMailingListSenderResolve miss-path must reference %q (Phase 4)", needle)
		}
	}
}
