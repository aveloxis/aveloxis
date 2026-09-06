// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// quotestrip_test.go — Part B: the quoted-history pattern library.
// Behavioral spec from the 2026-08-31 corpus measurement (20,000
// messages, 5 Apache lists): 82.5% of list mail carries quoted history
// (64% of body chars); the prototype rules took median body 4,774 → 302
// chars with 1/5,000 emptied. Shapes below are drawn from that corpus.
package mailinglist

import (
	"strings"
	"testing"
)

func TestStripQuotedHistoryTable(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain body unchanged",
			"We should bump the parent pom.\nAny objections?",
			"We should bump the parent pom.\nAny objections?"},
		{"empty body",
			"", ""},
		{"gt-quoted lines dropped (77.5% prevalence)",
			"Agreed.\n> On the other hand the release\n> branch is frozen.\nLet's ship.",
			"Agreed.\nLet's ship."},
		{"single-line attribution dropped (69.1%)",
			"+1\n\nOn Mon, Jan 5, 2026 at 3:14 PM Alice Smith <alice@example.org> wrote:\n> earlier text",
			"+1"},
		{"wrapped two-line attribution dropped",
			"Sounds right.\nOn Mon, Jan 5, 2026 at 3:14 PM Alice Smith\n<alice@example.org> wrote:\n> earlier",
			"Sounds right."},
		// Copilot round 24 (PR #193): authored prose beginning "On " with
		// NO date shape must be kept, even when a later line ends "wrote:".
		{"authored On-prose with a later wrote: is NOT stripped (round 24)",
			"On Windows the path separator differs.\nwe should document what Alice wrote:\nlet's add a note.",
			"On Windows the path separator differs.\nwe should document what Alice wrote:\nlet's add a note."},
		{"single-line On-prose ending wrote: without a date is kept (round 24)",
			"On second thought, here is what she wrote:",
			"On second thought, here is what she wrote:"},
		// Copilot round 26 (PR #193): a bare DIGIT is not a date — authored
		// numeric prose beginning "On " must be kept (round 24's [0-9] gate
		// stripped these).
		{"single-line On-prose with a bare number ending wrote: is kept (round 26)",
			"On issue 123, here is what Alice wrote:",
			"On issue 123, here is what Alice wrote:"},
		{"wrapped On-prose with a bare number is kept — no date shape (round 26)",
			"On issue 123, here is the change\nthat Alice reviewed and\nfinally wrote:\nplease merge.",
			"On issue 123, here is the change\nthat Alice reviewed and\nfinally wrote:\nplease merge."},
		{"On-prose mentioning a 4-digit number that is not a date is kept (round 26)",
			"On ticket 1234 we agreed on the API Bob wrote:",
			"On ticket 1234 we agreed on the API Bob wrote:"},
		{"slash-date attribution still strips (round 26 regression)",
			"+1\nOn 9/3/26, Carol <carol@x.org> wrote:\n> earlier",
			"+1"},
		{"signature delimiter drops to end (16.8%)",
			"Thanks for the review.\n-- \nAlice Smith\nApache Something PMC",
			"Thanks for the review."},
		{"original-message marker drops to end",
			"See below.\n-----Original Message-----\nFrom: Bob\nSubject: Re: thing\nold text",
			"See below."},
		{"underscore separator line dropped",
			"Reply here.\n________________________________\nFrom: Outlook quoting",
			"Reply here.\nFrom: Outlook quoting"},
		{"long dash separator line dropped (9.7%)",
			"Done.\n---------------------------------------------------------------------\nTo continue",
			"Done.\nTo continue"},
		{"unsubscribe footer drops to end",
			"Merged.\nTo unsubscribe, e-mail: dev-unsubscribe@arrow.apache.org\nFor additional commands, e-mail: dev-help@arrow.apache.org",
			"Merged."},
		{"atlassian jira trailer drops to end (22.3% of corpus carries jira urls)",
			"I can reproduce this on trunk.\n\n--\nThis message was sent by Atlassian Jira\n(v8.20.10#820010)",
			"I can reproduce this on trunk."},
		{"one hundred percent quote yields empty — legal, self-marks done",
			"> everything here\n> is quoted history\nOn Tue, Feb 2, 2026 at 9:00 AM Bob <bob@x.org> wrote:",
			""},
		{"base64url-ish content survives (no false positives)",
			"The node id is PR_kwDOBCyuKc8AAAABBMldbw and the hash\nis 0a1b2c3d4e5f66778899aabbccddeeff00112233.",
			"The node id is PR_kwDOBCyuKc8AAAABBMldbw and the hash\nis 0a1b2c3d4e5f66778899aabbccddeeff00112233."},
		{"blank runs collapse at the edges, internal newlines kept",
			"\n\n\nFirst point.\n\nSecond point.\n\n\n",
			"First point.\n\nSecond point."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, rule := StripQuotedHistory(tc.in)
			if got != tc.want {
				t.Errorf("clean:\n got: %q\nwant: %q", got, tc.want)
			}
			if rule != QuoteStripRuleVersion {
				t.Errorf("rule = %q, want %q", rule, QuoteStripRuleVersion)
			}
		})
	}
}

// TestStripQuotedHistoryNeverMutatesInputSemantics — the raw body is
// provenance; the function is pure and its rule version is a stable
// exported constant a rule change must bump.
func TestStripQuotedHistoryRuleVersionShape(t *testing.T) {
	if QuoteStripRuleVersion == "" || !strings.HasPrefix(QuoteStripRuleVersion, "qs-v") {
		t.Fatalf("QuoteStripRuleVersion = %q — must be a stable qs-vN identifier (it is stored per row and drives --rule-rerun)", QuoteStripRuleVersion)
	}
}
