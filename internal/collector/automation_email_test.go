// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// automation_email_test.go — C1-pre: the relay/automation sender
// predicate. Found 2026-08-31 on the production aveloxis DB: the
// sender-resolve ticker had minted email-only CONTRIBUTOR rows for
// jira@apache.org, git@apache.org, gitbox@apache.org, and even a list
// address (dev@myfaces.apache.org) — IsBotEmail only knows
// [bot]/noreply/@github.com, and bool_or(msg_class...) classified the
// relays human because they also send discuss-class mail. 83,746
// messages were falsely attributed to the jira@ phantom; a fast
// attribution pass would have handed it 5.48M more.
package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestIsAutomationEmail(t *testing.T) {
	cases := []struct {
		email string
		want  bool
	}{
		// The production phantoms.
		{"jira@apache.org", true},
		{"jira+amqnet@apache.org", true},
		{"git@apache.org", true},
		{"gitbox@apache.org", true},
		// Everything IsBotEmail already catches stays caught.
		{"dependabot[bot]@users.noreply.github.com", true},
		{"noreply@example.org", true},
		{"notifications@github.com", true},
		// Humans stay human — including users.noreply commit addresses.
		{"12345+alice@users.noreply.github.com", false},
		{"alice@example.org", false},
		{"jirasmith@example.org", false}, // name containing 'jira' is not a relay
		{"gitte@example.org", false},     // name starting 'git' is not a relay
	}
	for _, tc := range cases {
		if got := IsAutomationEmail(tc.email); got != tc.want {
			t.Errorf("IsAutomationEmail(%q) = %v, want %v", tc.email, got, tc.want)
		}
	}
}

// TestIsAutomationEmailSupersetOfIsBotEmail — the new predicate must
// never be NARROWER than the old one (a bot passing the automation
// gate would re-open the minting hole).
func TestIsAutomationEmailSupersetOfIsBotEmail(t *testing.T) {
	for _, email := range []string{
		"x[bot]@example.org", "noreply@x.org", "web-flow@github.com",
	} {
		if IsBotEmail(email) && !IsAutomationEmail(email) {
			t.Errorf("%q: IsBotEmail=true but IsAutomationEmail=false — the superset contract is broken", email)
		}
	}
}

// TestResolverGatesUseAutomationPredicate (review 2026-08-30 #12): the
// commit resolver and the shared email-identity chain gate on the FULL
// automation predicate — IsBotEmail alone lets a relay address
// (git@/jira@apache.org as a commit author) acquire the exact
// alias/identity rows the automation-phantom heal deletes.
func TestResolverGatesUseAutomationPredicate(t *testing.T) {
	for _, f := range []string{
		"internal/collector/commit_resolver.go",
		"internal/collector/email_identity_resolver.go",
	} {
		src := srctest.StripGoComments(srctest.Read(t, f))
		if strings.Contains(src, "IsBotEmail(") {
			t.Errorf("%s still gates on IsBotEmail( — use IsAutomationEmail (the pinned superset)", f)
		}
	}
}
