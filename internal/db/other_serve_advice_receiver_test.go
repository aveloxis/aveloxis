// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"errors"
	"strings"
	"testing"
)

// Round-11 finding 1 (L9), the same receiver-awareness class round 10
// finding 3 fixed for the OTHER branch.
//
// Advice branched only on len(o.From) == 0, so ANY non-empty listing led
// with "An (other address) entry is normally the primary, so this host
// is running the wrong command". But otherServeAddressesFrom tags each
// address with the code's own verdict, and a sighting whose entries are
// ALL "(this host)" is reachable and documented — round-8 finding 4:
// a "(this host)" entry is equally a serve STILL RUNNING here, and a
// foreground `aveloxis serve` beside a running one is reachable. So on
// a primary restarting beside its own draining backends the verdict was
// exactly backwards, and the fast-path WARN's tail then added
// "`aveloxis stop serve` on this host is the way to back out" — telling
// the operator to stop the primary.
//
// The same holds for an all-HIDDEN sighting (round-7 finding 1): a
// backend whose address this role cannot see carries NO verdict, so
// reading it as the primary is precisely the inference the hidden
// bucket exists to refuse.
//
// The verdict is warranted by an (other address) ENTRY, not by
// non-emptiness.

const wrongCommandConsequent = "running the wrong command"

func TestOtherServeAdviceWithdrawsTheVerdictWithoutAnOtherAddressEntry(t *testing.T) {
	cases := []struct {
		name string
		from []string
	}{
		{"all this-host", []string{"::1 (this host)", "127.0.0.1 (this host)"}},
		{"local socket, this host", []string{"local socket (this host)"}},
		{"all hidden", []string{"a backend of role aveloxis (client address not visible to role ops: pg_stat_activity shows the address of a session only to roles that HOLD the privileges of that session owner role)"}},
		{"this host and hidden", []string{"::1 (this host)", "a backend of role aveloxis (client address not visible to role ops: …)"}},
	}
	for _, c := range cases {
		got := OtherServe{Connected: true, From: c.from}.Advice()
		if strings.Contains(got, wrongCommandConsequent) {
			t.Errorf("%s: Advice must NOT lead with the wrong-command verdict when no (other address)\n"+
				"entry was printed — nothing in this sighting identifies the primary, and on a primary\n"+
				"restarting beside its own draining backends the verdict is backwards (round-11 finding 1).\ngot: %q", c.name, got)
		}
		if strings.Contains(got, "(other address)") {
			t.Errorf("%s: Advice must not explain an (other address) tag Describe() never printed —\n"+
				"the same receiver-awareness rule round-10 finding 3 applied to the tagless branch.\ngot: %q", c.name, got)
		}
	}
}

// The verdict IS warranted the moment one (other address) entry is on
// screen, even alongside this-host or hidden entries: that entry is the
// datum that distinguishes the primary.
func TestOtherServeAdviceKeepsTheVerdictWhenAnOtherAddressEntryIsPresent(t *testing.T) {
	cases := []struct {
		name string
		from []string
	}{
		{"only other address", []string{"10.0.0.5 (other address)"}},
		{"mixed", []string{"::1 (this host)", "10.0.0.5 (other address)"}},
		{"other address and hidden", []string{"a backend of role aveloxis (client address not visible to role ops: …)", "10.0.0.5 (other address)"}},
	}
	for _, c := range cases {
		got := OtherServe{Connected: true, From: c.from}.Advice()
		if !strings.Contains(got, wrongCommandConsequent) {
			t.Errorf("%s: an (other address) entry is exactly what warrants the verdict — withdrawing it\n"+
				"here would lose the whole point of the tag.\ngot: %q", c.name, got)
		}
		if !strings.Contains(got, "aveloxis start scancode-worker") {
			t.Errorf("%s: the verdict must still name the one command form; got %q", c.name, got)
		}
	}
}

// The tagless branch (listing failed or found nothing) is unchanged:
// round-10 finding 3 already made it state both readings without the
// tag framing, and it legitimately still names the command form because
// Describe() has printed no verdict either way.
func TestOtherServeAdviceTaglessBranchUnchanged(t *testing.T) {
	got := OtherServe{Connected: true, ListErr: errors.New("boom")}.Advice()
	if !strings.Contains(got, wrongCommandConsequent) || !strings.Contains(got, "aveloxis start scancode-worker") {
		t.Errorf("the tagless branch keeps its round-10 wording; got %q", got)
	}
}
