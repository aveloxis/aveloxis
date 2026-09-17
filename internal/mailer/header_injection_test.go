// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

// v0.27.3 — SMTP header-injection guard (CodeQL go/email-injection).
// Send builds the RFC 5322 header block with fmt.Sprintf; a CR/LF in
// any header value would inject arbitrary headers. The concrete attack:
// a user names their group `x\r\nBcc: victim@example.com` and the
// group-approval email's Subject embeds the group name.

import (
	"strings"
	"testing"
)

func TestSanitizeHeaderStripsCRLF(t *testing.T) {
	got := sanitizeHeader("group'\r\nBcc: attacker@evil.example\r\n")
	if strings.ContainsAny(got, "\r\n") {
		t.Errorf("sanitizeHeader must strip CR/LF, got %q", got)
	}
	if !strings.Contains(got, "group'") {
		t.Errorf("printable content should survive, got %q", got)
	}
}

func TestSanitizeHeaderPassesCleanValues(t *testing.T) {
	for _, s := range []string{
		"Welcome to Aveloxis",
		"Your Aveloxis group 'CHAOSS Metrics' has been approved",
		"user@example.com",
	} {
		if got := sanitizeHeader(s); got != s {
			t.Errorf("clean header %q must pass through, got %q", s, got)
		}
	}
}

// Header injection through Send itself — the Subject and the From display
// name — is covered behaviorally by TestSendKeepsInjectedHeadersOut, which
// reads the composed message back through net/mail.
