// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

// v0.27.3 — SMTP header-injection guard (CodeQL go/email-injection).
// Send builds the RFC 5322 header block with fmt.Sprintf; a CR/LF in
// any header value would inject arbitrary headers. The concrete attack:
// a user names their group `x\r\nBcc: victim@example.com` and the
// group-approval email's Subject embeds the group name.

import (
	"os"
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

func TestSendSanitizesHeaderValues(t *testing.T) {
	src := mustRead(t, "mailer.go")
	// The header block Sprintf must consume sanitized values, not the
	// raw parameters. Pin the sanitizer applications inside Send.
	body := src[strings.Index(src, "func (m *Mailer) Send("):]
	if next := strings.Index(body[1:], "\nfunc "); next > 0 {
		body = body[:next+1]
	}
	for _, needle := range []string{"sanitizeHeader(to)", "sanitizeHeader(subject)"} {
		if !strings.Contains(body, needle) {
			t.Errorf("Send must apply %s before building the SMTP header block — "+
				"raw CR/LF in a header value is SMTP header injection (CWE-93)", needle)
		}
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
