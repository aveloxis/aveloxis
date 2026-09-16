// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

// v0.27.3 — SMTP header-injection guard (CodeQL go/email-injection).
// Send builds the RFC 5322 header block with fmt.Sprintf; a CR/LF in
// any header value would inject arbitrary headers. The concrete attack:
// a user names their group `x\r\nBcc: victim@example.com` and the
// group-approval email's Subject embeds the group name.

import (
	"net/mail"
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
	// The recipient is PARSED into an addr-spec (a validation barrier),
	// then the parsed value is what the header block consumes. Before
	// v0.29.25 this pinned sanitizeHeader(to) — character scrubbing of the
	// raw form value; parsing is strictly stronger, so the pin follows.
	for _, needle := range []string{
		"mail.ParseAddress(strings.TrimSpace(to))",
		"recipient := parsed.Address",
		"(&mail.Address{Address: recipient}).String()",
		"sanitizeHeader(subject)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("Send must apply %s before building the SMTP header block — "+
				"raw CR/LF in a header value is SMTP header injection (CWE-93), and "+
				"the recipient arrives straight from a web form", needle)
		}
	}
	// The raw parameter must not reach the header block or the envelope,
	// and the parsed recipient must not pass through the body-value
	// normalizer — it collapses whitespace and truncates, which can
	// mutate a valid mailbox (spaces in a quoted local part are
	// significant) while the envelope keeps the unmodified value.
	for _, banned := range []string{"sanitizeHeader(to)", "[]string{to}", "[]string{sanitizeHeader(to)}", "sanitizeHeader(recipient)"} {
		if strings.Contains(body, banned) {
			t.Errorf("Send uses %q — the raw recipient parameter must not reach the message; use the parsed address", banned)
		}
	}
}

// TestToHeaderSerializationPreservesParsedMailbox: the To: header is built
// by re-serializing the parsed addr-spec with net/mail, NOT by passing it
// through scrubUntrusted — the normalizer collapses whitespace, which
// mutates a quoted local part where spaces are significant, while the
// envelope keeps the unmodified address (Copilot review on PR #207). The
// serialized form must re-parse to the identical addr-spec.
func TestToHeaderSerializationPreservesParsedMailbox(t *testing.T) {
	for _, in := range []string{
		"user@example.com",
		`"john  smith"@example.com`, // significant double space in a quoted local part
	} {
		parsed, err := mail.ParseAddress(in)
		if err != nil {
			t.Fatalf("ParseAddress(%q): %v", in, err)
		}
		serialized := (&mail.Address{Address: parsed.Address}).String()
		if strings.ContainsAny(serialized, "\r\n") {
			t.Errorf("serialized To header %q contains CR/LF", serialized)
		}
		back, err := mail.ParseAddress(serialized)
		if err != nil {
			t.Fatalf("re-parse %q: %v", serialized, err)
		}
		if back.Address != parsed.Address {
			t.Errorf("To header round-trip changed the addr-spec: %q -> %q", parsed.Address, back.Address)
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
