// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

// Behavioral tests of what Send actually hands to SMTP: the envelope
// recipient and the composed message, captured through Mailer.sendMail.
// They replace source-text pins and tests that exercised only net/mail;
// a mutation review of PR #207 showed those passed with the To: header
// re-scrubbed, the confirmation link re-capped, and so on.

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/mail"
	"net/smtp"
	"strings"
	"testing"
)

type sentMail struct {
	addr string
	from string
	to   []string
	msg  []byte
}

// captureMailer returns a configured Mailer whose deliveries are recorded
// instead of dialing Gmail, plus the log it writes.
func captureMailer(t *testing.T) (*Mailer, *[]sentMail, *bytes.Buffer) {
	t.Helper()
	sent := &[]sentMail{}
	logs := &bytes.Buffer{}
	m := &Mailer{
		cfg: Config{
			GmailUser:        "ops@example.com",
			GmailAppPassword: "abcdefghijklmnop",
			FromName:         "Aveloxis",
		},
		logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		sendMail: func(addr string, _ smtp.Auth, from string, to []string, msg []byte) error {
			*sent = append(*sent, sentMail{
				addr: addr,
				from: from,
				to:   append([]string(nil), to...),
				msg:  append([]byte(nil), msg...),
			})
			return nil
		},
	}
	return m, sent, logs
}

// readSent parses a captured message the way a receiving MTA would.
func readSent(t *testing.T, s sentMail) (*mail.Message, string) {
	t.Helper()
	msg, err := mail.ReadMessage(bytes.NewReader(s.msg))
	if err != nil {
		t.Fatalf("captured message does not parse: %v\n%s", err, s.msg)
	}
	body, err := io.ReadAll(msg.Body)
	if err != nil {
		t.Fatalf("read captured body: %v", err)
	}
	return msg, string(body)
}

// TestSendHeaderAndEnvelopeCarryTheSameAddress: the To: header and the SMTP
// envelope must name the same mailbox, byte for byte. net/smtp writes the
// envelope as `RCPT TO:<addr>` with no quoting, so the header must be
// exactly "<" + addr + ">" — anything the header scrubs or re-quotes that the
// envelope keeps is a disagreement (the zero-width-space case is what a
// header-only scrub would break).
func TestSendHeaderAndEnvelopeCarryTheSameAddress(t *testing.T) {
	for _, tc := range []struct{ name, to, want string }{
		{"plain", "user@example.com", "user@example.com"},
		{"display name is dropped", "Real Name <user@example.com>", "user@example.com"},
		{"needless quotes are dropped", `"plain"@example.com`, "plain@example.com"},
		{"trailing newline is trimmed", "user@example.com\n", "user@example.com"},
		{"comment is dropped", "user@example.com (work)", "user@example.com"},
		{"domain literal", "user@[127.0.0.1]", "user@[127.0.0.1]"},
		{"non-ASCII local part", "jos\u00e9@example.com", "jos\u00e9@example.com"},
		{"format rune is not scrubbed from one side only", "a\u200bb@example.com", "a\u200bb@example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, sent, _ := captureMailer(t)
			if err := m.Send(tc.to, "subject", "body"); err != nil {
				t.Fatalf("Send(%q) = %v", tc.to, err)
			}
			if len(*sent) != 1 {
				t.Fatalf("Send(%q) delivered %d messages, want 1", tc.to, len(*sent))
			}
			s := (*sent)[0]
			if s.addr != gmailSMTPHost || s.from != "ops@example.com" {
				t.Errorf("delivered via %q from %q, want %q from the configured gmail_user", s.addr, s.from, gmailSMTPHost)
			}
			if len(s.to) != 1 || s.to[0] != tc.want {
				t.Errorf("envelope recipients = %q, want [%q]", s.to, tc.want)
			}
			msg, _ := readSent(t, s)
			if got := msg.Header["To"]; len(got) != 1 || got[0] != "<"+tc.want+">" {
				t.Errorf("To: header = %q, want exactly %q — the envelope carries %q", got, "<"+tc.want+">", tc.want)
			}
		})
	}
}

// TestSendSkipsRecipientsItCannotDeliver: a recipient that does not parse,
// or whose addr-spec needs quoting, is skipped with a WARN and no error —
// the empty-recipient contract; a bad address must not break account
// creation or group approval.
//
// Quoting is refused because the envelope cannot carry it: net/smtp writes
// `RCPT TO:<%s>` raw, so `"john  smith"@example.com` goes on the wire as an
// invalid path, and a quoted `>` closes the path early and appends SMTP
// parameters of the sender's choosing.
func TestSendSkipsRecipientsItCannotDeliver(t *testing.T) {
	for _, tc := range []struct{ name, to string }{
		{"quoted local part with significant spaces", `"john  smith"@example.com`},
		{"quoted angle bracket closes the envelope path", `"x> NOTIFY=SUCCESS ORCPT=rfc822;a"@attacker.example`},
		{"quoted at sign", `"a@b"@example.com`},
		{"quoted escaped quote", `"a\"b"@example.com`},
		{"header injection attempt", "user@example.com\r\nBcc: victim@example.com"},
		{"no at sign", "not-an-address"},
		{"two addresses", "a@example.com, b@example.com"},
		{"empty", ""},
		{"spaces", "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, sent, logs := captureMailer(t)
			if err := m.Send(tc.to, "subject", "body"); err != nil {
				t.Errorf("Send(%q) = %v, want nil (skip, not error)", tc.to, err)
			}
			if len(*sent) != 0 {
				t.Errorf("Send(%q) delivered to envelope %q — it must be skipped", tc.to, (*sent)[0].to)
			}
			if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "mailer.Send skipped") {
				t.Errorf("Send(%q) skipped without a WARN; log:\n%s", tc.to, logs)
			}
		})
	}
}

// TestSendKeepsInjectedHeadersOut: every header value built from caller or
// config text is scrubbed, so a CR/LF cannot add a header (CWE-93 — a group
// named "x\r\nBcc: ..." reaching the Subject via the approval email).
func TestSendKeepsInjectedHeadersOut(t *testing.T) {
	m, sent, _ := captureMailer(t)
	m.cfg.FromName = "Aveloxis\r\nBcc: fromname@evil.example"
	if err := m.Send("user@example.com", "hello\r\nBcc: subject@evil.example", "body"); err != nil {
		t.Fatalf("Send = %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(*sent))
	}
	msg, body := readSent(t, (*sent)[0])
	if bcc := msg.Header["Bcc"]; len(bcc) != 0 {
		t.Errorf("injected Bcc header reached the message: %q", bcc)
	}
	if got, want := msg.Header.Get("Subject"), "hello Bcc: subject@evil.example"; got != want {
		t.Errorf("Subject = %q, want %q", got, want)
	}
	if got, want := msg.Header.Get("From"), "Aveloxis Bcc: fromname@evil.example <ops@example.com>"; got != want {
		t.Errorf("From = %q, want %q", got, want)
	}
	if body != "body\r\n" {
		t.Errorf("body = %q, want %q", body, "body\r\n")
	}
}

// TestSendEmailConfirmationMailsTheWholeLink: the confirmation URL is not a
// label, so the body-value cap must not apply — a site_url long enough to
// push the link past bodyValueMax would otherwise mail a broken link ending
// in an ellipsis. Control and format runes are still scrubbed.
func TestSendEmailConfirmationMailsTheWholeLink(t *testing.T) {
	longURL := "https://aveloxis.example/" + strings.Repeat("deploy/", 50) +
		"account/email/confirm?token=" + strings.Repeat("a", 64)
	if n := len([]rune(longURL)); n <= bodyValueMax {
		t.Fatalf("fixture is %d runes; it must exceed bodyValueMax (%d) to exercise the cap", n, bodyValueMax)
	}
	m, sent, _ := captureMailer(t)
	if err := m.SendEmailConfirmation("user@example.com", "alice", longURL); err != nil {
		t.Fatalf("SendEmailConfirmation = %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(*sent))
	}
	_, body := readSent(t, (*sent)[0])
	if !strings.Contains(body, longURL) {
		t.Errorf("the mailed body does not contain the whole link %q:\n%s", longURL, body)
	}

	m, sent, _ = captureMailer(t)
	if err := m.SendEmailConfirmation("user@example.com", "alice", "http://localhost:8082/account/email/confirm?token=ab\u202ecd"); err != nil {
		t.Fatalf("SendEmailConfirmation = %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(*sent))
	}
	if _, body := readSent(t, (*sent)[0]); !strings.Contains(body, "token=abcd") {
		t.Errorf("the bidi override was not scrubbed from the link:\n%q", body)
	}
}

// TestParseRecipient pins the one recipient rule directly — the shared
// parser internal/web's account-email form also calls. A quoted local part
// is refused with the typed ErrRecipientNeedsQuoting; an unparseable
// address with net/mail's own error, never the quoting one.
func TestParseRecipient(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
		quoting        bool // want ErrRecipientNeedsQuoting
		refused        bool // want some other error
	}{
		{name: "plain", in: "user@example.com", want: "user@example.com"},
		{name: "surrounding space", in: "  user@example.com \n", want: "user@example.com"},
		{name: "display name", in: "Real Name <user@example.com>", want: "user@example.com"},
		{name: "needless quotes", in: `"plain"@example.com`, want: "plain@example.com"},
		{name: "plus tag", in: "a+tag@sub.example.co.uk", want: "a+tag@sub.example.co.uk"},
		{name: "IP domain literal", in: "user@[127.0.0.1]", want: "user@[127.0.0.1]"},
		{name: "spaces in quotes", in: `"john  smith"@example.com`, quoting: true},
		{name: "angle bracket in quotes", in: `"x> NOTIFY=SUCCESS"@example.com`, quoting: true},
		{name: "at sign in quotes", in: `"a@b"@example.com`, quoting: true},
		{name: "tab in quotes", in: "\"a\tb\"@example.com", quoting: true},
		{name: "inner CRLF", in: "user@example.com\r\nBcc: victim@example.com", refused: true},
		{name: "no at sign", in: "alice", refused: true},
		{name: "two addresses", in: "a@example.com, b@example.com", refused: true},
		{name: "space in the domain", in: "user@exa mple.com", refused: true},
		{name: "non-IP domain literal", in: "user@[1.2.3.4> X]", refused: true},
		{name: "empty", in: "", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRecipient(tc.in)
			switch {
			case tc.quoting:
				if !errors.Is(err, ErrRecipientNeedsQuoting) {
					t.Errorf("ParseRecipient(%q) = %q, %v; want ErrRecipientNeedsQuoting", tc.in, got, err)
				}
			case tc.refused:
				if err == nil || errors.Is(err, ErrRecipientNeedsQuoting) {
					t.Errorf("ParseRecipient(%q) = %q, %v; want a parse error", tc.in, got, err)
				}
			default:
				if err != nil || got != tc.want {
					t.Errorf("ParseRecipient(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
				}
			}
		})
	}
}

// TestNewLeavesTheSMTPSender: the capture seam is for tests only. New must
// leave sendMail nil, which Send resolves to smtp.SendMail — for a valid
// config and for the disabled fallback alike.
func TestNewLeavesTheSMTPSender(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"},
		{GmailUser: "not-an-address", GmailAppPassword: "short"},
	} {
		if m := New(cfg, nil); m.sendMail != nil {
			t.Errorf("New(%+v) set sendMail — production mail must go through smtp.SendMail", cfg)
		}
	}
}
