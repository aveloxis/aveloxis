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
	"reflect"
	"strings"
	"testing"
	"time"
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

// TestSendSkipsRecipientsItCannotDeliver: a recipient that is empty, does
// not parse, or whose addr-spec needs quoting is skipped with a WARN and a
// typed ErrRecipientSkipped — never nil. A nil made the vulnerability digest
// log "sent" and advance its window, and `aveloxis test-mail` report
// success, for mail that was never attempted (round-2 review). Callers that
// must not break on a bad address filter it with IsSkip.
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
		{"local part over 64 octets", strings.Repeat("a", 65) + "@example.com"},
		{"header injection attempt", "user@example.com\r\nBcc: victim@example.com"},
		{"no at sign", "not-an-address"},
		{"two addresses", "a@example.com, b@example.com"},
		{"empty", ""},
		{"spaces", "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, sent, logs := captureMailer(t)
			err := m.Send(tc.to, "subject", "body")
			if !errors.Is(err, ErrRecipientSkipped) || !IsSkip(err) {
				t.Errorf("Send(%q) = %v, want an ErrRecipientSkipped that IsSkip reports", tc.to, err)
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

// TestSendReportsWhatItDidNotDo: a disabled mailer returns ErrNotConfigured
// (a nil *Mailer too), and a delivery failure is an error IsSkip does NOT
// report — the digest must retry that, and the web flows must log it.
func TestSendReportsWhatItDidNotDo(t *testing.T) {
	var nilMailer *Mailer
	if err := nilMailer.Send("user@example.com", "s", "b"); !errors.Is(err, ErrNotConfigured) || !IsSkip(err) {
		t.Errorf("nil mailer Send = %v, want ErrNotConfigured", err)
	}
	m, sent, _ := captureMailer(t)
	m.cfg.GmailUser = ""
	if err := m.Send("user@example.com", "s", "b"); !errors.Is(err, ErrNotConfigured) || !IsSkip(err) {
		t.Errorf("unconfigured Send = %v, want ErrNotConfigured", err)
	}
	if len(*sent) != 0 {
		t.Errorf("an unconfigured mailer delivered %d messages", len(*sent))
	}

	m, _, logs := captureMailer(t)
	m.sendMail = func(string, smtp.Auth, string, []string, []byte) error { return errors.New("535 5.7.8 rejected") }
	longComment := strings.Repeat("c", 4000)
	err := m.Send("Real Name <user@example.com> ("+longComment+")", "s", "b")
	if err == nil || IsSkip(err) {
		t.Errorf("a delivery failure must be a non-skip error, got %v", err)
	}
	if !strings.Contains(logs.String(), "mailer.Send failed") {
		t.Errorf("a delivery failure must be logged; log:\n%s", logs)
	}
	if strings.Contains(logs.String(), longComment[:400]) {
		t.Error("the failure log carries the raw recipient input; it must log the parsed, length-bounded address")
	}
	if IsSkip(nil) || IsSkip(errors.New("other")) {
		t.Error("IsSkip must be false for nil and for unrelated errors")
	}
}

// TestSendVulnerabilityDigestReportsASkippedRecipient: the digest is the
// caller that needs to know. An operator_email written as a list, or with a
// quoted local part, must come back as a skip — not nil, which advanced the
// digest window and dropped those findings for good.
func TestSendVulnerabilityDigestReportsASkippedRecipient(t *testing.T) {
	items := []VulnDigestItem{{RepoOwner: "o", RepoName: "r", VulnID: "GHSA-1", Severity: "CRITICAL", PackagePurl: "pkg:npm/x@1", Summary: "s"}}
	for _, to := range []string{"sec@example.com, ops@example.com", `"sec team"@example.com`} {
		m, sent, _ := captureMailer(t)
		if err := m.SendVulnerabilityDigest(to, time.Now(), items); !IsSkip(err) {
			t.Errorf("SendVulnerabilityDigest(%q) = %v, want a skip error", to, err)
		}
		if len(*sent) != 0 {
			t.Errorf("SendVulnerabilityDigest(%q) delivered", to)
		}
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
		tooLong        bool // want ErrRecipientTooLong
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
		// RFC 5321 section 4.5.3.1: a local part is at most 64 octets and a
		// path (the addr-spec in angle brackets) at most 256 octets, so an
		// addr-spec at most 254. Equality is allowed on both.
		{name: "64-octet local part", in: strings.Repeat("a", 64) + "@example.com", want: strings.Repeat("a", 64) + "@example.com"},
		{name: "65-octet local part", in: strings.Repeat("a", 65) + "@example.com", tooLong: true},
		{name: "254-octet address", in: "a@" + domainOfLength(252), want: "a@" + domainOfLength(252)},
		{name: "255-octet address", in: "a@" + domainOfLength(253), tooLong: true},
		// Octets, not runes: "é" is two octets in UTF-8.
		{name: "64-octet multi-byte local part", in: strings.Repeat("\u00e9", 32) + "@example.com", want: strings.Repeat("\u00e9", 32) + "@example.com"},
		{name: "66-octet multi-byte local part", in: strings.Repeat("\u00e9", 33) + "@example.com", tooLong: true},
		{name: "65 octets but 64 runes", in: strings.Repeat("a", 63) + "\u00e9@example.com", tooLong: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseRecipient(tc.in)
			switch {
			case tc.quoting:
				if !errors.Is(err, ErrRecipientNeedsQuoting) {
					t.Errorf("ParseRecipient(%q) = %q, %v; want ErrRecipientNeedsQuoting", tc.in, got, err)
				}
			case tc.tooLong:
				if !errors.Is(err, ErrRecipientTooLong) {
					t.Errorf("ParseRecipient(%d octets) = %v; want ErrRecipientTooLong", len(tc.in), err)
				}
			case tc.refused:
				if err == nil || errors.Is(err, ErrRecipientNeedsQuoting) || errors.Is(err, ErrRecipientTooLong) {
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

// domainOfLength returns a syntactically valid domain of exactly n octets
// (labels of at most 63 octets, as DNS requires).
func domainOfLength(n int) string {
	var labels []string
	for n > 0 {
		size := n
		if size > 63 {
			size = 63
		}
		if n-size == 1 { // a lone trailing octet cannot be a label after a dot
			size--
		}
		labels = append(labels, strings.Repeat("d", size))
		n -= size
		if n > 0 {
			n-- // the dot
		}
	}
	return strings.Join(labels, ".")
}

// TestDelivererDefaults pins both defaults of the send seam: a production
// binary's mailer delivers through smtp.SendMail, and inside a test binary
// a mailer with no seam installed refuses instead of dialing (round-7
// review: a broken seam made handler tests dial smtp.gmail.com). The
// refusal is checked by pointer BEFORE it is called, so even a mutation
// that restores smtp.SendMail here cannot dial.
func TestDelivererDefaults(t *testing.T) {
	sendMail := reflect.ValueOf(smtp.SendMail).Pointer()
	for _, cfg := range []Config{
		{},
		{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"},
		{GmailUser: "not-an-address", GmailAppPassword: "short"},
	} {
		m := New(cfg, nil)
		if m.sendMail != nil {
			t.Errorf("New(%+v) set sendMail — the seam is for tests only", cfg)
		}
		if got := reflect.ValueOf(m.delivererFor(false)).Pointer(); got != sendMail {
			t.Errorf("New(%+v): the production deliverer is not smtp.SendMail", cfg)
		}
		inTest := m.delivererFor(true)
		if reflect.ValueOf(inTest).Pointer() == sendMail {
			t.Fatalf("New(%+v): inside a test binary a mailer without a seam must not get smtp.SendMail", cfg)
		}
		if err := inTest(gmailSMTPHost, nil, "ops@example.com", []string{"user@example.com"}, nil); err == nil || !strings.Contains(err.Error(), "test seam") {
			t.Errorf("New(%+v): the in-test deliverer = %v, want a refusal naming the test seam", cfg, err)
		}
	}
}

// TestSendWithoutASeamRefusesInTests: a configured mailer with no seam, used
// from a test, returns an error instead of reaching SMTP — and that error is
// a delivery failure, not a skip.
func TestSendWithoutASeamRefusesInTests(t *testing.T) {
	m := New(Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil)
	if reflect.ValueOf(m.deliverer()).Pointer() == reflect.ValueOf(smtp.SendMail).Pointer() {
		t.Fatal("this test would dial smtp.gmail.com — the in-test refusal is gone")
	}
	err := m.Send("user@example.com", "s", "b")
	if err == nil || IsSkip(err) || !strings.Contains(err.Error(), "test seam") {
		t.Errorf("Send without a seam in a test = %v, want a non-skip refusal naming the test seam", err)
	}
}

// TestEnabledAgreesWithSend: Enabled is what callers check before promising
// a user an email, so it must be false exactly when Send reports
// ErrNotConfigured — including a whitespace-only gmail_user, which
// validation calls "empty" and used to leave Send dialing SMTP anyway.
func TestEnabledAgreesWithSend(t *testing.T) {
	var nilMailer *Mailer
	cases := map[string]*Mailer{
		"nil":                   nilMailer,
		"empty config":          New(Config{}, nil),
		"config New refused":    New(Config{GmailUser: "not-an-address", GmailAppPassword: "short"}, nil),
		"whitespace gmail_user": New(Config{GmailUser: "   "}, nil),
		"valid config":          New(Config{GmailUser: " ops@example.com ", GmailAppPassword: "abcdefghijklmnop"}, nil),
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			if m != nil {
				m.sendMail = func(string, smtp.Auth, string, []string, []byte) error { return nil }
			}
			err := m.Send("user@example.com", "s", "b")
			if disabled := errors.Is(err, ErrNotConfigured); m.Enabled() == disabled {
				t.Errorf("Enabled() = %v but Send returned %v", m.Enabled(), err)
			}
			if got := m.Deliverable("user@example.com"); (got == nil) != m.Enabled() {
				t.Errorf("Deliverable(valid address) = %v with Enabled() = %v", got, m.Enabled())
			}
		})
	}
	if m := New(Config{GmailUser: " ops@example.com ", GmailAppPassword: "abcdefghijklmnop"}, nil); m.cfg.GmailUser != "ops@example.com" {
		t.Errorf("New must store the trimmed gmail_user, got %q", m.cfg.GmailUser)
	}
}

// TestDeliverable: the up-front form of Send's own refusals, for callers
// that must decide before there is anything to send (the digest ticker).
func TestDeliverable(t *testing.T) {
	m, _, _ := captureMailer(t)
	if err := m.Deliverable("ops@example.com"); err != nil {
		t.Errorf("Deliverable(valid) = %v", err)
	}
	for _, to := range []string{"", "sec@example.com, ops@example.com", `"sec team"@example.com`} {
		if err := m.Deliverable(to); !errors.Is(err, ErrRecipientSkipped) {
			t.Errorf("Deliverable(%q) = %v, want ErrRecipientSkipped", to, err)
		}
	}
	if err := New(Config{}, nil).Deliverable("ops@example.com"); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("disabled Deliverable = %v, want ErrNotConfigured", err)
	}
}

// TestSiteURLNormalizedOnce: mail.site_url is normalized in New — spaces
// and trailing slashes removed — so every link builder, the web package's
// confirmation link and its startup WARN read one value (round-6 review: a
// trailing space broke group and add-request links while confirmation
// links were fine).
func TestSiteURLNormalizedOnce(t *testing.T) {
	valid := Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}
	for _, tc := range []struct{ in, want string }{
		{" https://aveloxis.io/ ", "https://aveloxis.io"},
		{"https://aveloxis.io//", "https://aveloxis.io"},
		{"   ", ""},
		{"", ""},
	} {
		cfg := valid
		cfg.SiteURL = tc.in
		m := New(cfg, nil)
		if got := m.SiteURL(); got != tc.want {
			t.Errorf("SiteURL() for %q = %q, want %q", tc.in, got, tc.want)
		}
		m.sendMail = func(string, smtp.Auth, string, []string, []byte) error { return nil }
		var sent []byte
		m.sendMail = func(_ string, _ smtp.Auth, _ string, _ []string, msg []byte) error { sent = msg; return nil }
		if err := m.SendGroupApproved("user@example.com", "alice", "g", 5); err != nil {
			t.Fatal(err)
		}
		wantLink := "(your Aveloxis site URL)"
		if tc.want != "" {
			wantLink = tc.want + "/groups/5"
		}
		if !strings.Contains(string(sent), "View your group: "+wantLink+"\r\n") && !strings.Contains(string(sent), "View your group: "+wantLink+"\n") {
			t.Errorf("site_url %q: group link is not %q:\n%s", tc.in, wantLink, sent)
		}
	}
}

// TestWithSendFunc: the exported capture hook for other packages' tests is
// the same seam Send uses. Production use is refused where the method lives
// (it panics outside a test binary), not by scanning sources for a
// spelling (round-7 review: a multi-line call and a method value both
// escaped the scan).
func TestWithSendFunc(t *testing.T) {
	var to []string
	m := New(Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}, nil).
		WithSendFunc(func(_ string, _ smtp.Auth, _ string, rcpt []string, _ []byte) error { to = rcpt; return nil })
	if err := m.Send("user@example.com", "s", "b"); err != nil || len(to) != 1 || to[0] != "user@example.com" {
		t.Errorf("Send through WithSendFunc: err=%v to=%q", err, to)
	}
}
