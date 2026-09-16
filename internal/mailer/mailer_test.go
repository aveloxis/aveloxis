// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the Gmail-backed mailer (v0.19.0).
//
// We use net/smtp + smtp.gmail.com:587 with STARTTLS and an App
// Password. No third-party mail library — stdlib handles everything.
//
// Operator config in aveloxis.json:
//
//   "mail": {
//     "gmail_user": "ops@yourdomain.com",
//     "gmail_app_password": "xxxx xxxx xxxx xxxx",
//     "from_name": "Aveloxis",
//     "site_url": "https://your-host.example"
//   }
//
// SendWelcome and SendGroupApproved are the two MVP templates. Both
// run through the same Send method, which means a future template
// just adds a new public function.

package mailer

import (
	"errors"
	"net/smtp"
	"strings"
	"testing"
)

// TestMailerExists pins the type so we can construct it from the
// web server.
func TestMailerExists(t *testing.T) {
	src := mustReadMailerSource(t, "mailer.go")
	if !strings.Contains(src, "type Mailer struct") {
		t.Error("mailer.go must define type Mailer for the Gmail-backed transactional mailer")
	}
}

// TestNewMailerSignature pins the constructor signature.
func TestNewMailerSignature(t *testing.T) {
	src := mustReadMailerSource(t, "mailer.go")
	if !strings.Contains(src, "func New(") {
		t.Error("mailer.go must define New() returning *Mailer")
	}
}

// TestSendWelcomeExists pins the welcome-email template.
func TestSendWelcomeExists(t *testing.T) {
	src := mustReadMailerSource(t, "mailer.go")
	if !strings.Contains(src, "func (m *Mailer) SendWelcome(") {
		t.Error("mailer.go must define SendWelcome — sent on first signup")
	}
}

// TestSendGroupApprovedExists pins the approval-notification template.
func TestSendGroupApprovedExists(t *testing.T) {
	src := mustReadMailerSource(t, "mailer.go")
	if !strings.Contains(src, "func (m *Mailer) SendGroupApproved(") {
		t.Error("mailer.go must define SendGroupApproved — sent when an admin approves a pending group")
	}
}

// TestMailerUsesGmailSMTPHost pins the transport. Hard-coding
// smtp.gmail.com:587 is intentional; the user explicitly asked for
// Gmail rather than a generic SMTP block.
func TestMailerUsesGmailSMTPHost(t *testing.T) {
	src := mustReadMailerSource(t, "mailer.go")
	if !strings.Contains(src, "smtp.gmail.com:587") {
		t.Error("mailer.go must connect to smtp.gmail.com:587 — the Gmail SMTP submission endpoint with STARTTLS")
	}
}

// TestMailerDisabledWhenUnconfigured pins the disabled-mailer fallback
// behaviorally: with no gmail_user, Send attempts nothing and returns
// ErrNotConfigured (v0.29.28 — it used to return nil, which callers that
// report delivery read as "sent"). Fire-and-forget callers filter it with
// IsSkip, so the rest of the app still works without email.
func TestMailerDisabledWhenUnconfigured(t *testing.T) {
	m := New(Config{}, nil)
	attempted := false
	m.sendMail = func(string, smtp.Auth, string, []string, []byte) error { attempted = true; return nil }
	if err := m.Send("user@example.com", "s", "b"); !errors.Is(err, ErrNotConfigured) || !IsSkip(err) {
		t.Errorf("Send on an unconfigured mailer = %v, want ErrNotConfigured", err)
	}
	if attempted {
		t.Error("an unconfigured mailer must not attempt delivery")
	}
}

func mustReadMailerSource(t *testing.T, name string) string {
	t.Helper()
	return readSrc(t, name)
}
