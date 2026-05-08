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

// TestMailerDisabledWhenUnconfigured pins the no-op fallback. If
// gmail_user is empty (operator hasn't configured the mailer), Send
// should silently skip rather than error out — the rest of the app
// must work without email.
func TestMailerDisabledWhenUnconfigured(t *testing.T) {
	src := mustReadMailerSource(t, "mailer.go")
	body := extractMailerFunc(src, "Send")
	if body == "" {
		t.Skip("Send not yet defined")
	}
	// We expect an early-return when GmailUser is empty.
	if !strings.Contains(body, "GmailUser") || !strings.Contains(body, `== ""`) {
		t.Error("Mailer.Send must early-return (no error) when GmailUser is empty so deployments without email config still work")
	}
}

func mustReadMailerSource(t *testing.T, name string) string {
	t.Helper()
	return readSrc(t, name)
}

func extractMailerFunc(src, name string) string {
	marker := "func (m *Mailer) " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
