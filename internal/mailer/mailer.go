// Package mailer sends transactional emails through Gmail SMTP using
// stdlib net/smtp.
//
// Setup (operator-side, see README):
//   1. Enable 2-Step Verification on the Gmail account
//   2. Generate an "App Password" for "Mail"
//   3. Add the credentials to aveloxis.json under the "mail" block
//
// The mailer is a no-op when GmailUser is empty so deployments without
// email config keep working — Send returns nil immediately. Operators
// who want email enable it by populating the config block; nothing
// else has to change in the calling code.
//
// Hard-coded transport: smtp.gmail.com:587 with STARTTLS. The user
// asked for Gmail specifically (not a generic SMTP block), so the
// host is fixed; only the credentials and From metadata are config.
package mailer

import (
	"fmt"
	"log/slog"
	"net/smtp"
	"strings"
	"time"
)

const gmailSMTPHost = "smtp.gmail.com:587"

// Config carries the operator-supplied Gmail credentials and
// from-line metadata. Loaded from the "mail" block of aveloxis.json.
type Config struct {
	GmailUser        string `json:"gmail_user"`
	GmailAppPassword string `json:"gmail_app_password"`
	FromName         string `json:"from_name"`
	SiteURL          string `json:"site_url"`
}

// Mailer sends transactional emails. Construct via New.
type Mailer struct {
	cfg    Config
	logger *slog.Logger
}

// New returns a Mailer. Safe to call with a zero Config — Send will
// then early-return on every call (no-op fallback for deployments
// that haven't configured email yet).
func New(cfg Config, logger *slog.Logger) *Mailer {
	return &Mailer{cfg: cfg, logger: logger}
}

// Send dispatches a single email. Subject and body are plain text.
// to should be a single RFC-5322 address; the bare local-part forms
// like "alice" without an "@" will be rejected by Gmail's submission
// host.
//
// Returns nil and logs at INFO level when the mailer is unconfigured
// (GmailUser == ""). This keeps the rest of the application code
// simple — callers don't need to special-case "is mail configured?"
// in their flow.
func (m *Mailer) Send(to, subject, body string) error {
	if m == nil || m.cfg.GmailUser == "" {
		// Unconfigured: silent no-op so deployments without email
		// keep working. Log at debug so the absence is observable
		// without flooding production logs.
		if m != nil && m.logger != nil {
			m.logger.Debug("mailer.Send skipped — gmail_user not configured",
				"to", to, "subject", subject)
		}
		return nil
	}
	if strings.TrimSpace(to) == "" {
		// Recipient missing — log and skip rather than error. The
		// most common case is a user whose OAuth provider didn't
		// return an email address; we don't want that to break
		// the calling code path (account creation, group approval).
		if m.logger != nil {
			m.logger.Warn("mailer.Send skipped — empty recipient",
				"subject", subject)
		}
		return nil
	}

	auth := smtp.PlainAuth("", m.cfg.GmailUser, m.cfg.GmailAppPassword, "smtp.gmail.com")

	from := m.cfg.GmailUser
	if m.cfg.FromName != "" {
		from = fmt.Sprintf("%s <%s>", m.cfg.FromName, m.cfg.GmailUser)
	}

	msg := []byte(fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: %s\r\n"+
			"Date: %s\r\n"+
			"MIME-Version: 1.0\r\n"+
			"Content-Type: text/plain; charset=UTF-8\r\n"+
			"\r\n"+
			"%s\r\n",
		from, to, subject, time.Now().Format(time.RFC1123Z), body))

	if err := smtp.SendMail(gmailSMTPHost, auth, m.cfg.GmailUser, []string{to}, msg); err != nil {
		if m.logger != nil {
			m.logger.Warn("mailer.Send failed",
				"to", to, "subject", subject, "error", err)
		}
		return fmt.Errorf("smtp send: %w", err)
	}
	return nil
}

// SendWelcome is the email sent on first signup. Confirms the
// account exists, names the OAuth provider, and points at the
// site URL. No verification link — GitHub/GitLab have already
// verified the email before handing it to us.
// SiteURL returns the configured site URL (e.g. "https://chaoss.tv")
// or "" if unset. v0.20.4 uses this to build click-to-confirm links
// in email bodies. Safely handles a nil mailer (returns "").
func (m *Mailer) SiteURL() string {
	if m == nil {
		return ""
	}
	return m.cfg.SiteURL
}

func (m *Mailer) SendWelcome(toEmail, login, provider string) error {
	subject := "Welcome to Aveloxis"
	siteURL := m.cfg.SiteURL
	if siteURL == "" {
		siteURL = "(your Aveloxis site URL)"
	}
	body := fmt.Sprintf(`Hello %s,

Your Aveloxis account has been created via %s OAuth. You can now
log in and create groups of repositories you'd like to track.

Note: groups created by non-administrator accounts enter a pending
state and are reviewed by an administrator before collection begins.
You'll get an email when your group is approved.

Sign in: %s

— Aveloxis
`, login, provider, siteURL)
	return m.Send(toEmail, subject, body)
}

// SendEmailConfirmation is the email sent when a user submits an
// email at /account/email. Contains a click-through link to
// /account/email/confirm?token=... that consumes the token and
// promotes email_pending to email. v0.20.4. Tokens expire in
// EmailConfirmationLifetime (24 hours by default).
func (m *Mailer) SendEmailConfirmation(toEmail, login, confirmURL string) error {
	subject := "Confirm your Aveloxis email address"
	body := fmt.Sprintf(`Hello %s,

Please confirm your email address by clicking the link below:

%s

This link expires in 24 hours. If you didn't request this confirmation,
ignore this email — your account email won't change without confirming.

— Aveloxis
`, login, confirmURL)
	return m.Send(toEmail, subject, body)
}

// SendGroupApproved is the email sent to the requesting user when
// an admin approves their pending group. Tells them collection has
// started and points at the group's detail page.
func (m *Mailer) SendGroupApproved(toEmail, login, groupName string, groupID int64) error {
	subject := fmt.Sprintf("Your Aveloxis group '%s' has been approved", groupName)
	siteURL := strings.TrimRight(m.cfg.SiteURL, "/")
	link := "(your Aveloxis site URL)"
	if siteURL != "" {
		link = fmt.Sprintf("%s/groups/%d", siteURL, groupID)
	}
	body := fmt.Sprintf(`Hello %s,

An administrator has approved your group '%s'. Aveloxis will begin
collecting data for the repositories you added — first results
typically appear within an hour, full collection of issues and pull
requests can take longer for large repos.

View your group: %s

— Aveloxis
`, login, groupName, link)
	return m.Send(toEmail, subject, body)
}
