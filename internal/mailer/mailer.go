// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package mailer sends transactional emails through Gmail SMTP using
// stdlib net/smtp.
//
// Setup (operator-side, see README):
//  1. Enable 2-Step Verification on the Gmail account
//  2. Generate an "App Password" for "Mail"
//  3. Add the credentials to aveloxis.json under the "mail" block
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

	// OperatorEmail is where fleet-level operator notifications go
	// (v0.27.12 vuln digest; v0.27.20 add-request submissions).
	// Populated from config.MailConfig.OperatorEmail by main.go;
	// empty = those notifications are silently skipped.
	OperatorEmail string `json:"operator_email"`
}

// OperatorEmail exposes the configured operator address so callers
// (web/api handlers) can address operator notifications without
// carrying the config block themselves.
func (m *Mailer) OperatorEmail() string {
	if m == nil {
		return ""
	}
	return m.cfg.OperatorEmail
}

// Mailer sends transactional emails. Construct via New.
type Mailer struct {
	cfg    Config
	logger *slog.Logger
}

// New returns a Mailer. Safe to call with a zero Config — Send will
// then early-return on every call (no-op fallback for deployments
// that haven't configured email yet).
//
// v0.20.14: runs ValidateAndLog against the supplied config. If
// validation fails (typo in gmail_user, wrong App Password format,
// partial config), the WARN is emitted at construction time —
// well before the first Send — and the mailer falls back to
// disabled behavior so the rest of the application keeps working.
// The caller does not need to inspect a return error; the mailer
// is always usable.
func New(cfg Config, logger *slog.Logger) *Mailer {
	if err := ValidateAndLog(cfg, logger); err != nil {
		// Validation failed: drop the bad config and behave as
		// if email were unconfigured. Send will hit its empty-
		// user early return on every call. The operator sees
		// the WARN at startup and the situation is recoverable
		// by fixing the config and restarting.
		return &Mailer{cfg: Config{}, logger: logger}
	}
	return &Mailer{cfg: cfg, logger: logger}
}

// sanitizeHeader strips CR/LF (and other ASCII control characters)
// from a value destined for an SMTP header line. Header values built
// with untrusted input (recipient addresses from the account-email
// form, group names in approval subjects) could otherwise inject
// arbitrary headers — CWE-93 / CodeQL go/email-injection.
func sanitizeHeader(s string) string {
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.ReplaceAll(s, "\n", "")
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
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

	// v0.20.14: strip display-format spaces from the App Password
	// (`abcd efgh ijkl mnop` → `abcdefghijklmnop`) so the value
	// operators copy-paste from Google's UI auths correctly.
	auth := smtp.PlainAuth("", m.cfg.GmailUser, normalizeAppPassword(m.cfg.GmailAppPassword), "smtp.gmail.com")

	from := m.cfg.GmailUser
	if m.cfg.FromName != "" {
		from = fmt.Sprintf("%s <%s>", sanitizeHeader(m.cfg.FromName), m.cfg.GmailUser)
	}

	// Header values are interpolated into the RFC 5322 header block, so
	// a CR/LF inside one would inject arbitrary headers (CWE-93 — e.g.
	// a group named "x\r\nBcc: ..." reaching the Subject line via the
	// approval email). Strip line breaks from every header value; the
	// body sits after the blank line and needs no such treatment.
	msg := []byte(fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: %s\r\n"+
			"Date: %s\r\n"+
			"MIME-Version: 1.0\r\n"+
			"Content-Type: text/plain; charset=UTF-8\r\n"+
			"\r\n"+
			"%s\r\n",
		from, sanitizeHeader(to), sanitizeHeader(subject),
		time.Now().Format(time.RFC1123Z), body))

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

// addRequestSampleMax bounds the URL listing in the operator's
// new-add-request email — a 50K-URL paste must not produce a
// megabyte email. The total count is always stated. Presentation
// bound only.
const addRequestSampleMax = 15

// SendAddRequestSubmitted notifies the operator that a non-admin
// submitted new (not-yet-tracked) content for approval (v0.27.20
// per-add approval). kind is "repos" or "org"; sample carries item
// URLs (or the org URL). No-op when to is empty.
func (m *Mailer) SendAddRequestSubmitted(to, requesterLogin, groupName, kind string, count int, sample []string, requestID int64) error {
	if to == "" {
		return nil
	}
	what := fmt.Sprintf("%d new repositories", count)
	if kind == "org" {
		what = "an organization"
	}
	subject := fmt.Sprintf("Aveloxis: %s requested collection of %s", requesterLogin, what)
	if len(sample) > addRequestSampleMax {
		sample = sample[:addRequestSampleMax]
	}
	siteURL := strings.TrimRight(m.cfg.SiteURL, "/")
	link := "(your Aveloxis site URL)/admin/groups/pending"
	if siteURL != "" {
		link = siteURL + "/admin/groups/pending"
	}
	body := fmt.Sprintf(`User %s asked to add %s to their group '%s'
(request #%d). None of it is currently collected, so collection will
not start until an administrator approves the request.

%s

Review pending additions: %s

— Aveloxis
`, requesterLogin, what, groupName, requestID, strings.Join(sample, "\n"), link)
	return m.Send(to, subject, body)
}

// SendAddRequestDecided notifies the requesting user of the admin's
// decision on their add-request (v0.27.20). No-op when toEmail is
// empty (user without an email on file).
func (m *Mailer) SendAddRequestDecided(toEmail, login, groupName, kind string, approved bool, count int) error {
	if toEmail == "" {
		return nil
	}
	what := fmt.Sprintf("%d repositories", count)
	if kind == "org" {
		what = "the organization you requested"
	}
	var subject, verdict string
	if approved {
		subject = fmt.Sprintf("Your Aveloxis addition to '%s' was approved", groupName)
		verdict = fmt.Sprintf(`An administrator approved adding %s to your group '%s'.
Collection has been queued — first results typically appear within an
hour; large repositories take longer.`, what, groupName)
	} else {
		subject = fmt.Sprintf("Your Aveloxis addition to '%s' was declined", groupName)
		verdict = fmt.Sprintf(`An administrator declined adding %s to your group '%s'.
Nothing was collected. If you believe this is a mistake, contact the
site operator.`, what, groupName)
	}
	body := fmt.Sprintf("Hello %s,\n\n%s\n\n— Aveloxis\n", login, verdict)
	return m.Send(toEmail, subject, body)
}

// digestBodyMaxItems caps the per-email listing so a fleet-scale burst
// of new findings can't produce a megabyte email; the subject and the
// closing line always carry the TOTAL count, so nothing is hidden —
// just not itemized past the cap. Presentation bound only.
const digestBodyMaxItems = 50

// SendVulnerabilityDigest emails the operator a digest of findings
// first detected since the previous digest window (v0.27.12). Called
// by the scheduler's digest ticker; no-op when the mailer is
// unconfigured (Send handles that). items must already be filtered to
// the operator's severity floor and ordered most-severe-first.
func (m *Mailer) SendVulnerabilityDigest(to string, since time.Time, items []VulnDigestItem) error {
	if len(items) == 0 {
		return nil
	}
	critical := 0
	for _, it := range items {
		if strings.EqualFold(it.Severity, "CRITICAL") {
			critical++
		}
	}
	subject := fmt.Sprintf("Aveloxis: %d new vulnerability finding(s)", len(items))
	if critical > 0 {
		subject += fmt.Sprintf(" (%d critical)", critical)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "New vulnerability findings detected since %s (UTC):\n\n",
		since.UTC().Format("2006-01-02 15:04"))
	shown := items
	if len(shown) > digestBodyMaxItems {
		shown = shown[:digestBodyMaxItems]
	}
	for _, it := range shown {
		summary := it.Summary
		if len(summary) > 100 {
			summary = summary[:100] + "…"
		}
		fmt.Fprintf(&b, "%-8s  %s/%s\n          %s  %s\n          %s\n\n",
			strings.ToUpper(it.Severity), it.RepoOwner, it.RepoName,
			it.VulnID, it.PackagePurl, summary)
	}
	if len(items) > len(shown) {
		fmt.Fprintf(&b, "…and %d more finding(s) not itemized here.\n\n", len(items)-len(shown))
	}
	if m.SiteURL() != "" {
		fmt.Fprintf(&b, "Dashboard: %s\n", m.SiteURL())
	}
	b.WriteString("\nYou receive this because mail.operator_email is configured in aveloxis.json.\n")
	return m.Send(to, subject, b.String())
}

// VulnDigestItem mirrors db.VulnDigestItem's display fields. Declared
// here (not imported) so the mailer package keeps zero aveloxis
// dependencies — callers copy the fields across.
type VulnDigestItem struct {
	RepoOwner   string
	RepoName    string
	VulnID      string
	Severity    string
	PackagePurl string
	Summary     string
}
