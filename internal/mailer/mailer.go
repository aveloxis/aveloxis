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
// The mailer sends nothing when GmailUser is empty, so deployments without
// email config keep working — Send returns ErrNotConfigured, which the
// fire-and-forget callers ignore via IsSkip. Operators who want email
// enable it by populating the config block; nothing else has to change in
// the calling code.
//
// Hard-coded transport: smtp.gmail.com:587 with STARTTLS. The user
// asked for Gmail specifically (not a generic SMTP block), so the
// host is fixed; only the credentials and From metadata are config.
package mailer

import (
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"net/smtp"
	"strings"
	"time"
	"unicode"
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

	// sendMail delivers the message Send composed. nil — what New leaves —
	// means smtp.SendMail. Tests set it to capture the envelope and the
	// composed message, which nothing else can observe.
	sendMail func(addr string, a smtp.Auth, from string, to []string, msg []byte) error
}

// deliverer resolves the sender Send uses: the test seam when set, else
// smtp.SendMail. TestNewDeliversThroughSMTPSendMail pins the default.
func (m *Mailer) deliverer() func(addr string, a smtp.Auth, from string, to []string, msg []byte) error {
	if m.sendMail != nil {
		return m.sendMail
	}
	return smtp.SendMail
}

// New returns a Mailer. Safe to call with a zero Config — Send will
// then return ErrNotConfigured on every call without attempting SMTP
// (the fallback for deployments that haven't configured email yet).
//
// v0.20.14: runs ValidateAndLog against the supplied config. If
// validation fails (typo in gmail_user, wrong App Password format,
// partial config), the WARN is emitted at construction time —
// well before the first Send — and the mailer falls back to
// disabled behavior so the rest of the application keeps working.
// The caller does not need to inspect a return error; the mailer
// is always usable.
func New(cfg Config, logger *slog.Logger) *Mailer {
	// Trimmed once, here: ValidateConfig trims too and calls a
	// whitespace-only gmail_user "empty" (mailer disabled), so the stored
	// value must agree or Enabled would say on while the log said off.
	cfg.GmailUser = strings.TrimSpace(cfg.GmailUser)
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

// sanitizeHeader strips CR/LF (and other control and format runes) from a
// value destined for an SMTP header line. Header values built with
// untrusted input (the From display name, group names in approval
// subjects) could otherwise inject arbitrary headers — CWE-93 / CodeQL
// go/email-injection. The To: address does not pass through it:
// ParseRecipient validates the address instead, and Send explains why.
func sanitizeHeader(s string) string { return scrubUntrusted(s) }

// ErrRecipientNeedsQuoting reports an address whose local part is only
// valid in quoted form. See ParseRecipient.
var ErrRecipientNeedsQuoting = errors.New("mail: the address needs a quoted local part, which the SMTP envelope cannot carry")

// ErrRecipientTooLong reports an address over the SMTP length limits. See
// ParseRecipient.
var ErrRecipientTooLong = errors.New("mail: the address exceeds the SMTP length limits (RFC 5321 section 4.5.3.1)")

// RFC 5321 section 4.5.3.1.1: a local part is at most 64 octets. Section
// 4.5.3.1.3: a path — the addr-spec inside its angle brackets — is at most
// 256 octets, so the addr-spec itself at most 254.
const (
	maxLocalPartOctets = 64
	maxPathOctets      = 256
)

// ErrNotConfigured and ErrRecipientSkipped are what Send returns when it
// deliberately sends nothing: the mailer is disabled, or the recipient is
// empty or not a deliverable address. They are errors, not nil, because a
// nil read as "delivered": the vulnerability digest advanced its window
// and `aveloxis test-mail` reported success for mail never attempted.
var (
	ErrNotConfigured    = errors.New("mailer: mail is not configured")
	ErrRecipientSkipped = errors.New("mailer: recipient skipped")
)

// IsSkip reports whether err is one of Send's deliberate skips. Send has
// already logged the skip, so fire-and-forget callers — the account,
// approval and add-request notifications, which must not break on a user
// without an address or a deployment without mail — ignore it. Callers
// that report delivery must not.
func IsSkip(err error) bool {
	return errors.Is(err, ErrNotConfigured) || errors.Is(err, ErrRecipientSkipped)
}

// ParseRecipient turns one caller-supplied address into the addr-spec that
// Send puts in BOTH the To: header and the SMTP envelope, or says why it
// cannot be sent. It is the one recipient rule: Send enforces it on every
// message, and the account-email form runs it so an address Send would
// skip is refused before the user is told to check their inbox.
//
// Parsing, not character scrubbing: mail.ParseAddress yields a structured
// address, so what reaches the message is an addr-spec by construction (a
// validation barrier, not a denylist; CodeQL go/email-injection alert 197
// is about request data reaching the message). A display name or comment
// is dropped; the addr-spec is what is returned.
//
// An addr-spec whose local part needs QUOTING is refused. net/smtp writes
// the envelope as `RCPT TO:<%s>` with no quoting and only a CR/LF check, so
// `"john  smith"@example.com` goes on the wire as the invalid path
// `<john  smith@example.com>`, and a quoted `>` closes the path early and
// appends SMTP parameters of the sender's choosing. Checking the local part
// is sufficient: ParseAddress admits only a dot-atom domain or an IP domain
// literal, neither of which can hold `>` or an ASCII space or tab. (Both
// halves can still hold non-ASCII runes, including Unicode spaces; those
// cannot close the path.) Such addresses are vanishingly rare in real
// mailboxes.
//
// An address over the RFC 5321 limits (maxLocalPartOctets,
// maxPathOctets) is refused too: net/smtp writes any length it is given,
// and the form that feeds this is bounded only by the request size.
func ParseRecipient(s string) (string, error) {
	parsed, err := mail.ParseAddress(strings.TrimSpace(s))
	if err != nil {
		return "", err
	}
	addr := parsed.Address
	if len("<"+addr+">") > maxPathOctets || strings.LastIndex(addr, "@") > maxLocalPartOctets {
		return "", ErrRecipientTooLong
	}
	if (&mail.Address{Address: addr}).String() != "<"+addr+">" {
		return "", ErrRecipientNeedsQuoting
	}
	return addr, nil
}

// sanitizeSample scrubs each entry of a user-submitted URL list and joins
// them one per line. The list itself is structure the template intends; the
// ENTRIES are attacker-supplied, so each is sanitized individually rather
// than the joined blob, which would collapse the intended line breaks.
func sanitizeSample(sample []string) string {
	out := make([]string, 0, len(sample))
	for _, s := range sample {
		if v := sanitizeBodyValue(s); v != "" {
			out = append(out, v)
		}
	}
	return strings.Join(out, "\n")
}

// Subjects scrub their untrusted parts at the interpolation, even though
// Send runs sanitizeHeader over the whole header value: the subject is the
// first thing an admin reads, and relying on a distant boundary is what let
// four subject lines look unsanitized to review. scrubUntrusted is
// idempotent, so the second pass changes nothing.
//
// bodyValueMax caps one interpolated value. A login, group name, purl or
// OSV summary is a label, not a document; the cap stops a single attacker-
// supplied field from dominating an operator's mail. Presentation bound
// only — nothing is hidden that the site does not also show.
const bodyValueMax = 300

// sanitizeBodyValue makes ONE untrusted value safe to interpolate into an
// email body. Bodies are plain text after the header block, so a newline
// cannot forge a header (net/smtp's DotWriter also dot-stuffs and
// normalizes line endings, so it cannot end the DATA phase either). What it
// CAN do is forge structure: a group name containing "\n\n— Aveloxis\n\nClick
// here: http://evil" produces a message that reads as if Aveloxis wrote it,
// mailed from this domain to an admin who is about to approve something
// (CodeQL go/email-injection, alert 16). Terminal escapes and Unicode bidi
// overrides do the same to a reader.
//
// So: line breaks collapse to a space, C0/C1 controls and Unicode bidi and
// invisible-format runes are dropped, runs of whitespace collapse, and the
// result is capped. Callers pass single-line values; multi-line body
// TEMPLATES are the package's own and are not passed through this.
func sanitizeBodyValue(s string) string { return scrubUntrusted(s) }

// sanitizeBodyURL scrubs a URL destined for an email body with the same
// control/format-rune filtering as sanitizeBodyValue but WITHOUT the
// bodyValueMax cap. URLs are not label-sized values: a configured site_url
// plus the confirmation path and a 64-character token can legitimately
// exceed the cap, and truncation would silently mail a broken link ending
// in an ellipsis (Copilot review on PR #207). Its one caller's link is
// built from mail.site_url or, in local development, from a Host that
// internal/web's emailConfirmBase has parsed down to a loopback host and a
// numeric port — so no free text reaches this uncapped.
func sanitizeBodyURL(s string) string { return scrubRunes(s) }

// scrubUntrusted is the normalizer for untrusted LABEL text in this
// package, used for header values and body values alike: scrubRunes'
// filter, then the bodyValueMax cap. Subjects need it as much as bodies:
// SendGroupApproved puts the same group name in both, and the subject is
// the first thing the admin reads. A link skips only the cap
// (sanitizeBodyURL); the To: address skips both, because ParseRecipient
// validates it instead.
func scrubUntrusted(s string) string {
	s = scrubRunes(s)
	// Truncate on RUNES: len() is bytes, and slicing mid-rune emitted
	// invalid UTF-8 into a body declared charset=UTF-8 (a 300-byte cut
	// through "项目" left an orphaned 0xe9 lead byte).
	if r := []rune(s); len(r) > bodyValueMax {
		s = string(r[:bodyValueMax]) + "…"
	}
	return s
}

// scrubRunes is the filter every scrubbed value in this package passes
// through, capped (scrubUntrusted) or not (sanitizeBodyURL).
//
// Rune classes, chosen by CATEGORY rather than an enumerated list — an
// earlier version listed specific bidi and zero-width runes and missed nine
// of them (U+200E/200F/061C, U+00AD, U+2061-2064, U+FFF9-FFFB, the U+E0000
// tag block used for ASCII smuggling):
//
//   - line separators (Zl/Zp, U+2028/U+2029) and CR/LF/TAB become a space,
//     so they cannot forge structure. These were previously neutralized
//     only as a side effect of strings.Fields, which a refactor could have
//     silently undone.
//   - all format runes (Unicode Cf) are dropped: bidi overrides and
//     isolates, zero-width joiners, the BOM, interlinear annotation and the
//     tag block all live here.
//   - C0, DEL and C1 controls are dropped (terminal escapes).
//
// Then whitespace runs collapse.
func scrubRunes(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r == '\r' || r == '\n' || r == '\t':
			return ' '
		case unicode.In(r, unicode.Zl, unicode.Zp): // U+2028, U+2029
			return ' '
		case r < 0x20 || r == 0x7f: // C0 and DEL
			return -1
		case r >= 0x80 && r <= 0x9f: // C1
			return -1
		case unicode.Is(unicode.Cf, r): // every format rune
			return -1
		}
		return r
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

// Send dispatches a single email. Subject and body are plain text.
// to must be a single address ParseRecipient accepts.
//
// Send returns nil only when the message was handed to SMTP. When it
// deliberately sends nothing it returns ErrNotConfigured (the mailer is
// disabled — logged at DEBUG, since deployments without email are normal)
// or ErrRecipientSkipped (logged at WARN); IsSkip recognizes both, so
// fire-and-forget callers need not special-case "is mail configured?".
func (m *Mailer) Send(to, subject, body string) error {
	// The recipient is PARSED rather than only scrubbed (ParseRecipient
	// says why), and Send enforces that itself instead of trusting callers:
	// every recipient passes through here — OAuth-provided and confirmed
	// stored emails, operator config, and the account-email form, which
	// runs the same parser first. A missing address is common (an OAuth
	// provider that returned none) and a disabled mailer is a normal
	// deployment, so both are returned as skips for fire-and-forget callers
	// to filter with IsSkip, not treated as failures here.
	recipient, err := m.recipientFor(to)
	if err != nil {
		if m != nil && m.logger != nil {
			if errors.Is(err, ErrNotConfigured) {
				m.logger.Debug("mailer.Send skipped — gmail_user not configured",
					"to", scrubUntrusted(to), "subject", subject)
			} else {
				m.logger.Warn("mailer.Send skipped — recipient is not deliverable",
					"subject", subject, "error", err)
			}
		}
		return err
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
	// approval email). The From display name and the Subject go through
	// sanitizeHeader; the body sits after the blank line and needs no such
	// treatment.
	//
	// The To: address is NOT scrubbed: ParseRecipient has already refused
	// anything with a CR, LF, ASCII space or tab, or quoting, and a scrub
	// that changed the header but not the envelope (dropping a format rune
	// the envelope keeps, say) would make the two name different mailboxes.
	// "<" + recipient + ">" is byte-identical to the envelope path net/smtp
	// writes below.
	msg := []byte(fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: %s\r\n"+
			"Date: %s\r\n"+
			"MIME-Version: 1.0\r\n"+
			"Content-Type: text/plain; charset=UTF-8\r\n"+
			"\r\n"+
			"%s\r\n",
		from, "<"+recipient+">", sanitizeHeader(subject),
		time.Now().Format(time.RFC1123Z), body))

	// The envelope carries the same addr-spec: net/smtp writes it as
	// `RCPT TO:<recipient>`, exactly the To: header value above.
	if err := m.deliverer()(gmailSMTPHost, auth, m.cfg.GmailUser, []string{recipient}, msg); err != nil {
		if m.logger != nil {
			// The parsed address, not the raw parameter: a display name
			// or comment in `to` is unbounded.
			m.logger.Warn("mailer.Send failed",
				"to", recipient, "subject", subject, "error", err)
		}
		return fmt.Errorf("smtp send: %w", err)
	}
	return nil
}

// Enabled reports whether Send can attempt delivery at all — false for a
// nil mailer, an empty or whitespace-only gmail_user, or a config New
// refused. Callers that would promise the user an email check it first.
// Safe on a nil mailer. It is the one "is mail on?" test: Send and
// Deliverable use it too.
func (m *Mailer) Enabled() bool {
	return m != nil && m.cfg.GmailUser != ""
}

// Deliverable reports, before anything is composed, whether Send could
// hand a message for to to SMTP: nil, ErrNotConfigured, or an
// ErrRecipientSkipped wrapping the reason. It applies exactly Send's own
// refusals, for callers that must decide up front — the vulnerability
// digest checks mail.operator_email once at startup.
func (m *Mailer) Deliverable(to string) error {
	_, err := m.recipientFor(to)
	return err
}

// recipientFor is Send's and Deliverable's shared check: the addr-spec to
// send to, or why nothing can be sent.
func (m *Mailer) recipientFor(to string) (string, error) {
	if !m.Enabled() {
		return "", ErrNotConfigured
	}
	if strings.TrimSpace(to) == "" {
		return "", fmt.Errorf("%w: empty recipient", ErrRecipientSkipped)
	}
	addr, err := ParseRecipient(to)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrRecipientSkipped, err)
	}
	return addr, nil
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
`, sanitizeBodyValue(login), sanitizeBodyValue(provider), siteURL)
	return m.Send(toEmail, subject, body)
}

// SendEmailConfirmation is the email sent when a user submits an
// email at /account/email. Contains a click-through link to
// /account/email/confirm?token=... that consumes the token and
// promotes email_pending to email. v0.20.4. Tokens expire in
// EmailConfirmationLifetime (24 hours by default).
// SendEmailConfirmation mails a click-to-confirm link.
//
// confirmURL is scrubbed like any other caller-supplied value, not exempted
// as package-built: it USED to be assembled from the request Host header
// when mail.site_url was unset, which let an authenticated attacker mail a
// victim a link to the attacker's server carrying the victim's token
// (Copilot review on PR #207; CodeQL alert 197). internal/web's
// emailConfirmBase now builds it from mail.site_url, or from a Host parsed
// down to a loopback host and a numeric port, and this scrub is the second
// layer. Scrubbed with the UNCAPPED sanitizer: a legitimate site_url plus
// path and token can exceed bodyValueMax, and truncating would silently
// mail a broken link.
func (m *Mailer) SendEmailConfirmation(toEmail, login, confirmURL string) error {
	subject := "Confirm your Aveloxis email address"
	body := fmt.Sprintf(`Hello %s,

Please confirm your email address by clicking the link below:

%s

This link expires in 24 hours. If you didn't request this confirmation,
ignore this email — your account email won't change without confirming.

— Aveloxis
`, sanitizeBodyValue(login), sanitizeBodyURL(confirmURL))
	return m.Send(toEmail, subject, body)
}

// SendGroupApproved is the email sent to the requesting user when
// an admin approves their pending group. Tells them collection has
// started and points at the group's detail page.
func (m *Mailer) SendGroupApproved(toEmail, login, groupName string, groupID int64) error {
	subject := fmt.Sprintf("Your Aveloxis group '%s' has been approved", sanitizeBodyValue(groupName))
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
`, sanitizeBodyValue(login), sanitizeBodyValue(groupName), link)
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
	subject := fmt.Sprintf("Aveloxis: %s requested collection of %s", sanitizeBodyValue(requesterLogin), what)
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
`, sanitizeBodyValue(requesterLogin), what, sanitizeBodyValue(groupName), requestID, sanitizeSample(sample), link)
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
		subject = fmt.Sprintf("Your Aveloxis addition to '%s' was approved", sanitizeBodyValue(groupName))
		verdict = fmt.Sprintf(`An administrator approved adding %s to your group '%s'.
Collection has been queued — first results typically appear within an
hour; large repositories take longer.`, what, sanitizeBodyValue(groupName))
	} else {
		subject = fmt.Sprintf("Your Aveloxis addition to '%s' was declined", sanitizeBodyValue(groupName))
		verdict = fmt.Sprintf(`An administrator declined adding %s to your group '%s'.
Nothing was collected. If you believe this is a mistake, contact the
site operator.`, what, sanitizeBodyValue(groupName))
	}
	body := fmt.Sprintf("Hello %s,\n\n%s\n\n— Aveloxis\n", sanitizeBodyValue(login), verdict)
	return m.Send(toEmail, subject, body)
}

// digestBodyMaxItems caps the per-email listing so a fleet-scale burst
// of new findings can't produce a megabyte email; the subject and the
// closing line always carry the TOTAL count, so nothing is hidden —
// just not itemized past the cap. Presentation bound only.
const digestBodyMaxItems = 50

// SendVulnerabilityDigest emails the operator a digest of findings
// first detected since the previous digest window (v0.27.12). Called
// by the scheduler's digest ticker. On an unconfigured mailer nothing is
// sent and Send's ErrNotConfigured comes back, which the digest treats as
// a failed send. items must already be filtered to
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
		// Every field here is external: the summary comes from the OSV
		// feed, the rest from collected repo data.
		// Rune-based for the same reason as scrubUntrusted's cap: an OSV
		// summary is arbitrary third-party text and often non-ASCII.
		summary := sanitizeBodyValue(it.Summary)
		if r := []rune(summary); len(r) > 100 {
			summary = string(r[:100]) + "…"
		}
		fmt.Fprintf(&b, "%-8s  %s/%s\n          %s  %s\n          %s\n\n",
			sanitizeBodyValue(strings.ToUpper(it.Severity)),
			sanitizeBodyValue(it.RepoOwner), sanitizeBodyValue(it.RepoName),
			sanitizeBodyValue(it.VulnID), sanitizeBodyValue(it.PackagePurl), summary)
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
