// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"unicode"
)

// normalizeAppPassword removes the display-format spaces Google
// inserts every four characters when showing an App Password. The
// underlying auth token is a contiguous 16-character string of
// lowercase ASCII letters; the spaces are purely a UX affordance.
// Operators who copy-paste the displayed value verbatim end up
// with the spaced form in aveloxis.json. We strip them here so
// either form works.
//
// Pre-v0.20.14 the password was passed to smtp.PlainAuth raw,
// which meant `abcd efgh ijkl mnop` was sent literally and Gmail
// rejected with `535 5.7.8 Username and Password not accepted`.
func normalizeAppPassword(p string) string {
	return strings.ReplaceAll(p, " ", "")
}

// normalizeSiteURL is mail.site_url as the mailer stores it: no surrounding
// spaces and no trailing slash, so every link builder appends paths the same
// way.
func normalizeSiteURL(site string) string {
	return strings.TrimRight(strings.TrimSpace(site), "/")
}

// validateSiteURL accepts an empty site_url or an absolute http or https URL
// whose host is a name or an IP address, with no query, fragment, user info or
// spaces: the value starts every link the mailer writes, and "aveloxis.example"
// made a relative link while "https://aveloxis.example?x=1" put the token
// inside another query (Copilot review of PR #207). url.Parse also accepts a
// list of URLs as one: "https://a.example,https://b.example" as one host
// (round-24 review), "https://a.example/,https://b.example/" as a path
// (round 25). The errors do not repeat the value, which could carry a
// password.
func validateSiteURL(site string) error {
	if site == "" {
		return nil
	}
	u, err := url.Parse(site)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || !siteHost(u.Hostname()) || strings.Count(site, "://") > 1 {
		return errors.New("mail.site_url must be an absolute http:// or https:// URL with a host, such as https://aveloxis.example")
	}
	if u.User != nil || strings.ContainsAny(site, "?#") || strings.IndexFunc(site, unicode.IsSpace) >= 0 {
		return errors.New("mail.site_url must not contain a query (?), a fragment (#), a user name or spaces")
	}
	return nil
}

// siteHost reports whether host is an IP address (an IPv6 zone allowed) or a
// name made of letters, digits, '-', '.', '_' and non-ASCII characters.
func siteHost(host string) bool {
	if host == "" {
		return false
	}
	if ip, _, _ := strings.Cut(host, "%"); net.ParseIP(ip) != nil {
		return true
	}
	for _, r := range host {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == '_' || r > unicode.MaxASCII) {
			return false
		}
	}
	return true
}

// appPasswordPattern: 16 lowercase ASCII letters, no digits, no
// specials. Google's App Password generator only emits this
// alphabet today. If Google ever expands the alphabet, this
// validator will need updating — but the failure mode is
// operator-visible (config rejected at startup with a clear
// message), not silent data loss.
func isValidAppPassword(p string) bool {
	if len(p) != 16 {
		return false
	}
	for _, r := range p {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return true
}

// ValidateConfig checks the operator-supplied mail block at
// config-load time. Returns nil for both:
//   - a fully-configured block (gmail_user contains @ AND the
//     normalized app password is 16 lowercase letters), and
//   - a fully-empty block (mailer disabled — Send attempts nothing
//     and returns ErrNotConfigured).
//
// Returns a descriptive error for everything else. The error
// message names the exact field that's wrong so operators don't
// have to guess.
//
// Called from New() (logs at WARN and continues with a disabled
// mailer if validation fails) so a misconfiguration doesn't
// break the rest of the application but is surfaced loudly at
// startup instead of waiting for the first email send.
func ValidateConfig(cfg Config) error {
	user := strings.TrimSpace(cfg.GmailUser)
	pass := normalizeAppPassword(cfg.GmailAppPassword)

	// Fully empty: mailer disabled, valid state.
	if user == "" && pass == "" {
		return nil
	}

	// Partial config: almost certainly a mistake.
	if user == "" {
		return errors.New("mail.gmail_user is empty but mail.gmail_app_password is set — either fill both fields (enabling the mailer) or leave both empty (disabling it). Gmail SMTP requires both the address and the App Password")
	}
	if pass == "" {
		return errors.New("mail.gmail_app_password is empty but mail.gmail_user is set — generate an App Password at https://myaccount.google.com/apppasswords and paste it into the config")
	}

	// gmail_user must look like an email address. Catches the
	// 2026-05-13 operator mistake of setting it to a bare domain
	// like "aveloxis.io".
	if !strings.Contains(user, "@") {
		return fmt.Errorf("mail.gmail_user %q is not an email address — Gmail SMTP requires the full address (e.g. you@gmail.com or you@yourdomain.com for Google Workspace), not the bare domain", user)
	}

	// gmail_app_password must look like a Google App Password:
	// 16 lowercase ASCII letters after stripping display spaces.
	if !isValidAppPassword(pass) {
		return fmt.Errorf("mail.gmail_app_password is %d character(s) after removing display-format spaces but Google App Passwords are exactly 16 lowercase letters. Generate one at https://myaccount.google.com/apppasswords (2-Step Verification must be enabled on the account). The displayed format `abcd efgh ijkl mnop` is fine — spaces are stripped on load. Regular account passwords do NOT work with SMTP since Google deprecated 'less secure app access' in 2022", len(pass))
	}

	// site_url starts every link the mailer writes.
	return validateSiteURL(normalizeSiteURL(cfg.SiteURL))
}

// ValidateAndLog runs ValidateConfig and emits a clear WARN line
// when it fails. Returns the same error for callers that want to
// decide what to do (e.g. fail-fast in a CLI vs a disabled mailer
// in the web server). When the config is empty (mailer disabled),
// logs an INFO so operators see the absence is intentional.
func ValidateAndLog(cfg Config, logger *slog.Logger) error {
	err := ValidateConfig(cfg)
	if err == nil {
		if logger != nil {
			if strings.TrimSpace(cfg.GmailUser) == "" {
				logger.Info("mailer disabled — mail.gmail_user is empty, transactional emails will not be sent")
			} else {
				// operator_email too: a lost one silences the add-request
				// notices and the digest, and nothing else would say so.
				logger.Info("mailer configured", "user", cfg.GmailUser, "from_name", cfg.FromName, "site_url", cfg.SiteURL, "operator_email", cfg.OperatorEmail)
			}
		}
		return nil
	}
	if logger != nil {
		logger.Warn("mailer configuration invalid — transactional emails will be skipped until this is fixed", "error", err)
	}
	return err
}
