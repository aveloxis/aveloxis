// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestEmailConfirmBase pins which Host headers may be trusted to build an
// emailed confirmation link when mail.site_url is unset. Everything
// non-loopback is attacker-reachable: the client sets Host, and a proxy
// forwarding it (nginx's usual `proxy_set_header Host $host`) passes it
// straight through. Trusting one let an attacker mail a victim a link to the
// attacker's server carrying the victim's confirmation token (Copilot, PR
// #207; CodeQL alert 197).
//
// The Host is parsed ONCE and that parse feeds both the loopback decision
// and the link. The port must be a canonical decimal in 1-65535: an earlier
// check ignored the port, so `localhost:<any text>` passed and the text rode
// into the mailed link. Malformed brackets and bracketed non-IPv6 hosts are
// refused, and every IPv6 literal — IPv4-mapped included — is bracketed in
// the link.
func TestEmailConfirmBase(t *testing.T) {
	const token = "0123abcd"
	const path = ""
	for _, tc := range []struct {
		name    string
		siteURL string
		host    string
		tls     bool
		want    string // "" = refused
	}{
		// site_url arrives normalized from mailer.New (no trailing slash).
		{"site_url wins over a hostile Host", "https://aveloxis.io", "evil.example.com", true, "https://aveloxis.io" + path},
		{"site_url wins over a loopback Host", "https://aveloxis.io", "localhost:8082", false, "https://aveloxis.io" + path},

		{"localhost", "", "localhost", false, "http://localhost" + path},
		{"localhost with port", "", "localhost:8082", false, "http://localhost:8082" + path},
		{"upper-case localhost", "", "LOCALHOST:8082", false, "http://LOCALHOST:8082" + path},
		{"TLS selects https", "", "localhost:8443", true, "https://localhost:8443" + path},
		{"highest port", "", "localhost:65535", false, "http://localhost:65535" + path},
		{"IPv4 loopback", "", "127.0.0.1:8082", false, "http://127.0.0.1:8082" + path},
		{"IPv4 loopback range", "", "127.1.2.3", false, "http://127.1.2.3" + path},
		{"bracketed IPv6 with port", "", "[::1]:8082", false, "http://[::1]:8082" + path},
		{"bracketed IPv6 without port", "", "[::1]", false, "http://[::1]" + path},
		{"bare IPv6", "", "::1", false, "http://[::1]" + path},
		{"bare IPv6 long form", "", "0:0:0:0:0:0:0:1", false, "http://[0:0:0:0:0:0:0:1]" + path},
		{"bare IPv4-mapped IPv6", "", "::ffff:127.0.0.1", false, "http://[::ffff:127.0.0.1]" + path},
		{"bracketed IPv4-mapped IPv6", "", "[::ffff:127.0.0.1]:8082", false, "http://[::ffff:127.0.0.1]:8082" + path},

		// Refused: attacker-controllable or malformed.
		{"hostile host", "", "evil.example.com", false, ""},
		{"hostile host with port", "", "evil.example.com:443", false, ""},
		{"production host", "", "chaoss.tv", false, ""},
		{"loopback-looking subdomain", "", "localhost.evil.example.com", false, ""},
		{"IPv4-looking subdomain", "", "127.0.0.1.evil.example.com", false, ""},
		{"unspecified address", "", "0.0.0.0", false, ""},
		{"private address", "", "10.0.0.5", false, ""},
		{"non-loopback IPv6", "", "[2001:db8::1]:8082", false, ""},
		{"empty", "", "", false, ""},
		{"text in the port", "", "localhost:URGENT-reverify-at-evil.example.com", false, ""},
		{"hostname in the port", "", "localhost:evil.com", false, ""},
		{"empty port", "", "localhost:", false, ""},
		{"port zero", "", "localhost:0", false, ""},
		{"port out of range", "", "localhost:65536", false, ""},
		{"port with a leading zero", "", "localhost:08082", false, ""},
		{"signed port", "", "localhost:+8082", false, ""},
		{"unclosed bracket", "", "[::1", false, ""},
		{"unopened bracket", "", "::1]", false, ""},
		{"bracketed hostname", "", "[localhost]", false, ""},
		{"bracketed hostname with port", "", "[localhost]:8082", false, ""},
		{"bracketed IPv4", "", "[127.0.0.1]:8082", false, ""},
		{"bracketed IPv6 with empty port", "", "[::1]:", false, ""},
		{"IPv6 zone", "", "[::1%25lo0]:8082", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/account/email", nil)
			r.Host = tc.host
			if tc.tls {
				r.TLS = &tls.ConnectionState{}
			}
			// The Host is trusted only with web.dev_mode on (v0.29.30): a
			// same-host proxy that does not forward Host makes every
			// visitor look like 127.0.0.1. Without dev_mode no Host may
			// build a link; a configured site_url is unaffected.
			site := tc.siteURL
			if base, ok := emailConfirmBase(tc.siteURL, r, false); site == "" && ok {
				t.Errorf("emailConfirmBase(site_url=%q, Host=%q) without dev_mode = %q — a request Host must not build a link outside dev_mode", tc.siteURL, tc.host, base)
			} else if site != "" && (!ok || base != tc.want) {
				t.Errorf("emailConfirmBase(site_url=%q) without dev_mode = %q, %v; want %q", tc.siteURL, base, ok, tc.want)
			}
			got, ok := emailConfirmBase(tc.siteURL, r, true)
			if tc.want == "" {
				if ok {
					t.Fatalf("emailConfirmBase(site_url=%q, Host=%q) = %q — this Host must be refused", tc.siteURL, tc.host, got)
				}
				return
			}
			if !ok || got != tc.want {
				t.Fatalf("emailConfirmBase(site_url=%q, Host=%q) = %q, %v; want %q", tc.siteURL, tc.host, got, ok, tc.want)
			}
			got = confirmationLink(got, token)
			if tc.siteURL != "" {
				return
			}
			// Every link built from a Host must be a valid URL whose host is
			// loopback, whose port is empty or numeric, and whose token
			// survived — the property, not just the table's spelling.
			u, err := url.Parse(got)
			if err != nil {
				t.Fatalf("link %q does not parse: %v", got, err)
			}
			h := u.Hostname()
			if ip := net.ParseIP(h); !strings.EqualFold(h, "localhost") && (ip == nil || !ip.IsLoopback()) {
				t.Errorf("link %q has non-loopback host %q", got, h)
			}
			if p := u.Port(); p != "" {
				if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
					t.Errorf("link %q has port %q", got, p)
				}
			}
			if u.Path != "/account/email/confirm" || u.Query().Get("token") != token {
				t.Errorf("link %q lost its path or token", got)
			}
		})
	}
}

// TestSubmitAccountEmail drives the account-email POST end to end against
// fakes of the two things it touches. It replaces a source pin that a
// round-2 review escaped twice with every test green: re-deriving the link
// as `(&url.URL{Host: r.Host, ...}).String()` (the original attacker-Host
// bug) and storing the raw form value.
//
// The order is part of the contract: whether a link can be mailed at all is
// decided BEFORE anything is stored, so a refusal leaves no email_pending
// behind for the dashboard to announce as "we sent a confirmation link".
func TestSubmitAccountEmail(t *testing.T) {
	const (
		invalid    = "Please enter a valid email address."
		notEnabled = "Email confirmation is not configured on this site. Contact the operator."
		saveFailed = "Could not save email. Try again."
		tokenFail  = "Could not generate confirmation. Try again."
		sendFailed = "We couldn't send the confirmation email. If one does arrive, its link still works; otherwise try again later."
	)
	for _, tc := range []struct {
		name       string
		form       string
		site       string
		enabled    bool
		host       string
		devMode    bool
		pendingErr error
		clearErr   error
		confirmErr error
		sendErr    error
		wantMsg    string   // "" = accepted (the handler redirects)
		wantStored string   // the address both store calls receive; "" = no store call
		wantClears []string // addresses ClearUserPendingEmailIf received
		wantLink   string   // the mailed link; "" = nothing mailed
		wantLog    string   // a log line that must appear
		noLog      string   // a log line that must not appear
	}{
		{name: "site_url wins over a hostile Host; the bare addr-spec is stored and mailed",
			form: "Real Name <user@example.com>", site: "https://aveloxis.io", enabled: true, host: "evil.example.com",
			wantStored: "user@example.com", wantLink: "https://aveloxis.io/account/email/confirm?token=tok123"},
		{name: "loopback Host without site_url in dev_mode",
			form: "user@example.com", enabled: true, host: "localhost:8082", devMode: true,
			wantStored: "user@example.com", wantLink: "http://localhost:8082/account/email/confirm?token=tok123"},
		{name: "loopback Host without site_url outside dev_mode is refused (a same-host proxy)",
			form: "user@example.com", enabled: true, host: "127.0.0.1:8082",
			wantMsg: notEnabled, wantLog: "web.dev_mode"},
		{name: "hostile Host without site_url is refused before anything is stored",
			form: "user@example.com", enabled: true, host: "evil.example.com", devMode: true,
			wantMsg: notEnabled, wantLog: "mail.site_url is not set"},
		{name: "text in a loopback port is refused before anything is stored",
			form: "user@example.com", enabled: true, host: "localhost:URGENT-reverify-at-evil.example.com", devMode: true,
			wantMsg: notEnabled},
		{name: "mail disabled AND no link base blames the mail block",
			form: "user@example.com", enabled: false, host: "evil.example.com",
			wantMsg: notEnabled, wantLog: "mail is not configured"},
		{name: "a disabled mailer is refused before anything is stored",
			form: "user@example.com", site: "https://aveloxis.io", enabled: false, host: "aveloxis.io",
			wantMsg: notEnabled, wantLog: "mail is not configured"},
		{name: "an address that needs quoting is refused",
			form: `"john  smith"@example.com`, site: "https://aveloxis.io", enabled: true, wantMsg: invalid},
		{name: "an over-long address is refused",
			form: strings.Repeat("a", 65) + "@example.com", site: "https://aveloxis.io", enabled: true, wantMsg: invalid},
		{name: "not an address", form: "not-an-address", site: "https://aveloxis.io", enabled: true, wantMsg: invalid},
		{name: "store failure stops before a token or a mail",
			form: "user@example.com", site: "https://aveloxis.io", enabled: true, pendingErr: errors.New("db down"),
			wantMsg: saveFailed, wantStored: "user@example.com", wantLog: "failed to set pending email"},
		{name: "token failure stops before a mail",
			form: "user@example.com", site: "https://aveloxis.io", enabled: true, confirmErr: errors.New("db down"),
			wantMsg: tokenFail, wantStored: "user@example.com", wantLog: "failed to create email confirmation"},
		// A failed send clears THIS submission's pending address (only
		// while it is still this one — a second tab may have replaced it)
		// and says a link that does arrive still works: net/smtp reports
		// QUIT's error after the server accepted the message.
		// Mixed case: the clear must receive exactly the stored address
		// (ParseRecipient keeps case), or it silently clears nothing.
		{name: "a delivery failure clears its own pending address and says so",
			form: "Real Name <User@Example.com>", site: "https://aveloxis.io", enabled: true, sendErr: errors.New("535 rejected"),
			wantMsg: sendFailed, wantStored: "User@Example.com", wantClears: []string{"User@Example.com"},
			wantLink: "https://aveloxis.io/account/email/confirm?token=tok123", wantLog: "failed to send confirmation email"},
		{name: "a mailer skip is treated as unsent but not logged again",
			form: "user@example.com", site: "https://aveloxis.io", enabled: true, sendErr: mailer.ErrRecipientSkipped,
			wantMsg: sendFailed, wantStored: "user@example.com", wantClears: []string{"user@example.com"},
			wantLink: "https://aveloxis.io/account/email/confirm?token=tok123", noLog: "failed to send confirmation email"},
		{name: "a failed clear is logged",
			form: "user@example.com", site: "https://aveloxis.io", enabled: true, sendErr: errors.New("535 rejected"), clearErr: errors.New("db down"),
			wantMsg: sendFailed, wantStored: "user@example.com", wantClears: []string{"user@example.com"},
			wantLink: "https://aveloxis.io/account/email/confirm?token=tok123", wantLog: "failed to clear the pending email"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeEmailStore{pendingErr: tc.pendingErr, confirmErr: tc.confirmErr, clearErr: tc.clearErr}
			m := &fakeConfirmMailer{site: tc.site, enabled: tc.enabled, sendErr: tc.sendErr}
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			r := httptest.NewRequest("POST", "/account/email", strings.NewReader(url.Values{"email": {tc.form}}.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tc.host != "" {
				r.Host = tc.host
			}

			msg := submitAccountEmail(r.Context(), st, confirmationPolicy{mailer: m, devMode: tc.devMode}, logger, r, &Session{UserID: 7, LoginName: "alice"})

			if msg != tc.wantMsg {
				t.Errorf("message = %q, want %q", msg, tc.wantMsg)
			}
			var wantCalls []string
			if tc.wantStored != "" {
				wantCalls = []string{tc.wantStored}
			}
			if !slices.Equal(st.pending, wantCalls) {
				t.Errorf("SetUserPendingEmail received %q, want %q", st.pending, wantCalls)
			}
			if !slices.Equal(st.clears, tc.wantClears) {
				t.Errorf("ClearUserPendingEmailIf received %q, want %q", st.clears, tc.wantClears)
			}
			for _, id := range st.userIDs {
				if id != 7 {
					t.Errorf("a store call used user_id %d, want the session's 7", id)
				}
			}
			wantTokens := wantCalls
			if tc.pendingErr != nil {
				wantTokens = nil
			}
			if !slices.Equal(st.confirmations, wantTokens) {
				t.Errorf("CreateEmailConfirmation received %q, want %q", st.confirmations, wantTokens)
			}
			if tc.wantLink == "" {
				if len(m.sent) != 0 {
					t.Errorf("mailed %+v, want nothing", m.sent)
				}
			} else if len(m.sent) != 1 || m.sent[0] != (sentConfirmation{tc.wantStored, "alice", tc.wantLink, db.EmailConfirmationLifetime}) {
				// The lifetime the mail states is the one the tokens have.
				t.Errorf("mailed %+v, want one confirmation to %q with link %q stating db.EmailConfirmationLifetime", m.sent, tc.wantStored, tc.wantLink)
			}
			if tc.wantLog != "" && !strings.Contains(logs.String(), tc.wantLog) {
				t.Errorf("log lacks %q:\n%s", tc.wantLog, logs.String())
			}
			if tc.noLog != "" && strings.Contains(logs.String(), tc.noLog) {
				t.Errorf("log has %q:\n%s", tc.noLog, logs.String())
			}
		})
	}
}

type fakeEmailStore struct {
	pending, confirmations, clears   []string
	userIDs                          []int
	pendingErr, confirmErr, clearErr error
}

func (f *fakeEmailStore) SetUserPendingEmail(_ context.Context, userID int, email string) error {
	f.userIDs = append(f.userIDs, userID)
	f.pending = append(f.pending, email)
	return f.pendingErr
}

func (f *fakeEmailStore) ClearUserPendingEmailIf(_ context.Context, userID int, email string) error {
	f.userIDs = append(f.userIDs, userID)
	f.clears = append(f.clears, email)
	return f.clearErr
}

func (f *fakeEmailStore) CreateEmailConfirmation(_ context.Context, userID int, email string) (string, error) {
	f.userIDs = append(f.userIDs, userID)
	f.confirmations = append(f.confirmations, email)
	if f.confirmErr != nil {
		return "", f.confirmErr
	}
	return "tok123", nil
}

type sentConfirmation struct {
	to, login, link string
	lifetime        time.Duration
}

type fakeConfirmMailer struct {
	site    string
	enabled bool
	sendErr error
	sent    []sentConfirmation
}

func (f *fakeConfirmMailer) SiteURL() string { return f.site }
func (f *fakeConfirmMailer) Enabled() bool   { return f.enabled }
func (f *fakeConfirmMailer) SendEmailConfirmation(to, login, link string, lifetime time.Duration) error {
	f.sent = append(f.sent, sentConfirmation{to, login, link, lifetime})
	return f.sendErr
}

// TestAccountEmailSubmissionHasOneEntryPoint is the wiring half of
// TestSubmitAccountEmail: the handler must hand the POST to
// submitAccountEmail with the real store and mailer, and the operations
// that submission guards — reading the form address, storing it, mailing
// the link — must happen nowhere else in the package. The call sites are
// DERIVED from the package's sources, so a second path added anywhere
// fails here, not only a change inside handleAccountEmail.
func TestAccountEmailSubmissionHasOneEntryPoint(t *testing.T) {
	src := srctest.Read(t, "internal/web/server.go")
	handler := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Server) handleAccountEmail("))
	const call = "submitAccountEmail(r.Context(), s.store, s.confirmationPolicy(), s.logger, r, sess)"
	if n := strings.Count(handler, call); n != 1 {
		t.Errorf("handleAccountEmail contains %q %d times, want exactly once", call, n)
	}
	submit := srctest.StripGoComments(srctest.FuncBody(t, src, "func submitAccountEmail("))
	files := srctest.PackageFiles(t, "internal/web", 4)
	if _, ok := files["internal/web/server.go"]; !ok {
		t.Fatalf("package scan did not include server.go (found %d files) — the derived call-site count would be vacuous", len(files))
	}
	// Selectors, with the receiver's dot and without the call parenthesis:
	// the narrow interfaces declare these methods without a dot, and a
	// method value (`f := s.store.SetUserPendingEmail`) is a call site too.
	for _, op := range []string{".SetUserPendingEmail", ".ClearUserPendingEmailIf", ".CreateEmailConfirmation", ".SendEmailConfirmation", `.FormValue("email")`} {
		inside := strings.Count(submit, op)
		if inside == 0 {
			t.Errorf("submitAccountEmail no longer performs %s — this pin has lost its subject", op)
		}
		total := 0
		for _, body := range files { // PackageFiles excludes _test.go
			total += strings.Count(srctest.StripGoComments(body), op)
		}
		if total != inside {
			t.Errorf("%s appears %d times in internal/web but %d times in submitAccountEmail — every account-email store or mail operation must go through the tested submission", op, total, inside)
		}
	}
}

// TestEmailGateRedirect: the dashboard sends a user without an address to
// /account/email only when that form could actually confirm one. Where no
// confirmation can be sent — mail off, or no trustworthy link base — the
// form refuses, so redirecting locked the user out of the dashboard
// (round-3 review of v0.29.28); the operator chose to let them in.
func TestEmailGateRedirect(t *testing.T) {
	for _, tc := range []struct {
		name               string
		confirmed, pending string
		enabled, devMode   bool
		site, host         string
		want               bool
	}{
		{name: "confirmed address", confirmed: "a@example.com", enabled: true, site: "https://aveloxis.io", want: false},
		{name: "pending address", pending: "a@example.com", enabled: true, site: "https://aveloxis.io", want: false},
		{name: "no address, confirmation possible", enabled: true, site: "https://aveloxis.io", host: "aveloxis.io", want: true},
		{name: "no address, loopback dev without site_url", enabled: true, devMode: true, host: "localhost:8082", want: true},
		{name: "no address, loopback Host outside dev_mode (a same-host proxy)", enabled: true, host: "127.0.0.1:8082", want: false},
		{name: "no address, mail disabled", enabled: false, site: "https://aveloxis.io", want: false},
		{name: "no address, no site_url behind a proxy", enabled: true, host: "aveloxis.io", want: false},
		{name: "whitespace addresses count as none", confirmed: "  ", pending: " ", enabled: true, site: "https://aveloxis.io", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/dashboard", nil)
			if tc.host != "" {
				r.Host = tc.host
			}
			p := confirmationPolicy{mailer: &fakeConfirmMailer{site: tc.site, enabled: tc.enabled}, devMode: tc.devMode}
			if got := emailGateRedirect(tc.confirmed, tc.pending, p, r); got != tc.want {
				t.Errorf("emailGateRedirect = %v, want %v", got, tc.want)
			}
		})
	}
	var nilMailer *mailer.Mailer
	if emailGateRedirect("", "", confirmationPolicy{mailer: nilMailer, devMode: true}, httptest.NewRequest("GET", "/dashboard", nil)) {
		t.Error("a nil mailer cannot confirm anything; the gate must let the user in")
	}
}

// TestDashboardEmailGate: the dashboard's lookups feed emailGateRedirect,
// and a lookup ERROR is not "no address" (SR-5): it is logged and the
// dashboard renders, instead of sending a user who has a confirmed address
// to the email form during a database blip.
func TestDashboardEmailGate(t *testing.T) {
	mailOn := &fakeConfirmMailer{site: "https://aveloxis.io", enabled: true}
	mailOnNoSite := &fakeConfirmMailer{enabled: true}
	for _, tc := range []struct {
		name                 string
		lookup               *fakeEmailLookup
		m                    *fakeConfirmMailer
		host                 string
		devMode              bool
		wantForm             bool
		wantPending, wantLog string
	}{
		{name: "no address on a site that can confirm", lookup: &fakeEmailLookup{}, m: mailOn, wantForm: true},
		{name: "no address on a site that cannot confirm", lookup: &fakeEmailLookup{}, m: &fakeConfirmMailer{}, wantForm: false},
		{name: "pending address is returned for the banner", lookup: &fakeEmailLookup{pending: "a@example.com"}, m: mailOn, wantPending: "a@example.com"},
		// The same-host proxy case: every Host reads 127.0.0.1. Only
		// dev_mode may treat it as local development.
		{name: "no address, loopback Host, no site_url, dev_mode off", lookup: &fakeEmailLookup{}, m: mailOnNoSite, host: "127.0.0.1:8082", wantForm: false},
		{name: "no address, loopback Host, no site_url, dev_mode on", lookup: &fakeEmailLookup{}, m: mailOnNoSite, host: "127.0.0.1:8082", devMode: true, wantForm: true},
		{name: "confirmed-email lookup error", lookup: &fakeEmailLookup{emailErr: errors.New("closed pool")}, m: mailOn, wantLog: "could not read"},
		{name: "pending-email lookup error", lookup: &fakeEmailLookup{pendingErr: errors.New("closed pool")}, m: mailOn, wantLog: "could not read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/dashboard", nil)
			if tc.host != "" {
				r.Host = tc.host
			}
			var logs bytes.Buffer
			form, pending := dashboardEmailGate(r.Context(), tc.lookup, confirmationPolicy{mailer: tc.m, devMode: tc.devMode}, slog.New(slog.NewTextHandler(&logs, nil)), r, 7)
			if form != tc.wantForm || pending != tc.wantPending {
				t.Errorf("dashboardEmailGate = %v, %q; want %v, %q", form, pending, tc.wantForm, tc.wantPending)
			}
			if tc.wantLog != "" && (!strings.Contains(logs.String(), tc.wantLog) || !strings.Contains(logs.String(), "level=WARN")) {
				t.Errorf("a lookup error must be logged at WARN with %q; log:\n%s", tc.wantLog, logs.String())
			}
		})
	}
}

type fakeEmailLookup struct {
	email, pending       string
	emailErr, pendingErr error
}

func (f *fakeEmailLookup) GetUserEmail(context.Context, int) (string, error) {
	return f.email, f.emailErr
}

func (f *fakeEmailLookup) GetUserLivePendingEmail(context.Context, int) (string, error) {
	return f.pending, f.pendingErr
}

// TestWithMailerWarnsWithoutSiteURL: a working mailer with no site_url
// refuses every confirmation link outside dev_mode, so startup says so and
// names the right fix (round-5 review: the only signal was an ERROR, at
// submit time, that read as "turn on dev_mode").
func TestWithMailerWarnsWithoutSiteURL(t *testing.T) {
	valid := mailer.Config{GmailUser: "ops@example.com", GmailAppPassword: "abcdefghijklmnop"}
	for _, tc := range []struct {
		name     string
		cfg      mailer.Config
		devMode  bool
		wantWarn bool
	}{
		{name: "mail on, no site_url, dev_mode off", cfg: valid, wantWarn: true},
		{name: "whitespace site_url counts as unset", cfg: mailer.Config{GmailUser: valid.GmailUser, GmailAppPassword: valid.GmailAppPassword, SiteURL: "  "}, wantWarn: true},
		{name: "site_url set", cfg: mailer.Config{GmailUser: valid.GmailUser, GmailAppPassword: valid.GmailAppPassword, SiteURL: "https://aveloxis.io"}},
		{name: "dev_mode on", cfg: valid, devMode: true},
		{name: "mail off", cfg: mailer.Config{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			s := &Server{logger: slog.New(slog.NewTextHandler(&logs, nil))}
			s.cfg.DevMode = tc.devMode
			s.WithMailer(mailer.New(tc.cfg, nil))
			warned := strings.Contains(logs.String(), "level=WARN") && strings.Contains(logs.String(), "set mail.site_url")
			if warned != tc.wantWarn {
				t.Errorf("startup WARN = %v, want %v; log:\n%s", warned, tc.wantWarn, logs.String())
			}
		})
	}
}
