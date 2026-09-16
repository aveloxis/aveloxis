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
		{"site_url wins over a hostile Host", "https://aveloxis.io/", "evil.example.com", true, "https://aveloxis.io" + path},
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
			got, ok := emailConfirmBase(tc.siteURL, r)
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
	)
	for _, tc := range []struct {
		name       string
		form       string
		site       string
		enabled    bool
		host       string
		pendingErr error
		confirmErr error
		sendErr    error
		wantMsg    string // "" = accepted (the handler redirects)
		wantStored string // the address both store calls receive; "" = no store call
		wantLink   string // the mailed link; "" = nothing mailed
		wantLog    string // a log line that must appear
		noLog      string // a log line that must not appear
	}{
		{name: "site_url wins over a hostile Host; the bare addr-spec is stored and mailed",
			form: "Real Name <user@example.com>", site: "https://aveloxis.io", enabled: true, host: "evil.example.com",
			wantStored: "user@example.com", wantLink: "https://aveloxis.io/account/email/confirm?token=tok123"},
		{name: "loopback Host without site_url",
			form: "user@example.com", enabled: true, host: "localhost:8082",
			wantStored: "user@example.com", wantLink: "http://localhost:8082/account/email/confirm?token=tok123"},
		{name: "hostile Host without site_url is refused before anything is stored",
			form: "user@example.com", enabled: true, host: "evil.example.com",
			wantMsg: notEnabled, wantLog: "refusing to send an email confirmation link"},
		{name: "text in a loopback port is refused before anything is stored",
			form: "user@example.com", enabled: true, host: "localhost:URGENT-reverify-at-evil.example.com",
			wantMsg: notEnabled},
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
		{name: "a delivery failure is logged and the user is still redirected",
			form: "user@example.com", site: "https://aveloxis.io", enabled: true, sendErr: errors.New("535 rejected"),
			wantStored: "user@example.com", wantLink: "https://aveloxis.io/account/email/confirm?token=tok123",
			wantLog: "failed to send confirmation email"},
		{name: "a mailer skip is not logged again",
			form: "user@example.com", site: "https://aveloxis.io", enabled: true, sendErr: mailer.ErrRecipientSkipped,
			wantStored: "user@example.com", wantLink: "https://aveloxis.io/account/email/confirm?token=tok123",
			noLog: "failed to send confirmation email"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeEmailStore{pendingErr: tc.pendingErr, confirmErr: tc.confirmErr}
			m := &fakeConfirmMailer{site: tc.site, enabled: tc.enabled, sendErr: tc.sendErr}
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			r := httptest.NewRequest("POST", "/account/email", strings.NewReader(url.Values{"email": {tc.form}}.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tc.host != "" {
				r.Host = tc.host
			}

			msg := submitAccountEmail(r.Context(), st, m, logger, r, &Session{UserID: 7, LoginName: "alice"})

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
			} else if len(m.sent) != 1 || m.sent[0] != (sentConfirmation{tc.wantStored, "alice", tc.wantLink}) {
				t.Errorf("mailed %+v, want one confirmation to %q with link %q", m.sent, tc.wantStored, tc.wantLink)
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
	pending, confirmations []string
	pendingErr, confirmErr error
}

func (f *fakeEmailStore) SetUserPendingEmail(_ context.Context, _ int, email string) error {
	f.pending = append(f.pending, email)
	return f.pendingErr
}

func (f *fakeEmailStore) CreateEmailConfirmation(_ context.Context, _ int, email string) (string, error) {
	f.confirmations = append(f.confirmations, email)
	if f.confirmErr != nil {
		return "", f.confirmErr
	}
	return "tok123", nil
}

type sentConfirmation struct{ to, login, link string }

type fakeConfirmMailer struct {
	site    string
	enabled bool
	sendErr error
	sent    []sentConfirmation
}

func (f *fakeConfirmMailer) SiteURL() string { return f.site }
func (f *fakeConfirmMailer) Enabled() bool   { return f.enabled }
func (f *fakeConfirmMailer) SendEmailConfirmation(to, login, link string) error {
	f.sent = append(f.sent, sentConfirmation{to, login, link})
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
	const call = "submitAccountEmail(r.Context(), s.store, s.mailer, s.logger, r, sess)"
	if n := strings.Count(handler, call); n != 1 {
		t.Errorf("handleAccountEmail contains %q %d times, want exactly once", call, n)
	}
	submit := srctest.StripGoComments(srctest.FuncBody(t, src, "func submitAccountEmail("))
	files := srctest.PackageFiles(t, "internal/web", 4)
	if _, ok := files["internal/web/server.go"]; !ok {
		t.Fatalf("package scan did not include server.go (found %d files) — the derived call-site count would be vacuous", len(files))
	}
	// Call sites, with the receiver's dot: the narrow interfaces declare
	// these methods without one.
	for _, op := range []string{".SetUserPendingEmail(", ".CreateEmailConfirmation(", ".SendEmailConfirmation(", `.FormValue("email")`} {
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
