// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
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
	for _, tc := range []struct {
		name    string
		siteURL string
		host    string
		tls     bool
		want    string // "" = refused
	}{
		// site_url arrives normalized from mailer.New (no trailing slash).
		{"site_url wins over a hostile Host", "https://aveloxis.io", "evil.example.com", true, "https://aveloxis.io"},
		{"site_url wins over a loopback Host", "https://aveloxis.io", "localhost:8082", false, "https://aveloxis.io"},

		{"localhost", "", "localhost", false, "http://localhost"},
		{"localhost with port", "", "localhost:8082", false, "http://localhost:8082"},
		{"upper-case localhost", "", "LOCALHOST:8082", false, "http://LOCALHOST:8082"},
		{"TLS selects https", "", "localhost:8443", true, "https://localhost:8443"},
		{"highest port", "", "localhost:65535", false, "http://localhost:65535"},
		{"IPv4 loopback", "", "127.0.0.1:8082", false, "http://127.0.0.1:8082"},
		{"IPv4 loopback range", "", "127.1.2.3", false, "http://127.1.2.3"},
		{"bracketed IPv6 with port", "", "[::1]:8082", false, "http://[::1]:8082"},
		{"bracketed IPv6 without port", "", "[::1]", false, "http://[::1]"},
		{"bare IPv6", "", "::1", false, "http://[::1]"},
		{"bare IPv6 long form", "", "0:0:0:0:0:0:0:1", false, "http://[0:0:0:0:0:0:0:1]"},
		{"bare IPv4-mapped IPv6", "", "::ffff:127.0.0.1", false, "http://[::ffff:127.0.0.1]"},
		{"bracketed IPv4-mapped IPv6", "", "[::ffff:127.0.0.1]:8082", false, "http://[::ffff:127.0.0.1]:8082"},

		// Refused: attacker-controllable or malformed.
		{"hostile host", "", "evil.example.com", false, ""},
		{"hostile host with port", "", "evil.example.com:443", false, ""},
		{"production host", "", "metrics.example.org", false, ""},
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
			o := LinkOrigin{Host: tc.host, TLS: tc.tls}
			// The Host is trusted only with web.dev_mode on (v0.29.30): a
			// same-host proxy that does not forward Host makes every
			// visitor look like 127.0.0.1. Without dev_mode no Host may
			// build a link; a configured site_url is unaffected.
			if base, ok := emailConfirmBase(tc.siteURL, o, false); tc.siteURL == "" && ok {
				t.Errorf("emailConfirmBase(site_url=%q, Host=%q) without dev_mode = %q — a request Host must not build a link outside dev_mode", tc.siteURL, tc.host, base)
			} else if tc.siteURL != "" && (!ok || base != tc.want) {
				t.Errorf("emailConfirmBase(site_url=%q) without dev_mode = %q, %v; want %q", tc.siteURL, base, ok, tc.want)
			}
			got, ok := emailConfirmBase(tc.siteURL, o, true)
			if tc.want == "" {
				if ok {
					t.Fatalf("emailConfirmBase(site_url=%q, Host=%q) = %q — this Host must be refused", tc.siteURL, tc.host, got)
				}
				return
			}
			if !ok || got != tc.want {
				t.Fatalf("emailConfirmBase(site_url=%q, Host=%q) = %q, %v; want %q", tc.siteURL, tc.host, got, ok, tc.want)
			}
			got = confirmationLink(got, "", token)
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

// emailStore is the account-e-mail half of the fake store, recording what
// SubmitAccountEmail stores.
type emailStore struct {
	pending, confirmations, clears   []string
	userIDs                          []int
	pendingErr, confirmErr, clearErr error
	ctxClear                         bool // the clear fails on a done context, like a real store call
}

func (e *emailStore) into(st *fakeStore) *fakeStore {
	st.setUserPendingEmail = func(_ context.Context, userID int, email string) error {
		e.userIDs = append(e.userIDs, userID)
		e.pending = append(e.pending, email)
		return e.pendingErr
	}
	st.clearUserPendingEmailIf = func(ctx context.Context, userID int, email string) error {
		if e.ctxClear {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		e.userIDs = append(e.userIDs, userID)
		e.clears = append(e.clears, email)
		return e.clearErr
	}
	st.createEmailConfirmation = func(_ context.Context, userID int, email string) (string, error) {
		e.userIDs = append(e.userIDs, userID)
		e.confirmations = append(e.confirmations, email)
		if e.confirmErr != nil {
			return "", e.confirmErr
		}
		return "tok123", nil
	}
	return st
}

// TestSubmitAccountEmail drives the account-email submission end to end
// against fakes of the two things it touches. It replaces a source pin
// that a round-2 review escaped twice with every test green: re-deriving
// the link from the request (the original attacker-Host bug) and storing
// the raw form value.
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
		noMailer   bool
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
		{name: "no mailer at all is refused the same way",
			form: "user@example.com", noMailer: true, host: "localhost:8082", devMode: true,
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
			es := &emailStore{pendingErr: tc.pendingErr, confirmErr: tc.confirmErr, clearErr: tc.clearErr}
			m := &fakeMailer{site: tc.site, enabled: tc.enabled, sendErr: tc.sendErr}
			opts := Options{DevMode: tc.devMode}
			if !tc.noMailer {
				opts.Mailer = m
			}
			svc, _, logs := testService(t, es.into(&fakeStore{}), opts)

			msg := svc.SubmitAccountEmail(context.Background(), Actor{UserID: 7, Kind: Session}, "alice", tc.form, LinkOrigin{Host: tc.host})

			if msg != tc.wantMsg {
				t.Errorf("message = %q, want %q", msg, tc.wantMsg)
			}
			var wantCalls []string
			if tc.wantStored != "" {
				wantCalls = []string{tc.wantStored}
			}
			if !slices.Equal(es.pending, wantCalls) {
				t.Errorf("SetUserPendingEmail received %q, want %q", es.pending, wantCalls)
			}
			if !slices.Equal(es.clears, tc.wantClears) {
				t.Errorf("ClearUserPendingEmailIf received %q, want %q", es.clears, tc.wantClears)
			}
			for _, id := range es.userIDs {
				if id != 7 {
					t.Errorf("a store call used user_id %d, want the actor's 7", id)
				}
			}
			wantTokens := wantCalls
			if tc.pendingErr != nil {
				wantTokens = nil
			}
			if !slices.Equal(es.confirmations, wantTokens) {
				t.Errorf("CreateEmailConfirmation received %q, want %q", es.confirmations, wantTokens)
			}
			sent := m.mails()
			if tc.wantLink == "" {
				if len(sent) != 0 {
					t.Errorf("mailed %+v, want nothing", sent)
				}
			} else if len(sent) != 1 || sent[0].kind != "confirmation" || sent[0].to != tc.wantStored || sent[0].login != "alice" || sent[0].link != tc.wantLink || sent[0].lifetime != db.EmailConfirmationLifetime {
				// The lifetime the mail states is the one the tokens have.
				t.Errorf("mailed %+v, want one confirmation to %q with link %q stating db.EmailConfirmationLifetime", sent, tc.wantStored, tc.wantLink)
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

// TestEmailGate: the dashboard sends a user without an address to the
// e-mail form only when that form could actually confirm one. Where no
// confirmation can be sent — mail off, or no trustworthy link base — the
// form refuses, so redirecting locked the user out of the dashboard
// (round-3 review of v0.29.28); the operator chose to let them in. A
// lookup ERROR is not "no address" (SR-5): logged at WARN, and the
// dashboard renders.
func TestEmailGate(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		confirmed, pending   string
		emailErr, pendingErr error
		enabled, devMode     bool
		noMailer             bool
		site, host           string
		wantForm             bool
		wantPending, wantLog string
	}{
		{name: "confirmed address", confirmed: "a@example.com", enabled: true, site: "https://aveloxis.io"},
		{name: "pending address is returned for the banner", pending: "a@example.com", enabled: true, site: "https://aveloxis.io", wantPending: "a@example.com"},
		{name: "no address, confirmation possible", enabled: true, site: "https://aveloxis.io", host: "aveloxis.io", wantForm: true},
		{name: "no address, loopback dev without site_url", enabled: true, devMode: true, host: "localhost:8082", wantForm: true},
		{name: "no address, loopback Host outside dev_mode (a same-host proxy)", enabled: true, host: "127.0.0.1:8082"},
		{name: "no address, mail disabled", enabled: false, site: "https://aveloxis.io"},
		{name: "no address, no mailer", noMailer: true, devMode: true, host: "localhost:8082"},
		{name: "no address, no site_url behind a proxy", enabled: true, host: "aveloxis.io"},
		{name: "whitespace addresses count as none", confirmed: "  ", pending: " ", enabled: true, site: "https://aveloxis.io", wantForm: true, wantPending: " "},
		{name: "confirmed-email lookup error", emailErr: errors.New("closed pool"), enabled: true, site: "https://aveloxis.io", wantLog: "could not read"},
		{name: "pending-email lookup error", pendingErr: errors.New("closed pool"), enabled: true, site: "https://aveloxis.io", wantLog: "could not read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{
				getUserEmail:            func(context.Context, int) (string, error) { return tc.confirmed, tc.emailErr },
				getUserLivePendingEmail: func(context.Context, int) (string, error) { return tc.pending, tc.pendingErr },
			}
			opts := Options{DevMode: tc.devMode}
			if !tc.noMailer {
				opts.Mailer = &fakeMailer{site: tc.site, enabled: tc.enabled}
			}
			svc, _, logs := testService(t, st, opts)
			form, pending := svc.EmailGate(context.Background(), Actor{UserID: 7, Kind: Session}, LinkOrigin{Host: tc.host})
			if form != tc.wantForm || pending != tc.wantPending {
				t.Errorf("EmailGate = %v, %q; want %v, %q", form, pending, tc.wantForm, tc.wantPending)
			}
			if tc.wantLog != "" && (!strings.Contains(logs.String(), tc.wantLog) || !strings.Contains(logs.String(), "level=WARN")) {
				t.Errorf("a lookup error must be logged at WARN with %q; log:\n%s", tc.wantLog, logs.String())
			}
		})
	}
}

// TestEmailGateOnAnEndedRequest — NET-6 review r5 F2: a request ended by
// http_timeout_seconds logged WARN "could not read the user's email …
// context deadline exceeded". The request's end is Debug; the same error
// on a live request is still WARN (r5 F1).
func TestEmailGateOnAnEndedRequest(t *testing.T) {
	err := fmt.Errorf("q: %w", context.DeadlineExceeded)
	for _, tc := range []struct {
		name     string
		ended    bool
		wantWarn bool
	}{{"ended request", true, false}, {"live request", false, true}} {
		ctx := context.Background()
		if tc.ended {
			c, cancel := context.WithCancel(ctx)
			cancel()
			ctx = c
		}
		st := &fakeStore{getUserEmail: func(context.Context, int) (string, error) { return "", err }}
		svc, _, logs := testService(t, st, Options{})
		svc.EmailGate(ctx, Actor{UserID: 7, Kind: Session}, LinkOrigin{})
		if got := strings.Contains(logs.String(), "level=WARN"); got != tc.wantWarn {
			t.Errorf("%s: WARN logged = %v; want %v:\n%s", tc.name, got, tc.wantWarn, logs.String())
		}
	}
}

// TestConfirmationSendFailureIsLoggedAfterTheClientLeft — NET-6 review r6
// F2: the SMTP send takes no context, so its failure can never be the
// request's end; wrapped in LogFailure it dropped to Debug whenever nginx
// had already given up on the request (an SMTP dial can outlast nginx's
// 60 s). It is a WARN regardless.
func TestConfirmationSendFailureIsLoggedAfterTheClientLeft(t *testing.T) {
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	es := &emailStore{}
	m := &fakeMailer{site: "https://aveloxis.io", enabled: true, sendErr: errors.New("dial tcp: i/o timeout")}
	svc, _, logs := testService(t, es.into(&fakeStore{}), Options{Mailer: m})
	svc.SubmitAccountEmail(gone, Actor{UserID: 7, Kind: Session}, "alice", "user@example.com", LinkOrigin{})
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "failed to send confirmation email") {
		t.Errorf("an SMTP failure must be a WARN even after the client left:\n%s", logs.String())
	}
}

// TestPendingEmailIsClearedAfterTheClientLeft — NET-6 review r7 F1: when
// the confirmation send fails after the request ended (an SMTP dial can
// outlast nginx and the bound), the clean-up ran on the cancelled request
// context, failed at once, and was logged at Debug — leaving the pending
// address set, so the dashboard kept announcing a link that was never
// sent. The clean-up belongs to a write that already committed: it runs
// detached from the request, bounded on its own.
func TestPendingEmailIsClearedAfterTheClientLeft(t *testing.T) {
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	es := &emailStore{ctxClear: true}
	m := &fakeMailer{site: "https://aveloxis.io", enabled: true, sendErr: errors.New("dial tcp: i/o timeout")}
	svc, _, logs := testService(t, es.into(&fakeStore{}), Options{Mailer: m})
	svc.SubmitAccountEmail(gone, Actor{UserID: 7, Kind: Session}, "alice", "user@example.com", LinkOrigin{})
	if len(es.clears) != 1 || strings.Contains(logs.String(), "failed to clear the pending email") {
		t.Errorf("the pending address must be cleared even after the client left (clears %v):\n%s", es.clears, logs.String())
	}
}

// With web.spa_url set the mailed link lands on the front end's profile
// page, which confirms through the API; without it, on this process's
// /account/email/confirm (2026-10-04). The token rides unchanged. On the
// front end it travels in the FRAGMENT, not the query (reviews
// 2026-10-04): a fragment never reaches nginx's access log, the front
// end's analytics tag excludes it, and the page moves it into the tab's
// session storage and drops it from the URL while its script loads,
// before any request is issued, so a signed-out click's login round trip
// carries a token-free ?next=. The value is taken as written: config
// refuses a non-canonical spa_url at load
// (TestWebSPAURLRefusedAtLoadUnlessCanonical), so no reader trims.
func TestConfirmationLinkFollowsTheFrontEnd(t *testing.T) {
	for _, c := range []struct{ base, spa, want string }{
		{"https://x.example", "", "https://x.example/account/email/confirm?token=abc"},
		{"https://x.example", "https://x.example", "https://x.example/profile.html#token=abc"},
		{"https://x.example", "https://gui.example:8443", "https://gui.example:8443/profile.html#token=abc"},
	} {
		if got := confirmationLink(c.base, c.spa, "abc"); got != c.want {
			t.Errorf("confirmationLink(%q, %q) = %q, want %q", c.base, c.spa, got, c.want)
		}
	}
	// The Service mails the front-end link when SPAURL is set.
	es := &emailStore{}
	m := &fakeMailer{site: "https://x.example", enabled: true}
	svc, _, _ := testService(t, es.into(&fakeStore{}), Options{Mailer: m, SPAURL: "https://gui.example"})
	if msg := svc.SubmitAccountEmail(context.Background(), Actor{UserID: 7, Kind: Session}, "alice", "user@example.com", LinkOrigin{}); msg != "" {
		t.Fatal(msg)
	}
	if sent := m.mails(); len(sent) != 1 || sent[0].link != "https://gui.example/profile.html#token=tok123" {
		t.Errorf("mailed %+v", sent)
	}
}

// TestConfirmAccountEmail: one classification of the token's errors for
// the web page and the API.
func TestConfirmAccountEmail(t *testing.T) {
	for _, tc := range []struct {
		name    string
		token   string
		err     error
		want    ConfirmOutcome
		wantLog string
		reads   bool
	}{
		{"confirmed", "tok", nil, ConfirmConfirmed, "account email confirmed", true},
		{"empty token never reaches the store", "  ", nil, ConfirmInvalid, "", false},
		{"another account's token", "tok", &db.TokenOwnerMismatchError{OwnerID: 9}, ConfirmInvalid, "belongs to another account", true},
		{"unknown, expired or used", "tok", db.ErrConfirmationTokenInvalid, ConfirmInvalid, "rejected: unknown, expired, or already used", true},
		{"database failure", "tok", errors.New("closed pool"), ConfirmError, "failed to confirm user email", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			st := &fakeStore{confirmEmailToken: func(_ context.Context, token string, uid int) (string, error) {
				reads++
				if token != "tok" || uid != 7 {
					t.Errorf("store received %q for user %d", token, uid)
				}
				return "", tc.err
			}}
			svc, _, logs := testService(t, st, Options{})
			if got := svc.ConfirmAccountEmail(context.Background(), Actor{UserID: 7, Kind: Session}, tc.token); got != tc.want {
				t.Errorf("= %v, want %v", got, tc.want)
			}
			if (reads > 0) != tc.reads {
				t.Errorf("store reads = %d", reads)
			}
			if tc.wantLog != "" && !strings.Contains(logs.String(), tc.wantLog) {
				t.Errorf("log lacks %q:\n%s", tc.wantLog, logs.String())
			}
		})
	}
}
