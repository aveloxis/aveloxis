// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"crypto/tls"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestEmailConfirmURL pins which Host headers may be trusted to build an
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
func TestEmailConfirmURL(t *testing.T) {
	const token = "0123abcd"
	const path = "/account/email/confirm?token=" + token
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
			got, ok := emailConfirmURL(tc.siteURL, r, token)
			if tc.want == "" {
				if ok {
					t.Fatalf("emailConfirmURL(site_url=%q, Host=%q) = %q — this Host must be refused", tc.siteURL, tc.host, got)
				}
				return
			}
			if !ok || got != tc.want {
				t.Fatalf("emailConfirmURL(site_url=%q, Host=%q) = %q, %v; want %q", tc.siteURL, tc.host, got, ok, tc.want)
			}
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

// TestHandleAccountEmailUsesTheSharedParsers is the wiring half of the two
// runtime tests: the handler must take the address through the mailer's
// recipient parser (the same one Send enforces) and build the link only
// through emailConfirmURL. Assembling a URL inline from r.Host is how the
// unchecked Host reached the mail before.
func TestHandleAccountEmailUsesTheSharedParsers(t *testing.T) {
	src := srctest.Read(t, "internal/web/server.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Server) handleAccountEmail("))
	for _, needle := range []string{
		"email, err := mailer.ParseRecipient(r.FormValue(\"email\"))",
		"confirmURL, ok := emailConfirmURL(s.mailer.SiteURL(), r, token)",
		"s.mailer.SendEmailConfirmation(email, sess.LoginName, confirmURL)",
	} {
		if n := strings.Count(body, needle); n != 1 {
			t.Errorf("handleAccountEmail contains %q %d times, want exactly once", needle, n)
		}
	}
	// Any "://" — `scheme + "://"`, `"http://" + r.Host` — means a URL is
	// being assembled here rather than in emailConfirmURL.
	if strings.Contains(body, "://") {
		t.Error("handleAccountEmail assembles a URL inline — build the confirmation link only through emailConfirmURL, which validates the Host")
	}
}
