// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// web.spa_url starts the mailed page links when it is set (2026-10-04: the
// group, pending-approvals and confirmation links; welcome and digest links
// stay on mail.site_url), exactly as mail.site_url does without it — so it takes the same rule, at load, naming the key:
// absolute http(s), a host, no query/fragment/user/spaces, and the canonical
// form with no trailing slash (every reader appends "/page.html"; one
// spelling, SR-17, instead of a trim at each reader). The review found the
// sibling unguarded: "aveloxis.io" mailed a relative link, "https://x?x=1"
// put the token inside another query, and a trailing space broke the
// confirmation link while the mails looked fine.
func TestWebSPAURLRefusedAtLoadUnlessCanonical(t *testing.T) {
	write := func(body string) string {
		p := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Hosts a browser accepts as written are accepted here too: an
	// underscore (a Docker Compose service name in development), hyphens
	// anywhere, a trailing dot, a name whose last label starts with digits.
	for _, ok := range []string{"", "https://gui.example", "http://localhost:8000", "https://[::1]:8000", "https://10.1.2.3", "https://gui.example:65535", "https://gui.example:1", "http://gui.example:443", "https://gui.example:80", "https://xn--mnchen-3ya.example", "https://gui.example:8443", "http://web_gui:8000", "https://ab--cd.example", "https://gui.example.", "https://3com.example"} {
		cfg, err := Load(write(`{"web": {"spa_url": "` + ok + `"}}`))
		if err != nil {
			t.Errorf("spa_url %q must load: %v", ok, err)
		} else if cfg.Web.SPAURL != ok {
			t.Errorf("spa_url %q loaded as %q: the value must reach the readers as written", ok, cfg.Web.SPAURL)
		}
	}
	for _, bad := range []string{
		"aveloxis.io",               // relative: the mail would carry aveloxis.io/profile.html
		"https://gui.example?x=1",   // the token would land inside another query
		"https://gui.example#frag",  // the page would be lost in the fragment
		"https://gui.example/",      // the readers append /page.html: a trailing slash doubles it
		"https://gui.example//",     // same, and safeNextTarget trims only one
		"https://gui.example ",      // breaks the confirmation link and the OAuth return
		" https://gui.example",      // same
		"https://u:p@gui.example",   // credentials in every mail
		"ftp://gui.example",         // not a web origin
		"https://gui.example:99999", // url.Parse accepts it; no browser can open it (Copilot 5407929995)
		"https://gui.example:0",
		// The value must be the origin exactly as the browser's address bar
		// shows it: safeNextTarget compares the front end's next (built from
		// location.origin) against spa_url as written, so any other spelling
		// sends every SPA sign-in to /dashboard with only a WARN (reviews
		// 2026-10-04) — the scheme's default port, a leading zero, a trailing
		// colon, uppercase, a Unicode host (the browser sends punycode), an
		// uncompressed IPv6 literal, and a PATH (the front end is served at
		// its origin's root: its pages link /assets and /login.html from /).
		"https://gui.example:443",
		"http://gui.example:80",
		"https://gui.example:08080",
		"https://gui.example:",
		"https://[::1]:",
		"https://GUI.example",
		"HTTPS://gui.example",
		"https://münchen.example",
		"https://[0:0:0:0:0:0:0:1]:8000",
		"https://gui.example/gui",
		// IPv4 shorthands a browser rewrites (WHATWG: a last label of digits
		// or 0x-hex makes the host an IPv4 candidate): 127.1 → 127.0.0.1,
		// octal, hex, a trailing dot (round-3 review).
		"http://127.1:8000",
		"https://010.1.2.3",
		"https://0xa.1.2.3",
		"https://10.1.2.3.",
		"https://167772163",
		"https://a.example,https://b.example", // one host as far as url.Parse is concerned
	} {
		_, err := Load(write(`{"web": {"spa_url": "` + bad + `"}}`))
		if err == nil || !strings.Contains(err.Error(), "web.spa_url") {
			t.Errorf("spa_url %q must be refused at load naming web.spa_url, got %v", bad, err)
		}
	}
}
