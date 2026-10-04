// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// web.spa_url starts every mailed link when it is set (2026-10-04: the
// group, pending-approvals and confirmation links), exactly as mail.site_url
// does without it — so it takes the same rule, at load, naming the key:
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
	for _, ok := range []string{"", "https://gui.example", "http://localhost:8000", "https://gui.example/gui", "https://[::1]:8000", "https://10.1.2.3"} {
		cfg, err := Load(write(`{"web": {"spa_url": "` + ok + `"}}`))
		if err != nil {
			t.Errorf("spa_url %q must load: %v", ok, err)
		} else if cfg.Web.SPAURL != ok {
			t.Errorf("spa_url %q loaded as %q: the value must reach the readers as written", ok, cfg.Web.SPAURL)
		}
	}
	for _, bad := range []string{
		"aveloxis.io",                         // relative: the mail would carry aveloxis.io/profile.html
		"https://gui.example?x=1",             // the token would land inside another query
		"https://gui.example#frag",            // the page would be lost in the fragment
		"https://gui.example/",                // the readers append /page.html: a trailing slash doubles it
		"https://gui.example//",               // same, and safeNextTarget trims only one
		"https://gui.example ",                // breaks the confirmation link and the OAuth return
		" https://gui.example",                // same
		"https://u:p@gui.example",             // credentials in every mail
		"ftp://gui.example",                   // not a web origin
		"https://a.example,https://b.example", // one host as far as url.Parse is concerned
	} {
		_, err := Load(write(`{"web": {"spa_url": "` + bad + `"}}`))
		if err == nil || !strings.Contains(err.Error(), "web.spa_url") {
			t.Errorf("spa_url %q must be refused at load naming web.spa_url, got %v", bad, err)
		}
	}
}
