// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

import (
	"io"
	"log/slog"
	"testing"
)

// The links in the group emails point at the pages the deployment
// actually serves (2026-10-04): the separate-repo front end's pages when
// web.spa_url is set, the web process's own pages otherwise — a plain
// aveloxis deployment keeps the old links unchanged.
func TestGroupLinksFollowTheFrontEnd(t *testing.T) {
	for _, c := range []struct{ site, spa, wantGroup, wantPending string }{
		{"https://x.example", "", "https://x.example/groups/7", "https://x.example/admin/groups/pending"},
		{"https://x.example/", "", "https://x.example/groups/7", "https://x.example/admin/groups/pending"},
		{"https://x.example", "https://x.example", "https://x.example/group.html?group=7", "https://x.example/pending-groups.html"},
		// A port composes; a trailing slash or a path never arrives (config
		// refuses them at load, TestWebSPAURLRefusedAtLoadUnlessCanonical: the
		// front end is served at its origin), so the readers take the value
		// as written.
		{"https://x.example", "https://gui.example:8443", "https://gui.example:8443/group.html?group=7", "https://gui.example:8443/pending-groups.html"},
		// No site at all: the placeholder the body already carried.
		{"", "", "(your Aveloxis site URL)", "(your Aveloxis site URL)/admin/groups/pending"},
	} {
		m := New(Config{SiteURL: c.site, SPAURL: c.spa}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if got := m.groupPageLink(7); got != c.wantGroup {
			t.Errorf("site %q spa %q: groupPageLink = %q, want %q", c.site, c.spa, got, c.wantGroup)
		}
		if got := m.pendingApprovalsLink(); got != c.wantPending {
			t.Errorf("site %q spa %q: pendingApprovalsLink = %q, want %q", c.site, c.spa, got, c.wantPending)
		}
	}
}
