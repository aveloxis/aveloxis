// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// ux_fixes_2026_08_05_test.go — api-layer pins for the v0.27.84 UX
// round: /me carries the display identity, the groups list carries
// pending_adds, and org-add responses distinguish "registered" from
// "pending".

import (
	"strings"
	"testing"
)

func TestHandleMeCarriesIdentity(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	body := extractFuncBody(t, src, "handleMe")
	for _, needle := range []string{"GetUserIdentity", `"name"`, `"avatar_url"`} {
		if !strings.Contains(body, needle) {
			t.Errorf("handleMe must carry the display identity (missing %q) — "+
				"the greeting and nav avatar render from /me, never from mocks", needle)
		}
	}
}

func TestGroupJSONCarriesPendingAdds(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	if !strings.Contains(src, `json:"pending_adds"`) {
		t.Error("groupJSON must carry pending_adds — the GUI renders the real " +
			"'additions awaiting approval' state from the groups list")
	}
}

func TestOrgAddResponseCarriesRegistered(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	body := extractFuncBody(t, src, "handleGroupAddRepo")
	if !strings.Contains(body, `"registered"`) {
		t.Error("handleGroupAddRepo must set resp[\"registered\"] for registered org adds — " +
			"the GUI distinguishes 'tracked now' (incl. the v0.27.84 auto-approve) from 'pending'")
	}
}
