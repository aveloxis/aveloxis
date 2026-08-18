// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// Copilot finding on PR #179 (2026-08-18), verified real as a LATENT
// hazard: handleAddOrg (web) and handleGroupAddRepo kind:"org" (api) pass
// raw trimmed user input to AddOrgToGroup, and platform.ParseOrgURL
// TOLERATES schemeless input — so "github.com/foo" produced a fully
// WORKING registration (org_name/platform parse fine, enumeration scans
// it) while the stored org_url stayed schemeless. That silently defeats
// every exact/prefix org_url matcher: ReconcileOrgRepoLinks
// (starts_with against https:// repo_git values), the v0.19.3
// GetUserGroupIDsForOrgURL bridge, and the v0.27.84
// IsOrgRegisteredAnywhere dedup (schemeless and schemed registrations of
// the SAME org would count as different orgs). Production had zero
// schemeless rows on 2026-08-18, so the fix is the store choke point all
// four callers route through — no migration.

// TestAddOrgToGroupCanonicalizesScheme pins the normalization shape: the
// scheme-prepend must happen alongside the existing trim, BEFORE the
// registered-anywhere check, the add-request write, and the
// user_org_requests INSERT all see the value.
func TestAddOrgToGroupCanonicalizesScheme(t *testing.T) {
	// NOTE: cannot comment-strip here — stripLineComments is naive and
	// would truncate the needle line at the "//" INSIDE the "https://"
	// string literal. Instead the ordering anchor below uses the CALL-site
	// form (`s.IsOrgRegisteredAnywhere(`), which prose comments at the fix
	// site never contain.
	body := extractFunctionBody(t, "web_store.go", "AddOrgToGroup")

	schemeIdx := strings.Index(body, `"https://" + orgURL`)
	if schemeIdx < 0 {
		t.Fatal("AddOrgToGroup must canonicalize schemeless org URLs " +
			"(prepend https:// when no :// present) — a schemeless org_url " +
			"row registers and scans fine but silently defeats " +
			"ReconcileOrgRepoLinks, GetUserGroupIDsForOrgURL, and " +
			"IsOrgRegisteredAnywhere")
	}
	// Ordering: normalization must precede the IsOrgRegisteredAnywhere
	// consult so schemed/schemeless registrations of the same org dedup.
	regIdx := strings.Index(body, "s.IsOrgRegisteredAnywhere(")
	if regIdx >= 0 && regIdx < schemeIdx {
		t.Error("scheme canonicalization must happen BEFORE the " +
			"s.IsOrgRegisteredAnywhere( consult — otherwise github.com/foo " +
			"and https://github.com/foo register as different orgs")
	}
}

// TestAddOrgToGroupCanonicalizesSchemeEndToEnd proves the stored row is
// canonical when the caller supplies schemeless input.
func TestAddOrgToGroupCanonicalizesSchemeEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)

	cleanup := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests
			WHERE org_url LIKE '%avtest-schemeless-org%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests
			WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = 'avtest-schemeless-admin')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name = 'avtest-schemeless-group'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = 'avtest-schemeless-admin'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Admin user so the registration bypasses the pending-approval arm and
	// lands directly in user_org_requests.
	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ('avtest-schemeless-admin', TRUE)
		RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatalf("seed admin user: %v", err)
	}
	var groupID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
		VALUES ($1, 'avtest-schemeless-group', 'approved') RETURNING group_id`, userID).Scan(&groupID); err != nil {
		t.Fatalf("seed group: %v", err)
	}

	out, err := store.AddOrgToGroup(ctx, userID, groupID, "github.com/avtest-schemeless-org")
	if err != nil {
		t.Fatalf("AddOrgToGroup: %v", err)
	}
	if !out.Registered {
		t.Fatal("admin org add must register immediately")
	}

	var stored string
	if err := store.pool.QueryRow(ctx, `
		SELECT org_url FROM aveloxis_ops.user_org_requests
		WHERE group_id = $1`, groupID).Scan(&stored); err != nil {
		t.Fatalf("read stored org_url: %v", err)
	}
	if stored != "https://github.com/avtest-schemeless-org" {
		t.Errorf("stored org_url = %q, want the canonical %q — schemeless "+
			"storage silently defeats every org_url prefix/equality matcher",
			stored, "https://github.com/avtest-schemeless-org")
	}
}
