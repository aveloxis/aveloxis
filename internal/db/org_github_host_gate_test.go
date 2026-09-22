// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57, round 2 on the Copilot 5260961848 fixes: the org host gate lives
// in the STORE, in both registration writers, with the deployment's GitHub
// base as a required parameter — so the web add, the portal add, the CLI
// loaders and an admin's approval cannot register an org whose NAME every
// refresh would then look up on the wrong host. The never-scanned probe
// counts only rows a refresh will enumerate, or a skipped row would re-fire
// the demand scan on every poll tick forever.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

const enterpriseBase = "https://ghe.example.invalid/api/v3"

type orgGateFixture struct {
	store *PostgresStore
	ctx   context.Context
	slug  string
}

func newOrgGateFixture(t *testing.T) *orgGateFixture {
	t.Helper()
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := &orgGateFixture{store: store, ctx: ctx, slug: fmt.Sprintf("_avorggate%d", time.Now().UnixNano())}
	clean := func() {
		p := store.pool
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1)`, fx.slug+"%")
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1))`, fx.slug+"%")
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1)`, fx.slug+"%")
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1)`, fx.slug+"%")
		_, _ = p.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE $1`, fx.slug+"%")
	}
	clean()
	t.Cleanup(clean)
	return fx
}

func (fx *orgGateFixture) userAndGroup(t *testing.T, name string, admin bool) (int, int64) {
	t.Helper()
	uid, err := fx.store.UpsertOAuthUser(fx.ctx, OAuthUserInfo{Login: fx.slug + name, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fx.store.pool.Exec(fx.ctx, `UPDATE aveloxis_ops.users SET admin = $2 WHERE user_id = $1`, uid, admin); err != nil {
		t.Fatal(err)
	}
	gid, err := fx.store.CreateUserGroup(fx.ctx, uid, fx.slug+name)
	if err != nil {
		t.Fatal(err)
	}
	return uid, gid
}

func (fx *orgGateFixture) registered(t *testing.T, gid int64) int {
	t.Helper()
	var n int
	if err := fx.store.pool.QueryRow(fx.ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (fx *orgGateFixture) pendingRequests(t *testing.T, uid int) int {
	t.Helper()
	var n int
	if err := fx.store.pool.QueryRow(fx.ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, uid).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAddOrgToGroupRefusesAnOrgOffTheGitHubHost(t *testing.T) {
	fx := newOrgGateFixture(t)
	uid, gid := fx.userAndGroup(t, "admin", true)

	for _, tc := range []struct {
		name, orgURL, base string
		refused            bool
	}{
		{"github.com org on an Enterprise deployment", "https://github.com/acme", enterpriseBase, true},
		{"Enterprise org on a public deployment", "https://ghe.example.invalid/acme", "", true},
		{"a self-host without gitlab in its name is labelled github, so it is off the host too", "https://code.example.invalid/acme", "", true},
		{"own-host org on an Enterprise deployment", "https://ghe.example.invalid/home", enterpriseBase, false},
		{"github.com org on a public deployment", "https://github.com/home", "", false},
		{"www.github.com is github.com", "https://www.github.com/www", "", false},
		{"schemeless github.com", "github.com/bare", "", false},
		{"a GitLab group is not gated by the GitHub host", "https://gitlab.com/group", enterpriseBase, false},
	} {
		before := fx.registered(t, gid)
		_, err := fx.store.AddOrgToGroup(fx.ctx, uid, gid, tc.orgURL, tc.base)
		if tc.refused {
			if !errors.Is(err, ErrOrgOffGitHubHost) {
				t.Errorf("%s: AddOrgToGroup(%q, base %q) err = %v, want ErrOrgOffGitHubHost", tc.name, tc.orgURL, tc.base, err)
			}
			if n := fx.registered(t, gid); n != before {
				t.Errorf("%s: registered a row anyway (%d → %d)", tc.name, before, n)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: AddOrgToGroup(%q, base %q) = %v, want registered", tc.name, tc.orgURL, tc.base, err)
		}
		if n := fx.registered(t, gid); n != before+1 {
			t.Errorf("%s: rows %d → %d, want one more", tc.name, before, n)
		}
	}

	// A URL that does not parse is refused by the checks that name that
	// defect, never as "off the host" — the portal's 400 text depends on it.
	if _, err := fx.store.AddOrgToGroup(fx.ctx, uid, gid, "https://github.com/\x00acme", enterpriseBase); err == nil || errors.Is(err, ErrOrgOffGitHubHost) {
		t.Errorf("unparseable org URL: err = %v, want a rejection that is NOT ErrOrgOffGitHubHost", err)
	}

	// A non-admin's foreign org must not even PEND: the request would be
	// approved into the same wrong-host row.
	nuid, ngid := fx.userAndGroup(t, "member", false)
	if _, err := fx.store.AddOrgToGroup(fx.ctx, nuid, ngid, "https://github.com/acme", enterpriseBase); !errors.Is(err, ErrOrgOffGitHubHost) {
		t.Errorf("non-admin foreign org: err = %v, want ErrOrgOffGitHubHost", err)
	}
	if n := fx.pendingRequests(t, nuid); n != 0 {
		t.Errorf("non-admin foreign org created %d add-requests, want 0", n)
	}
}

func TestDecideAddRequestRefusesAPendingOrgOffTheGitHubHost(t *testing.T) {
	fx := newOrgGateFixture(t)
	uid, gid := fx.userAndGroup(t, "req", false)
	// A request that pended BEFORE this deployment's base was set, or through
	// a writer that predates the gate.
	reqID, err := fx.store.createAddRequest(fx.ctx, uid, gid, "org", "https://github.com/foreign", nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := fx.store.DecideAddRequest(fx.ctx, reqID, 1, true, enterpriseBase); !errors.Is(err, ErrOrgOffGitHubHost) {
		t.Fatalf("approving a foreign-host org: err = %v, want ErrOrgOffGitHubHost", err)
	}
	if n := fx.registered(t, gid); n != 0 {
		t.Errorf("approval registered %d rows, want 0", n)
	}
	var status string
	if err := fx.store.pool.QueryRow(fx.ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, reqID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Errorf("request status after the refused approval = %q, want pending (the flip and the registration are one transaction)", status)
	}
	// Rejecting it is still the admin's way out.
	if _, changed, err := fx.store.DecideAddRequest(fx.ctx, reqID, 1, false, enterpriseBase); err != nil || !changed {
		t.Errorf("rejecting the foreign-host request: changed=%v err=%v, want changed", changed, err)
	}
}

func TestHasNeverScannedOrgsCountsOnlyScanEligibleRows(t *testing.T) {
	fx := newOrgGateFixture(t)
	uid, gid := fx.userAndGroup(t, "probe", true)
	baseline, err := fx.store.HasNeverScannedOrgs(fx.ctx, enterpriseBase)
	if err != nil {
		t.Fatal(err)
	}
	seed := func(orgURL, platformName string) {
		t.Helper()
		if _, err := fx.store.pool.Exec(fx.ctx, `INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform) VALUES ($1, $2, $3, $4, $5)`,
			uid, gid, orgURL, orgURL, platformName); err != nil {
			t.Fatal(err)
		}
	}
	// A github row off the host (never stamped — it would re-fire the demand
	// scan every tick) and a GitLab group (stamped by the full pass, but
	// nothing enumerates it, so a demand scan would only stamp it).
	seed("https://github.com/foreign", "github")
	seed("https://gitlab.com/group", "gitlab")
	got, err := fx.store.HasNeverScannedOrgs(fx.ctx, enterpriseBase)
	if err != nil {
		t.Fatal(err)
	}
	if got && !baseline {
		t.Error("the probe counted rows the refresh will never enumerate — the demand scan would re-fire every poll tick forever")
	}
	seed("https://ghe.example.invalid/home", "github")
	if got, err := fx.store.HasNeverScannedOrgs(fx.ctx, enterpriseBase); err != nil || !got {
		t.Errorf("probe with an eligible never-scanned row = %v, %v; want true", got, err)
	}
}
