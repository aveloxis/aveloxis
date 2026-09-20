// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// v0.29.57, round 1 on the Copilot review 5260961848 fixes: the web scan and
// the CLI gained a "is this org on the deployment's GitHub host" gate, but the
// two PERIODIC paths kept enumerating the org NAME on the configured base
// whatever host the registered URL named. On an Enterprise deployment an org
// registered as https://github.com/acme was enumerated as the Enterprise
// host's "acme" — a same-named org's repos linked into the user's group. The
// consumer enforces it too (SR-18; the rejected-group gate has the same
// belt-and-braces shape): a row whose host is not the deployment's is
// skipped, logged, and left unstamped.

import (
	"fmt"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestRefreshUserOrgsSkipsAnOrgOffTheConfiguredGitHubHost(t *testing.T) {
	fx, ctx := newOrgScanFixture(t)
	suffix := time.Now().UnixNano()
	home := fmt.Sprintf("_avhome%d", suffix)
	foreign := fmt.Sprintf("_avforeign%d", suffix)
	fx.orgRepos[home] = []string{"alpha"}
	fx.orgRepos[foreign] = []string{"beta"}
	fx.cleanupOrgRepos(t, ctx, home)
	fx.cleanupOrgRepos(t, ctx, foreign)

	u, g := fx.seedUserAndGroup(t, ctx, fmt.Sprintf("_avhost%d", suffix), "H", "approved")
	fx.registerOrg(t, ctx, u, g, home) // on the deployment's host
	fx.registerOrgURL(t, ctx, u, g, "https://github.com/"+foreign, foreign)

	fx.s.refreshUserOrgs(ctx, false)

	if n := fx.hitCount("/orgs/" + foreign + "/repos"); n != 0 {
		t.Errorf("an org registered on github.com was enumerated %d times on the configured Enterprise base — the name is not the org", n)
	}
	if n := fx.hitCount("/orgs/" + home + "/repos"); n == 0 {
		t.Error("the org on the deployment's own host was never enumerated")
	}
	if n := fx.linkCount(t, ctx, g, foreign); n != 0 {
		t.Errorf("repos linked from the foreign-host org = %d, want 0", n)
	}
	if n := fx.linkCount(t, ctx, g, home); n != 1 {
		t.Errorf("repos linked from the home-host org = %d, want 1", n)
	}
	var foreignStamped, homeStamped bool
	if err := fx.pool.QueryRow(ctx, `SELECT last_scanned IS NOT NULL FROM aveloxis_ops.user_org_requests WHERE group_id = $1 AND org_name = $2`, g, foreign).Scan(&foreignStamped); err != nil {
		t.Fatal(err)
	}
	if err := fx.pool.QueryRow(ctx, `SELECT last_scanned IS NOT NULL FROM aveloxis_ops.user_org_requests WHERE group_id = $1 AND org_name = $2`, g, home).Scan(&homeStamped); err != nil {
		t.Fatal(err)
	}
	if foreignStamped {
		t.Error("the skipped row was stamped as scanned (SR-3: a marker only over work proven done)")
	}
	if !homeStamped {
		t.Error("the enumerated row was not stamped")
	}
}

func TestRefreshGitHubOrgSkipsAnOrgOffTheConfiguredGitHubHost(t *testing.T) {
	fx, ctx := newOrgScanFixture(t)
	suffix := time.Now().UnixNano()
	home := fmt.Sprintf("_avlhome%d", suffix)
	foreign := fmt.Sprintf("_avlforeign%d", suffix)
	fx.orgRepos[home] = []string{"alpha"}
	fx.orgRepos[foreign] = []string{"beta"}
	fx.cleanupOrgRepos(t, ctx, home)
	fx.cleanupOrgRepos(t, ctx, foreign)

	if n := fx.s.refreshGitHubOrg(ctx, db.OrgGroup{Name: foreign, Type: "github_org", Website: "https://github.com/" + foreign}); n != 0 {
		t.Errorf("legacy repo_group on github.com yielded %d new repos from the configured Enterprise base", n)
	}
	if n := fx.hitCount("/orgs/" + foreign + "/repos"); n != 0 {
		t.Errorf("legacy repo_group on github.com was enumerated %d times on the configured base", n)
	}
	fx.s.refreshGitHubOrg(ctx, db.OrgGroup{Name: home, Type: "github_org", Website: "https://" + fx.webHost() + "/" + home})
	if n := fx.hitCount("/orgs/" + home + "/repos"); n == 0 {
		t.Error("legacy repo_group on the deployment's own host was never enumerated")
	}
}
