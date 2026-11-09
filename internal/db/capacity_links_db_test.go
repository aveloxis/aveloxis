// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// setLinksQuota sets repo_links_per_day for the test (restored at cleanup).
func setLinksQuota(t *testing.T, f *allocFixture, allowed int, mode capacity.Mode) {
	t.Helper()
	before, err := f.store.GetCapacityQuotas(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	prev, had := before[capacity.QuotaRepoLinksPerDay]
	t.Cleanup(func() {
		if had {
			cleanupExecRetry(context.Background(), f.store, `UPDATE aveloxis_ops.capacity_quotas SET allowed = $1, mode = $2 WHERE name = $3`,
				prev.Allowed, string(prev.Mode), capacity.QuotaRepoLinksPerDay)
		}
		cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_ops.request_counts WHERE subject = $1`, linksTodayKey(f.user))
	})
	if err := f.store.SetCapacityQuota(f.ctx, capacity.QuotaRepoLinksPerDay, allowed, mode, 0); err != nil {
		t.Fatal(err)
	}
}

// The day's additions count only the targets that are linked (Copilot PR
// #252, thirteenth review): the decision counted new-to-account targets from
// the REQUESTED ids, while the insert skips an id with no repository row (a
// merge deleted it) — a failed addition still spent the allowance. The
// targets are locked and filtered inside the deciding transaction, before
// the decision, so the count and the insert see one set.
func TestRepoLinksPerDayCountsOnlyLinkedTargets(t *testing.T) {
	f := newAllocFixture(t, false, 100, 6, capacity.Enforce)
	setLinksQuota(t, f, 5, capacity.Enforce)
	deleted := f.repos[1]
	mustExecRetry(f.ctx, t, f.store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, deleted)
	mustExecRetry(f.ctx, t, f.store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, deleted)
	count := func() int {
		t.Helper()
		var n int
		if err := f.store.pool.QueryRow(f.ctx, `SELECT COALESCE((SELECT requests FROM aveloxis_ops.request_counts WHERE subject = $1 AND day = $2::date), 0)`,
			linksTodayKey(f.user), capacity.UTCDate(capacity.UTCDay.Start(f.store.capacityNow()))).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	n, err := f.store.AddRepoIDsToGroup(f.ctx, f.group, []int64{f.repos[0], deleted, f.repos[2]})
	if err != nil || n != 2 {
		t.Fatalf("a batch with one deleted target links the other two: %d %v", n, err)
	}
	if got := count(); got != 2 {
		t.Errorf("the day's additions after linking 2 of 3 requested = %d, want 2", got)
	}
	if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, deleted); !errors.Is(err, ErrRepoNotFound) {
		t.Errorf("linking the deleted repository = %v, want ErrRepoNotFound", err)
	}
	if got := count(); got != 2 {
		t.Errorf("a failed addition spends nothing: the day's additions = %d, want 2", got)
	}
	// Both deciding transactions filter their targets to existing ones before
	// the decision and charge what the insert returned.
	for _, fn := range []string{"func (s *PostgresStore) linkWithinCap(", "func (s *PostgresStore) addPasteWithinCap("} {
		body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", fn))
		filter, decide := strings.Index(body, "existingLinkTargets(ctx, tx,"), strings.Index(body, "s.decideWithinCap(ctx, tx,")
		if filter < 0 || decide < filter || !strings.Contains(body, "chargeLinked(d, linked)") {
			t.Errorf("%s must filter its targets (existingLinkTargets) before decideWithinCap and count chargeLinked(d, linked) (positions %d, %d)", fn, filter, decide)
		}
	}
}

// chargeLinked charges the admitted new-to-account targets the insert
// returned, plus the held URLs' additions; an admitted target a merge deleted
// between the read and the insert is not charged (the race the plain filter
// cannot close).
func TestChargeLinkedChargesOnlyWhatLinked(t *testing.T) {
	for _, c := range []struct {
		name   string
		d      capDecision
		linked []int64
		want   int
	}{
		{"all admitted linked", capDecision{fresh: []int64{1, 2, 3}, admit: 2, counted: 2}, []int64{1, 2}, 2},
		{"one admitted deleted before the insert", capDecision{fresh: []int64{1, 2, 3}, admit: 2, counted: 2}, []int64{1}, 1},
		{"an already-reached target linked too", capDecision{fresh: []int64{1}, admit: 1, counted: 1}, []int64{1, 9}, 1},
		{"held URLs stay counted", capDecision{fresh: []int64{1, 2}, admit: 2, counted: 5}, []int64{2}, 4},
		{"nothing counted", capDecision{fresh: []int64{1}, admit: 1}, nil, 0},
		{"none admitted", capDecision{fresh: []int64{1, 2}, admit: 0, counted: 3}, nil, 3},
	} {
		if got := chargeLinked(c.d, c.linked); got != c.want {
			t.Errorf("%s: chargeLinked = %d, want %d", c.name, got, c.want)
		}
	}
}

// A target the decision refuses is not locked (the L10 pass on the
// thirteenth-review fix: locking every requested target before the decision
// put a FOR SHARE — xmax, a dirtied page, a WAL record — on each refused one,
// and an account at its allocation is re-offered the same refused org
// repositories every cycle). Only the insert locks, and only a new link.
func TestARefusedLinkTargetIsNotLocked(t *testing.T) {
	f := newAllocFixture(t, false, 100, 6, capacity.Enforce)
	setLinksQuota(t, f, 1, capacity.Enforce)
	if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[0]); err != nil || !ok {
		t.Fatalf("the one addition today = %v, %v", ok, err)
	}
	xmax := func(id int64) string {
		t.Helper()
		var x string
		if err := f.store.pool.QueryRow(f.ctx, `SELECT xmax::text FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&x); err != nil {
			t.Fatal(err)
		}
		return x
	}
	before := xmax(f.repos[3])
	if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[3]); err == nil {
		t.Fatal("the second addition today must be refused")
	}
	if after := xmax(f.repos[3]); after != before {
		t.Errorf("a refused target must not be locked (xmax %s -> %s)", before, after)
	}
}

// v0.29.89 (operator 2026-10-10): repositories NEWLY added to an account's
// groups are counted per UTC day, so add–read–remove churn under the
// allocation is bounded. Removing does not give an addition back; a
// repository the account already reaches links freely; organization links
// are not counted.
func TestRepoLinksPerDayBoundsChurn(t *testing.T) {
	f := newAllocFixture(t, false, 100, 6, capacity.Enforce)
	setLinksQuota(t, f, 3, capacity.Enforce)
	for _, id := range f.repos[:3] {
		if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, id); err != nil || !ok {
			t.Fatalf("addition under the daily value = %v, %v", ok, err)
		}
	}
	_, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[3])
	ex, ok := capacity.AsExceeded(err)
	if !ok || ex.Quota != capacity.QuotaRepoLinksPerDay || ex.Kind != capacity.KindRate || ex.Window != capacity.UTCDay || ex.ResetAt.IsZero() {
		t.Fatalf("the 4th addition today = %v; want the repo_links_per_day rate refusal", err)
	}
	// Already reached: links freely (a second group).
	other, err := f.store.CreateUserGroup(f.ctx, f.user, "links-other")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := f.store.AddRepoToGroupByID(f.ctx, other, f.repos[0]); err != nil || !ok {
		t.Errorf("a repository the account reaches = %v, %v; want linked", ok, err)
	}
	// Churn: removing gives nothing back.
	for _, g := range []int64{f.group, other} {
		if _, err := f.store.RemoveRepoFromGroup(f.ctx, f.user, g, f.repos[0]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[0]); err == nil {
		t.Error("re-adding a removed repository must count as an addition (the churn this quota bounds)")
	}
	// Organization links an administrator approved are not the account's
	// additions (v0.29.91: only those; TestOrgLinksCountUnlessAnAdministratorDecided).
	var admin int
	if err := f.store.pool.QueryRow(f.ctx, `SELECT user_id FROM aveloxis_ops.users WHERE admin ORDER BY user_id LIMIT 1`).Scan(&admin); err != nil {
		t.Fatal(err)
	}
	registerOrgForTest(t, f, f.group, admin)
	if ok, err := f.store.AddOrgRepoToGroupByID(f.ctx, f.group, f.repos[4]); err != nil || !ok {
		t.Errorf("an administrator-approved organization link past the daily value = %v, %v; want linked (not counted)", ok, err)
	}
	// An override raises one account.
	five := 5
	if err := f.store.SetCapacityOverride(f.ctx, CapacityOverride{UserID: f.user, ReposAllowed: intPtr(100), LinksPerDay: &five}, 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[3]); err != nil || !ok {
		t.Errorf("after an override of 5 = %v, %v; want linked", ok, err)
	}
}

// Shadow: counted and logged, never refused; a bulk paste that would cross
// an enforced value adds nothing.
func TestRepoLinksPerDayShadowAndBulk(t *testing.T) {
	f := newAllocFixture(t, false, 100, 4, capacity.Enforce)
	setLinksQuota(t, f, 1, capacity.Shadow)
	for _, id := range f.repos[:2] {
		if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, id); err != nil || !ok {
			t.Fatalf("shadow = %v, %v; want linked", ok, err)
		}
	}
	if err := f.store.SetCapacityQuota(f.ctx, capacity.QuotaRepoLinksPerDay, 3, capacity.Enforce, 0); err != nil {
		t.Fatal(err)
	}
	// Two counted already (shadow still counts): a paste of two more fails whole.
	_, err := f.store.AddReposToGroup(f.ctx, f.user, f.group, f.urls[2:4], 0, "")
	if ex, ok := capacity.AsExceeded(err); !ok || ex.Quota != capacity.QuotaRepoLinksPerDay {
		t.Fatalf("a paste past the daily value = %v; want the refusal", err)
	}
	if got := f.linked(t); got != 2 {
		t.Errorf("a refused paste linked something: reach %d, want 2", got)
	}
}

func intPtr(v int) *int { return &v }

// Closing review r3 F3: login_name is unique only exactly, so a login can
// name two accounts apart from letter case; the escrow export must never
// pick one silently.
func TestAccountByLoginRefusesAmbiguity(t *testing.T) {
	store, ctx := openSR5Store(t)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.users WHERE lower(login_name) IN ('_avamb', '_avsolo')`)
	})
	for _, l := range []string{"_avAmb", "_avamb", "_avSolo"} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ($1, 'github')`, l)
	}
	// Round 4 R4-1: an exact match does not decide either — both forges
	// treat names case-insensitively, so case never says which person.
	if _, err := store.AccountByLogin(ctx, "_avAmb"); !errors.Is(err, ErrAmbiguousLogin) {
		t.Errorf("an exact login with a case twin = %v; want ErrAmbiguousLogin", err)
	}
	if _, err := store.AccountByLogin(ctx, "_AVAMB"); !errors.Is(err, ErrAmbiguousLogin) {
		t.Errorf("a login naming two accounts apart from case = %v; want ErrAmbiguousLogin", err)
	}
	if a, err := store.AccountByLogin(ctx, "_avsolo"); err != nil || a.Login != "_avSolo" || a.Provider != "github" {
		t.Errorf("one case-insensitive match = %+v, %v; want it, with its provider", a, err)
	}
	if _, err := store.AccountByLogin(ctx, "_avnobody"); !errors.Is(err, ErrUserNotFound) {
		t.Errorf("no account = %v; want ErrUserNotFound", err)
	}
}

// Round 5 R5-4: the account queries the Capacity page reads, against the
// database — ids, forge, GitLab host, a legacy empty provider (read as
// GitHub, one rule with sign-in), and the candidates of an ambiguous login.
func TestCapacityAccountQueries(t *testing.T) {
	f := newAllocFixture(t, false, 50, 2, capacity.Shadow)
	store, ctx := f.store, f.ctx
	var legacy int
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gl_oauth_host) VALUES ($1, '', '') RETURNING user_id`, f.tag+"-legacy").Scan(&legacy); err != nil {
		t.Fatal(err)
	}
	var gl int
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gl_oauth_host) VALUES ($1, 'gitlab', 'https://gitlab.example.org') RETURNING user_id`, strings.ToUpper(f.tag)).Scan(&gl); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.users WHERE user_id = ANY($1)`, []int{legacy, gl})
	})
	if a, err := store.AccountByID(ctx, legacy); err != nil || a.Provider != "github" {
		t.Errorf("a legacy account = %+v, %v; want provider github", a, err)
	}
	if a, err := store.AccountByID(ctx, gl); err != nil || a.Forge() != "gitlab (https://gitlab.example.org)" {
		t.Errorf("a GitLab account's forge = %q, %v", a.Forge(), err)
	}
	_, err := store.AccountByLogin(ctx, f.tag)
	var amb *AmbiguousLoginError
	if !errors.As(err, &amb) || len(amb.Candidates) != 2 || !strings.Contains(err.Error(), "gitlab (https://gitlab.example.org)") {
		t.Fatalf("an ambiguous login = %v; want both candidates with their forge", err)
	}
	if a, err := store.CapacityAccountByID(ctx, f.user); err != nil || a.UserID != f.user || a.Repos.Allowed != 50 {
		t.Errorf("CapacityAccountByID = %+v, %v", a, err)
	}
	// The three lists include the fixture account with its forge.
	if _, err := store.AddRepoToGroupByID(ctx, f.group, f.repos[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddRequestCounts(ctx, time.Now(), map[string]int64{requestsTodayKey(f.user): 1 << 40}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.request_counts WHERE subject = $1`, requestsTodayKey(f.user))
	})
	for name, list := range map[string]func(context.Context) ([]CapacityAccount, error){
		"overrides": store.CapacityAccountsWithOverrides, "busiest": store.BusiestAccountsToday,
	} {
		accounts, err := list(ctx)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		found := false
		for _, a := range accounts {
			if a.UserID == f.user && a.Provider == "github" && a.Login == f.tag {
				found = true
			}
		}
		if !found {
			t.Errorf("%s does not list the fixture account with its forge", name)
		}
	}
	largest, err := store.LargestAccounts(ctx)
	if err != nil {
		t.Fatalf("LargestAccounts: %v", err)
	}
	for _, a := range largest {
		if a.Provider == "" || a.Login == "" {
			t.Errorf("LargestAccounts row without identity: %+v", a)
		}
	}
}
