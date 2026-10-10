// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// allocFixture is one account with a group, a repository override of
// allowed, and tracked repositories to link (v0.29.89, summary/53 A8).
type allocFixture struct {
	store *PostgresStore
	ctx   context.Context
	user  int
	group int64
	repos []int64
	urls  []string
	tag   string
}

// newAllocFixture makes the account (admin as asked), allowed as its
// repos_allowed override, n tracked repositories, and sets the
// repos_per_account quota to mode (restored at cleanup).
func newAllocFixture(t *testing.T, admin bool, allowed, n int, mode capacity.Mode) *allocFixture {
	t.Helper()
	store, ctx := openSR5Store(t)
	f := &allocFixture{store: store, ctx: ctx, tag: fmt.Sprintf("_avalloc%d", time.Now().UnixNano())}
	before, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	prev, had := before[capacity.QuotaReposPerAccount]
	t.Cleanup(func() {
		if had {
			cleanupExecRetry(context.Background(), store, `UPDATE aveloxis_ops.capacity_quotas SET allowed = $1, mode = $2 WHERE name = $3`,
				prev.Allowed, string(prev.Mode), capacity.QuotaReposPerAccount)
		}
	})
	if err := store.SetCapacityQuota(ctx, capacity.QuotaReposPerAccount, 1000, mode, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, admin) VALUES ($1, 'github', $2) RETURNING user_id`,
		f.tag, admin).Scan(&f.user); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN
			(SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id = $1)`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id = $1`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN
			(SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.collection_groups WHERE group_id IN
			(SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.user_capacity WHERE user_id = $1`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, f.user)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, f.tag)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, f.tag)
	})
	if err := store.SetCapacityOverride(ctx, CapacityOverride{UserID: f.user, ReposAllowed: &allowed}, 0); err != nil {
		t.Fatal(err)
	}
	if f.group, err = store.CreateUserGroup(ctx, f.user, "alloc"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		url := fmt.Sprintf("https://github.com/%s/r%d", f.tag, i)
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
			VALUES ($1, $2, $3, 1, 1) RETURNING repo_id`, url, f.tag, fmt.Sprintf("r%d", i)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at) VALUES ($1, 'queued', 100, NOW())`, id)
		f.repos = append(f.repos, id)
		f.urls = append(f.urls, url)
	}
	return f
}

func (f *allocFixture) linked(t *testing.T) int {
	t.Helper()
	n, err := f.store.AccountReach(f.ctx, f.user)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func wantExceeded(t *testing.T, err error, used, allowed, wanted int) {
	t.Helper()
	ex, ok := capacity.AsExceeded(err)
	if !ok {
		t.Fatalf("err = %v; want a *capacity.Exceeded", err)
	}
	if ex.Quota != capacity.QuotaReposPerAccount || ex.Used != used || ex.Allowed != allowed || ex.Wanted != wanted {
		t.Errorf("Exceeded = quota %s used %d allowed %d wanted %d; want %s %d %d %d",
			ex.Quota, ex.Used, ex.Allowed, ex.Wanted, capacity.QuotaReposPerAccount, used, allowed, wanted)
	}
	if ex.Contact == "" {
		t.Error("Exceeded carries no contact address; the message must say whom to write")
	}
}

// TestRepoAllocationRefusesPastTheCapAndAdmitsWhatTheAccountReaches: the
// cap (3) is reached by three links; a fourth repository is refused; one
// the account already reaches (in another group) still links — the cap
// counts distinct repositories, not links.
func TestRepoAllocationRefusesPastTheCapAndAdmitsWhatTheAccountReaches(t *testing.T) {
	f := newAllocFixture(t, false, 3, 4, capacity.Enforce)
	for _, id := range f.repos[:3] {
		if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, id); err != nil || !ok {
			t.Fatalf("link %d under the cap = %v, %v", id, ok, err)
		}
	}
	_, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[3])
	wantExceeded(t, err, 3, 3, 1)
	if got := f.linked(t); got != 3 {
		t.Errorf("reach after a refusal = %d; want 3", got)
	}
	other, err := f.store.CreateUserGroup(f.ctx, f.user, "alloc-other")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := f.store.AddRepoToGroupByID(f.ctx, other, f.repos[0]); err != nil || !ok {
		t.Errorf("a repository the account reaches, linked into a second group at the cap = %v, %v; want linked", ok, err)
	}
}

// TestRepoAllocationCountsPendingAdditions: pending repository items count
// toward the cap (operator decision 2026-10-10).
func TestRepoAllocationCountsPendingAdditions(t *testing.T) {
	f := newAllocFixture(t, false, 3, 2, capacity.Enforce)
	if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[0]); err != nil {
		t.Fatal(err)
	}
	out, err := f.store.AddReposToGroup(f.ctx, f.user, f.group, []string{f.url("new-a"), f.url("new-b")}, 0)
	if err != nil || out.Pending != 2 {
		t.Fatalf("two unknown URLs = %+v, %v; want 2 pending", out, err)
	}
	_, err = f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[1])
	wantExceeded(t, err, 3, 3, 1)
	// A rejected request no longer holds places.
	if _, _, err := f.store.DecideAddRequest(f.ctx, out.RequestID, 0, false, ""); err != nil {
		t.Fatal(err)
	}
	if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[1]); err != nil || !ok {
		t.Errorf("after the pending request is rejected = %v, %v; want linked", ok, err)
	}
}

func (f *allocFixture) url(name string) string {
	return fmt.Sprintf("https://github.com/%s/%s", f.tag, name)
}

// TestRepoAllocationApprovedItemsKeepTheirPlace: the approval flips the
// request before its pass links the items; the place each item held must
// not come free at the flip (another add would take it and the pass would
// be refused forever), and the pass links into the held place at the cap.
func TestRepoAllocationApprovedItemsKeepTheirPlace(t *testing.T) {
	f := newAllocFixture(t, false, 3, 2, capacity.Enforce)
	if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[0]); err != nil {
		t.Fatal(err)
	}
	out, err := f.store.AddReposToGroup(f.ctx, f.user, f.group, []string{f.url("held-a"), f.url("held-b")}, 0)
	if err != nil || out.Pending != 2 {
		t.Fatalf("two unknown URLs = %+v, %v; want 2 pending", out, err)
	}
	if _, changed, err := f.store.DecideAddRequest(f.ctx, out.RequestID, 0, true, ""); err != nil || !changed {
		t.Fatalf("approve = %v, %v", changed, err)
	}
	// Approved, not yet processed: the places are still held.
	_, err = f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[1])
	wantExceeded(t, err, 3, 3, 1)
	processed, failed, err := f.store.ProcessApprovedAddRequest(f.ctx, out.RequestID)
	if err != nil || processed != 2 || failed != 0 {
		t.Fatalf("the approval pass at the cap = %d processed, %d failed, %v; want 2, 0, nil", processed, failed, err)
	}
	if got := f.linked(t); got != 3 {
		t.Errorf("reach after the pass = %d; want 3 (the held places became links)", got)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, f.url("held-%"))
		cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, f.url("held-%"))
		cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1`, f.url("held-%"))
	})
}

// TestRepoAllocationRequestWriterHoldsUnderTheLock: the bulk add's
// precheck takes no lock, so the request writer decides again under it —
// at the cap a new request is refused even when the precheck is skipped.
func TestRepoAllocationRequestWriterHoldsUnderTheLock(t *testing.T) {
	f := newAllocFixture(t, false, 2, 1, capacity.Enforce)
	if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[0]); err != nil {
		t.Fatal(err)
	}
	_, err := f.store.createAddRequest(f.ctx, f.user, f.group, "repos", "", []string{f.url("w-a"), f.url("w-b")}, "pending")
	wantExceeded(t, err, 1, 2, 2)
	if _, err := f.store.createAddRequest(f.ctx, f.user, f.group, "repos", "", []string{f.url("w-a")}, "pending"); err != nil {
		t.Errorf("one item into the last place = %v; want held", err)
	}
	// Organization requests hold no repository places.
	if _, err := f.store.createAddRequest(f.ctx, f.user, f.group, "org", "https://github.com/"+f.tag, nil, "pending"); err != nil {
		t.Errorf("an org request at the cap = %v; want written", err)
	}
}

// TestRepoAllocationRejectedGroupReleasesItsPlaces: items of a group an
// administrator rejected are never processed, so they hold nothing.
func TestRepoAllocationRejectedGroupReleasesItsPlaces(t *testing.T) {
	f := newAllocFixture(t, false, 2, 1, capacity.Enforce)
	if _, err := f.store.AddReposToGroup(f.ctx, f.user, f.group, []string{f.url("rj-a"), f.url("rj-b")}, 0); err != nil {
		t.Fatal(err)
	}
	other, err := f.store.CreateUserGroup(f.ctx, f.user, "alloc-after-reject")
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.AddRepoToGroupByID(f.ctx, other, f.repos[0])
	wantExceeded(t, err, 2, 2, 1)
	mustExecRetry(f.ctx, t, f.store, `UPDATE aveloxis_ops.user_groups SET status = 'rejected' WHERE group_id = $1`, f.group)
	if ok, err := f.store.AddRepoToGroupByID(f.ctx, other, f.repos[0]); err != nil || !ok {
		t.Errorf("after the group is rejected = %v, %v; want linked", ok, err)
	}
}

// TestRepoAllocationExemptsAdminsButNotTheirTokens: an administrator's own
// session is exempt; the same account through an API token is capped.
func TestRepoAllocationExemptsAdminsButNotTheirTokens(t *testing.T) {
	f := newAllocFixture(t, true, 1, 2, capacity.Enforce)
	for _, id := range f.repos {
		if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, id); err != nil || !ok {
			t.Fatalf("admin link %d = %v, %v; want exempt", id, ok, err)
		}
	}
	other, err := f.store.CreateUserGroup(f.ctx, f.user, "alloc-token")
	if err != nil {
		t.Fatal(err)
	}
	extra := newAllocFixture(t, false, 5, 1, capacity.Enforce).repos[0]
	_, err = f.store.AddRepoToGroupByID(WithoutAdminPrivilege(f.ctx), other, extra)
	wantExceeded(t, err, 2, 1, 1)
}

// TestRepoAllocationConcurrentAddsAtTheEdgeAdmitOne: two adds at 2-of-3
// racing for the last place — exactly one wins (the per-owner lock).
func TestRepoAllocationConcurrentAddsAtTheEdgeAdmitOne(t *testing.T) {
	f := newAllocFixture(t, false, 3, 2+8, capacity.Enforce)
	for _, id := range f.repos[:2] {
		if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, id); err != nil {
			t.Fatal(err)
		}
	}
	// Each add waits at the decision point for its rivals (up to a bound):
	// without the per-owner lock all eight decide on the same reach; with it
	// they pass one at a time, each waiting out the bound.
	var arrived sync.WaitGroup
	arrived.Add(len(f.repos[2:]))
	all := make(chan struct{})
	go func() { arrived.Wait(); close(all) }()
	f.store.allocationDecided = func() {
		arrived.Done()
		select {
		case <-all:
		case <-time.After(150 * time.Millisecond):
		}
	}
	t.Cleanup(func() { f.store.allocationDecided = nil })
	var wg sync.WaitGroup
	var mu sync.Mutex
	won, refused := 0, 0
	for _, id := range f.repos[2:] {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, id)
			mu.Lock()
			defer mu.Unlock()
			switch _, capped := capacity.AsExceeded(err); {
			case err == nil && ok:
				won++
			case capped:
				refused++
			default:
				t.Errorf("concurrent add = %v, %v", ok, err)
			}
		}(id)
	}
	wg.Wait()
	if won != 1 || refused != 7 || f.linked(t) != 3 {
		t.Errorf("concurrent adds at 2-of-3: won %d refused %d reach %d; want 1, 7, 3", won, refused, f.linked(t))
	}
}

// TestRepoAllocationShadowLinksAndOffDoesNotCount: under Shadow the add
// past the cap is linked (and logged); under Off nothing is counted.
func TestRepoAllocationShadowLinksAndOffDoesNotCount(t *testing.T) {
	for _, mode := range []capacity.Mode{capacity.Shadow, capacity.Off} {
		t.Run(string(mode), func(t *testing.T) {
			f := newAllocFixture(t, false, 1, 3, mode)
			for _, id := range f.repos {
				if ok, err := f.store.AddRepoToGroupByID(f.ctx, f.group, id); err != nil || !ok {
					t.Fatalf("%s: link %d = %v, %v; want linked", mode, id, ok, err)
				}
			}
			if got := f.linked(t); got != 3 {
				t.Errorf("%s: reach = %d; want 3", mode, got)
			}
		})
	}
}

// TestRepoAllocationBulkAddIsAllOrNothing: a paste that does not fit
// writes nothing — no links, no pending request.
func TestRepoAllocationBulkAddIsAllOrNothing(t *testing.T) {
	f := newAllocFixture(t, false, 3, 3, capacity.Enforce)
	unknown := fmt.Sprintf("https://github.com/%s/not-tracked-yet", f.tag)
	_, err := f.store.AddReposToGroup(f.ctx, f.user, f.group, append(append([]string{}, f.urls...), unknown), 0)
	wantExceeded(t, err, 0, 3, 4)
	var requests int
	if err := f.store.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, f.user).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if got := f.linked(t); got != 0 || requests != 0 {
		t.Errorf("after a refused paste: reach %d, requests %d; want 0, 0", got, requests)
	}
	// The same paste without the extra fits exactly.
	out, err := f.store.AddReposToGroup(f.ctx, f.user, f.group, f.urls, 0)
	if err != nil || out.Linked != 3 {
		t.Errorf("a paste that fits = %+v, %v; want 3 linked", out, err)
	}
}

// TestRepoAllocationCollectionCopyIsAllOrNothing: copying a collection that
// does not fit links nothing and returns the refusal.
func TestRepoAllocationCollectionCopyIsAllOrNothing(t *testing.T) {
	src := newAllocFixture(t, true, 10, 3, capacity.Enforce)
	for _, id := range src.repos {
		if _, err := src.store.AddRepoToGroupByID(src.ctx, src.group, id); err != nil {
			t.Fatal(err)
		}
	}
	cid, err := src.store.CreateCollection(src.ctx, src.tag, "", 0, src.user)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), src.store, `DELETE FROM aveloxis_ops.collection_groups WHERE collection_id = $1`, cid)
		cleanupExecRetry(context.Background(), src.store, `DELETE FROM aveloxis_ops.collections WHERE collection_id = $1`, cid)
	})
	if err := src.store.AddGroupToCollection(src.ctx, cid, src.group); err != nil {
		t.Fatal(err)
	}
	f := newAllocFixture(t, false, 2, 0, capacity.Enforce)
	n, err := f.store.CopyCollectionToGroup(f.ctx, cid, f.user, f.group)
	wantExceeded(t, err, 0, 2, 3)
	if n != 0 || f.linked(t) != 0 {
		t.Errorf("a copy that does not fit linked %d (reach %d); want none", n, f.linked(t))
	}
}

// TestRepoAllocationFillModeLinksWhatFits: the comparison record and the
// organization reconcile link as many as fit and never fail for the cap.
func TestRepoAllocationFillModeLinksWhatFits(t *testing.T) {
	f := newAllocFixture(t, false, 2, 4, capacity.Enforce)
	n, err := f.store.RecordComparisonRepos(f.ctx, f.user, f.repos)
	if err != nil || n != 2 || f.linked(t) != 2 {
		t.Errorf("comparison record at cap 2 of 4 = %d, %v (reach %d); want 2 linked, no error", n, err, f.linked(t))
	}

	g := newAllocFixture(t, false, 3, 5, capacity.Enforce)
	mustExecRetry(g.ctx, t, g.store, `INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, 'github')`, g.user, g.group, "https://github.com/"+g.tag, g.tag)
	if _, err := g.store.ReconcileOrgRepoLinks(g.ctx); err != nil {
		t.Fatalf("reconcile at the cap = %v; want no error", err)
	}
	if got := g.linked(t); got != 3 {
		t.Errorf("reconcile filled reach %d; want the cap, 3", got)
	}
}

// TestRepoAllocationSharedWithMeRefusal: the Shared-with-Me auto-add at
// the cap returns the refusal as itself (the API renders it).
func TestRepoAllocationSharedWithMeRefusal(t *testing.T) {
	f := newAllocFixture(t, false, 1, 2, capacity.Enforce)
	if _, err := f.store.AddRepoToGroupByID(f.ctx, f.group, f.repos[0]); err != nil {
		t.Fatal(err)
	}
	_, err := f.store.EnsureRepoSharedWithUser(f.ctx, f.user, f.repos[1])
	wantExceeded(t, err, 1, 1, 1)
	if errors.Is(err, ErrSharedRepoNotFound) {
		t.Error("a cap refusal must not read as not-found")
	}
}
