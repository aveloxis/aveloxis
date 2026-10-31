// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dbFixture is the DB-tier harness: a real store, its pool for seeding,
// and a Service over it with recording hooks.
type dbFixture struct {
	ctx   context.Context
	store *db.PostgresStore
	pool  *pgxpool.Pool
	svc   *Service
	hooks *recHooks
	logs  *logSink
}

func openFixture(t *testing.T) *dbFixture {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	hooks := newRecHooks()
	logs := &logSink{}
	svc := New(store, Options{}, hooks, slog.New(slog.NewTextHandler(logs, nil)))
	return &dbFixture{ctx: ctx, store: store, pool: pool, svc: svc, hooks: hooks, logs: logs}
}

// user seeds an account (admin or not) and removes it at the end.
func (f *dbFixture) user(t *testing.T, admin bool) int {
	t.Helper()
	var id int
	login := fmt.Sprintf("_avadmin_%d_%d", time.Now().UnixNano(), os.Getpid())
	if err := f.pool.QueryRow(f.ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', $2) RETURNING user_id`, login, admin).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, id)
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, id)
	})
	return id
}

// group seeds a group for the owner and removes it at the end.
func (f *dbFixture) group(t *testing.T, owner int) int64 {
	t.Helper()
	gid, err := f.store.CreateUserGroup(f.ctx, owner, fmt.Sprintf("_avadmin_grp_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, gid)
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.user_groups WHERE group_id = $1`, gid)
	})
	return gid
}

// TestGroupAccessOverTheStore (AVELOXIS_TEST_DB) drives the one access rule
// against the real store: the owner reads and writes, an administrator's
// session reads (logged), an administrator's API token and a stranger are
// refused, an unknown group is NotFound for everyone. The ownership
// assertions that lived on the store's readers (GetPortalGroupReposForUser,
// GetPortalGroupOrgsForUser, GetPendingAddItemsForUser,
// GetGroupRepoIDsForUser) live here now; those readers take no caller.
func TestGroupAccessOverTheStore(t *testing.T) {
	f := openFixture(t)
	owner, admin, stranger := f.user(t, false), f.user(t, true), f.user(t, false)
	gid := f.group(t, owner)
	for _, tc := range []struct {
		name string
		a    Actor
		want Access
		err  error
	}{
		{"owner reads", Actor{owner, Session}, Read, nil},
		{"owner writes", Actor{owner, Session}, Write, nil},
		{"owner's token reads", Actor{owner, APIToken}, Read, nil},
		{"admin session reads", Actor{admin, Session}, Read, nil},
		{"admin session writes", Actor{admin, Session}, Write, db.ErrGroupNotOwned},
		{"admin token reads", Actor{admin, APIToken}, Read, db.ErrGroupNotOwned},
		{"operator reads", Actor{0, Operator}, Read, nil},
		{"stranger reads", Actor{stranger, Session}, Read, db.ErrGroupNotOwned},
		{"stranger writes", Actor{stranger, Session}, Write, db.ErrGroupNotOwned},
	} {
		ref, err := f.svc.GroupAccess(f.ctx, tc.a, gid, tc.want)
		if !errors.Is(err, tc.err) || (tc.err == nil && (ref.ID != gid || ref.OwnerID != owner)) {
			t.Errorf("%s: GroupAccess = %+v, %v; want err %v", tc.name, ref, err, tc.err)
		}
	}
	if !strings.Contains(f.logs.String(), "read another account's group") {
		t.Errorf("an administrator's read must be logged:\n%s", f.logs.String())
	}
	for _, a := range []Actor{{owner, Session}, {admin, Session}, {0, Operator}} {
		if _, err := f.svc.GroupAccess(f.ctx, a, 1<<40, Read); !errors.Is(err, db.ErrGroupNotFound) {
			t.Errorf("unknown group for %s = %v; want ErrGroupNotFound", a, err)
		}
	}
	// A token under db.WithoutAdminPrivilege is what the API sends: the
	// store answers "not an administrator" even for the admin's own id.
	if _, err := f.svc.GroupAccess(db.WithoutAdminPrivilege(f.ctx), Actor{admin, Session}, gid, Read); !errors.Is(err, db.ErrGroupNotOwned) {
		t.Errorf("admin session under WithoutAdminPrivilege = %v; want ErrGroupNotOwned", err)
	}
	// The dumb readers answer for the group, whoever asks.
	if _, _, err := f.store.GetPortalGroupRepos(f.ctx, stranger, gid, 1, 10, "", ""); err != nil {
		t.Errorf("GetPortalGroupRepos: %v", err)
	}
	if _, err := f.store.GetGroupOrgs(f.ctx, gid); err != nil {
		t.Errorf("GetGroupOrgs: %v", err)
	}
	if _, err := f.store.GetGroupRepoIDs(f.ctx, gid); err != nil {
		t.Errorf("GetGroupRepoIDs: %v", err)
	}
}

// TestAuthorizeGroupRepoOverTheStore: membership through user_repos.
func TestAuthorizeGroupRepoOverTheStore(t *testing.T) {
	f := openFixture(t)
	owner := f.user(t, false)
	gid := f.group(t, owner)
	var repoID int64
	if err := f.pool.QueryRow(f.ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'o', 'r', 1) RETURNING repo_id`, fmt.Sprintf("https://github.com/o/r_%d", time.Now().UnixNano())).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.user_repos WHERE repo_id = $1`, repoID)
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	if _, err := f.svc.AuthorizeGroupRepo(f.ctx, Actor{owner, Session}, gid, repoID); !errors.Is(err, ErrRepoNotInGroup) {
		t.Fatalf("not yet linked = %v; want ErrRepoNotInGroup", err)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, gid, repoID); err != nil {
		t.Fatal(err)
	}
	if ref, err := f.svc.AuthorizeGroupRepo(f.ctx, Actor{owner, Session}, gid, repoID); err != nil || ref.ID != gid {
		t.Fatalf("linked = %+v, %v", ref, err)
	}
}

// TestSetUserAdminIsAtomic (AVELOXIS_TEST_DB, DBS-10): two administrators
// demoting each other at the same instant cannot both succeed — exactly
// one demotion lands and the other is ErrLastAdmin (with the bootstrap
// administrator demoted first, so these two are the only ones). An unknown
// user is ErrUserNotFound; a self-demotion never reaches the store.
func TestSetUserAdminIsAtomic(t *testing.T) {
	f := openFixture(t)
	a, b := f.user(t, true), f.user(t, true)
	// Every other administrator steps down for the duration (restored by
	// the cleanup), so a and b are the last two.
	rows, err := f.pool.Query(f.ctx, `SELECT user_id FROM aveloxis_ops.users WHERE admin AND user_id NOT IN ($1, $2)`, a, b)
	if err != nil {
		t.Fatal(err)
	}
	var others []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		others = append(others, id)
	}
	rows.Close()
	if _, err := f.pool.Exec(f.ctx, `UPDATE aveloxis_ops.users SET admin = FALSE WHERE user_id = ANY($1)`, others); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = ANY($1)`, others)
	})
	for round := 0; round < 5; round++ {
		if _, err := f.pool.Exec(f.ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id IN ($1, $2)`, a, b); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		start := make(chan struct{})
		for i, pair := range [][2]int{{a, b}, {b, a}} {
			wg.Add(1)
			go func(i int, actor, target int) {
				defer wg.Done()
				<-start
				errs[i] = f.svc.SetUserAdmin(f.ctx, Actor{actor, Session}, target, false)
			}(i, pair[0], pair[1])
		}
		close(start)
		wg.Wait()
		var admins int
		if err := f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM aveloxis_ops.users WHERE admin AND user_id IN ($1, $2)`, a, b).Scan(&admins); err != nil {
			t.Fatal(err)
		}
		// The actor's privilege is read before the store's transaction takes
		// the role lock, so the demotion that runs second is refused either
		// inside the lock, as the last administrator's (ErrLastAdmin), or
		// before it, as no longer an administrator (ErrNotAdmin) when the
		// first demotion committed first — CI's -race timing produces both
		// (integration run 41140962779 on 0.29.124). Either is the one
		// refusal the invariant below requires.
		refused := 0
		for _, e := range errs {
			if errors.Is(e, db.ErrLastAdmin) || errors.Is(e, ErrNotAdmin) {
				refused++
			} else if e != nil {
				t.Fatalf("round %d: unexpected error %v", round, e)
			}
		}
		if admins != 1 || refused != 1 {
			t.Fatalf("round %d: %d administrators remain, %d refusals (%v); want exactly one of each", round, admins, refused, errs)
		}
	}
	// The last round demoted one of the two; a acts as an administrator again.
	if _, err := f.pool.Exec(f.ctx, `UPDATE aveloxis_ops.users SET admin = TRUE WHERE user_id = $1`, a); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.SetUserAdmin(f.ctx, Actor{a, Session}, 1<<30, true); !errors.Is(err, db.ErrUserNotFound) {
		t.Errorf("unknown user = %v; want ErrUserNotFound", err)
	}
	if err := f.svc.SetUserAdmin(f.ctx, Actor{a, Session}, a, false); !errors.Is(err, ErrSelfDemotion) {
		t.Errorf("self-demotion = %v; want ErrSelfDemotion", err)
	}
	if h := f.hooks.got(); len(h) == 0 || !strings.HasPrefix(h[0], "role ") {
		t.Errorf("hooks = %v; want RoleChanged for the demoted account", h)
	}
}

// TestGetAddRequestReadsOneRow (AVELOXIS_TEST_DB): the single-row read
// behind the submission notice carries the same columns and sample as the
// queue listing, and answers ErrAddRequestNotFound for no such request.
func TestGetAddRequestReadsOneRow(t *testing.T) {
	f := openFixture(t)
	owner := f.user(t, false)
	gid := f.group(t, owner)
	out, err := f.svc.AddToGroup(f.ctx, Actor{owner, Session}, gid, []string{
		fmt.Sprintf("github.com/_avadmin/%d-a", time.Now().UnixNano()),
		fmt.Sprintf("https://github.com/_avadmin/%d-b", time.Now().UnixNano()),
		"not a url",
	})
	if err != nil || out.Pending != 2 || out.RequestID == 0 || len(out.Invalid) != 1 {
		t.Fatalf("AddToGroup = %+v, %v; want 2 pending, 1 invalid", out, err)
	}
	t.Cleanup(func() {
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1`, out.RequestID)
		_, _ = f.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, out.RequestID)
	})
	req, err := f.store.GetAddRequest(f.ctx, out.RequestID)
	if err != nil || req.RequestID != out.RequestID || req.GroupID != gid || req.UserID != owner || req.Kind != "repos" || req.ItemCount != 2 || len(req.SampleURLs) != 2 {
		t.Fatalf("GetAddRequest = %+v, %v", req, err)
	}
	pending, err := f.store.ListPendingAddRequests(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range pending {
		if p.RequestID == req.RequestID {
			found = true
			if p.ItemCount != req.ItemCount || len(p.SampleURLs) != len(req.SampleURLs) || p.SampleURLs[0] != req.SampleURLs[0] {
				t.Errorf("listing row %+v differs from the single read %+v", p, req)
			}
		}
	}
	if !found {
		t.Error("the new request is not in the pending listing")
	}
	if _, err := f.store.GetAddRequest(f.ctx, 1<<40); !errors.Is(err, db.ErrAddRequestNotFound) {
		t.Errorf("unknown request = %v; want ErrAddRequestNotFound", err)
	}
	if h := f.hooks.got(); len(h) != 0 {
		t.Errorf("a pending paste linked nothing; hooks = %v", h)
	}
}
