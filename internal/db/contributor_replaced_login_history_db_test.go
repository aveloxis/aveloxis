// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The operator's rule (2026-11-21): a contributor's login changes when a new
// one is observed, and the old one is in contributor_login_history. Before
// 0.29.216 three writers of a login recorded nothing, and no writer recorded
// the login it replaced, so the rule held only when some other path had
// happened to record the old login first. Every writer of
// contributors.gh_login and contributor_identities.login is driven here over
// rows that have NO history: afterwards the login written and the login
// replaced are both there, once, and a neighbour's history is as it was.

type loginHistoryFixture struct {
	t     *testing.T
	ctx   context.Context
	store *PostgresStore
}

// history returns "platform:login:source" of every history row of the
// contributor, in order.
func (f *loginHistoryFixture) history(id string) string {
	f.t.Helper()
	rows, err := f.store.pool.Query(f.ctx, `SELECT platform_id::text || ':' || login || ':' || source
		FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid ORDER BY platform_id, login`, id)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return strings.Join(out, " ")
}

func (f *loginHistoryFixture) seed(id, cntrbLogin, ghLogin string) {
	f.t.Helper()
	mustExecRetry(f.ctx, f.t, f.store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login)
		VALUES ($1::uuid, $2, $3)`, id, cntrbLogin, ghLogin)
}

func (f *loginHistoryFixture) identity(id string, platform int, uid int64, login string) {
	f.t.Helper()
	mustExecRetry(f.ctx, f.t, f.store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login)
		VALUES ($1::uuid, $2, $3, $4)`, id, platform, uid, login)
}

func (f *loginHistoryFixture) want(id, when, want string) {
	f.t.Helper()
	if got := f.history(id); got != want {
		f.t.Errorf("%s: history = %q, want %q", when, got, want)
	}
}

func TestEveryWriterOfALoginLeavesTheWrittenAndTheReplacedLoginInHistory(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	f := &loginHistoryFixture{t: t, ctx: ctx, store: store}

	const (
		p        = "_av_lh_"
		fullUID  = int64(99887731) // UpsertContributorFull, the row found by its id
		freshUID = int64(99887732) // UpsertContributorFull, a new row
		brUID    = int64(99887733) // RenameContributorGhLogin
		batUID   = int64(99887734) // the batch's rename recovery
		glUID    = int64(99887735) // the batch, a GitLab identity whose login changed
		resUID   = int64(99887736) // Resolve, a row found by its login
		newUID   = int64(99887737) // Resolve, a new row
		linkUID  = int64(99887738) // the search link
		nbrUID   = int64(99887739)
		byLogID  = "0a000000-0000-4000-8000-00000000b201" // UpsertContributorFull, the row found by its login
		glID     = "0a000000-0000-4000-8000-00000000b202"
		resID    = "0a000000-0000-4000-8000-00000000b203"
		linkID   = "0a000000-0000-4000-8000-00000000b204"
		nbrID    = "0a000000-0000-4000-8000-00000000b205"
	)
	fullID := PlatformUUID(1, fullUID).String()
	freshID := PlatformUUID(1, freshUID).String()
	brID := PlatformUUID(1, brUID).String()
	batID := PlatformUUID(1, batUID).String()
	newID := PlatformUUID(1, newUID).String()
	ids := []string{fullID, freshID, brID, batID, newID, byLogID, glID, resID, linkID, nbrID, PlatformUUID(1, 99887740).String()}
	clean := func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]) OR cntrb_login LIKE $2`, ids, `\_av\_lh\_%`)
	}
	clean()
	t.Cleanup(clean)

	// The neighbour: a login, an identity and one history row, none of which
	// any call below names.
	f.seed(nbrID, p+"nbr", p+"nbr_now")
	f.identity(nbrID, 1, nbrUID, p+"nbr_now")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_login_history (cntrb_id, platform_id, login, source)
		VALUES ($1::uuid, 1, $2, 'observation')`, nbrID, p+"nbr_was")
	neighbour := func(when string) {
		t.Helper()
		f.want(nbrID, when+": the neighbour", "1:"+p+"nbr_was:observation")
	}

	t.Run("UpsertContributorFull, the row found by its id", func(t *testing.T) {
		f.seed(fullID, p+"full_first", p+"full_old")
		for i := 0; i < 2; i++ { // twice: nothing is recorded twice
			if _, id, err := store.UpsertContributorFull(ctx, fullID, p+"full_new", fullUID, ""); err != nil || id != fullID {
				t.Fatalf("UpsertContributorFull = (%q, %v)", id, err)
			}
		}
		f.want(fullID, "renamed", "1:"+p+"full_new:observation 1:"+p+"full_old:backfill")
		neighbour("by id")
	})

	t.Run("UpsertContributorFull, the row found by its login", func(t *testing.T) {
		f.seed(byLogID, p+"bylogin", p+"bylogin_other")
		if _, id, err := store.UpsertContributorFull(ctx, PlatformUUID(1, 99887740).String(), p+"bylogin", 99887740, ""); err != nil || id != byLogID {
			t.Fatalf("UpsertContributorFull = (%q, %v)", id, err)
		}
		f.want(byLogID, "found by login", "1:"+p+"bylogin:observation 1:"+p+"bylogin_other:backfill")
		neighbour("by login")
	})

	t.Run("UpsertContributorFull, a new row", func(t *testing.T) {
		if created, id, err := store.UpsertContributorFull(ctx, freshID, p+"fresh", freshUID, ""); err != nil || id != freshID || !created {
			t.Fatalf("UpsertContributorFull = (%v, %q, %v)", created, id, err)
		}
		f.want(freshID, "created", "1:"+p+"fresh:observation")
		neighbour("a new row")
	})

	t.Run("RenameContributorGhLogin", func(t *testing.T) {
		f.seed(brID, p+"br_first", p+"br_old")
		f.identity(brID, 1, brUID, p+"br_identity_old")
		for i := 0; i < 2; i++ {
			if err := store.RenameContributorGhLogin(ctx, brID, p+"br_new", brUID); err != nil {
				t.Fatal(err)
			}
		}
		f.want(brID, "renamed", "1:"+p+"br_identity_old:backfill 1:"+p+"br_new:rename_breadth 1:"+p+"br_old:backfill")
		neighbour("the breadth rename")
	})

	t.Run("the batch upsert's rename recovery", func(t *testing.T) {
		f.seed(batID, p+"bat_old", p+"bat_old")
		f.identity(batID, 1, batUID, p+"bat_old")
		for i := 0; i < 2; i++ {
			if err := store.UpsertContributorBatch(ctx, []model.Contributor{
				{Login: p + "bat_new", Identities: []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: batUID, Login: p + "bat_new"}}},
				{Login: p + "nbr", Identities: []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: nbrUID, Login: p + "nbr_now"}}},
			}); err != nil {
				t.Fatal(err)
			}
		}
		f.want(batID, "renamed", "1:"+p+"bat_new:rename_recovery 1:"+p+"bat_old:backfill")
		// The neighbour was in the batch under the login it has: its
		// observation is recorded, and nothing as replaced.
		f.want(nbrID, "the neighbour in the batch", "1:"+p+"nbr_now:observation 1:"+p+"nbr_was:observation")
		mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid AND login = $2`, nbrID, p+"nbr_now")
	})

	t.Run("the batch upsert, another platform's identity under a new login", func(t *testing.T) {
		f.seed(glID, p+"gl", "")
		f.identity(glID, 2, glUID, p+"gl_old")
		if err := store.UpsertContributorBatch(ctx, []model.Contributor{
			{Login: p + "gl", Identities: []model.ContributorIdentity{{Platform: model.PlatformGitLab, UserID: glUID, Login: p + "gl_new"}}},
		}); err != nil {
			t.Fatal(err)
		}
		f.want(glID, "identity renamed", "2:"+p+"gl_new:observation 2:"+p+"gl_old:backfill")
		neighbour("the GitLab identity")
	})

	t.Run("the resolver, a row found by its login", func(t *testing.T) {
		f.seed(resID, p+"res", "")
		if id, err := NewContributorResolver(store).Resolve(ctx, 1, resUID, p+"res", "", "", "", "", "", ""); err != nil || id != resID {
			t.Fatalf("Resolve = (%q, %v)", id, err)
		}
		f.want(resID, "identity written", "1:"+p+"res:observation")
		neighbour("the resolver by login")
	})

	t.Run("the resolver, a new row", func(t *testing.T) {
		if id, err := NewContributorResolver(store).Resolve(ctx, 1, newUID, p+"new", "", "", "", "", "", ""); err != nil || id != newID {
			t.Fatalf("Resolve = (%q, %v)", id, err)
		}
		f.want(newID, "created", "1:"+p+"new:observation")
		neighbour("the resolver's insert")
	})

	t.Run("the search link", func(t *testing.T) {
		f.seed(linkID, "", "")
		if err := store.LinkContributorToGitHubUser(ctx, linkID, p+"linked", linkUID); err != nil {
			t.Fatal(err)
		}
		f.want(linkID, "linked", "1:"+p+"linked:observation")
		neighbour("the search link")
	})
}

// A platform user id of 0 names no identity (v0.27.108): the statement that
// keeps replaced logins never takes one from whatever row carries it.
func TestReplacedLoginsAreNotTakenFromTheIdentityOfNoUser(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	const id = "0a000000-0000-4000-8000-00000000b211"
	clean := func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	}
	clean()
	t.Cleanup(clean)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login) VALUES ($1::uuid, '_av_lh0_a', '_av_lh0_a')`, id)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login)
		VALUES ($1::uuid, 1, 0, '_av_lh0_poisoned') ON CONFLICT (platform_id, platform_user_id) DO NOTHING`, id)
	var r replacedLogins
	r.add("", 1, 0, "_av_lh0_incoming")
	r.add(id, 1, 0, "_av_lh0_a") // the contributor's own gh_login: nothing is replaced
	r.add(id, 1, 0, "")          // no login: not sent
	if len(r.logins) != 2 {
		t.Fatalf("entries = %d, want 2 (an empty login is not one)", len(r.logins))
	}
	if err := recordReplacedLogins(ctx, store.pool, &r); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributor_login_history
		WHERE cntrb_id = $1::uuid OR login = '_av_lh0_poisoned'`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("history rows = %d, want 0", n)
	}
}
