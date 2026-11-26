// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// An address that comes for a contributor changes nothing that says whose
// address it is (0.29.217, review round 10, C10-1; SR-6): no alias row is
// written for it, and the matched row's cntrb_email is not replaced.
//
// GitLab's contributor listing gives a git author name and an address and
// no user id, and the name is stored as the login. Two people whose git
// user.name is the same string, or a name that equals another forge's
// login, match one row by that name. 0.29.216 made each one's address an
// alias of that row; that step is gone (these paths write what 0.29.215
// wrote, with the address stored first), and this is the reviewer's probe
// kept as its test.
func TestAnAddressObservedForAContributorWhoHasOneIsAttributedToNobody(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	const (
		login     = "_av_no_alex"
		nbrLogin  = "_av_no_neighbour"
		uid       = int64(99887761)
		nbrUID    = int64(99887762)
		own       = "_av_no_own@example.org"
		nbrOwn    = "_av_no_nbr@example.org"
		strangerA = "_av_no_stranger@corp-a.example"
		strangerB = "_av_no_stranger@corp-b.example"
		later     = "_av_no_later@example.org"
	)
	id, nbrID := PlatformUUID(1, uid).String(), PlatformUUID(1, nbrUID).String()
	ids := []string{id, nbrID}
	addresses := []string{own, nbrOwn, strangerA, strangerB, later}
	clean := func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id = ANY($1::uuid[]) OR alias_email = ANY($2)`, ids, addresses)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]) OR cntrb_login = ANY($2)`, ids, []string{login, nbrLogin})
	}
	clean()
	t.Cleanup(clean)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, cntrb_email, cntrb_canonical)
		VALUES ($1::uuid, $2, $2, $3, $3), ($4::uuid, $5, $5, $6, $6)`, id, login, own, nbrID, nbrLogin, nbrOwn)
	batch := func(c model.Contributor) {
		t.Helper()
		if err := store.UpsertContributorBatch(ctx, []model.Contributor{c}); err != nil {
			t.Fatalf("UpsertContributorBatch(%s): %v", c.Login, err)
		}
	}
	byID := func(l string, u int64, email string) model.Contributor {
		return model.Contributor{Login: l, Email: email,
			Identities: []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: u, Login: l}}}
	}
	batch(byID(login, uid, own))
	batch(byID(nbrLogin, nbrUID, nbrOwn))

	state := func() string {
		t.Helper()
		var s string
		if err := store.pool.QueryRow(ctx, `
			SELECT (SELECT string_agg(cntrb_login || '=' || COALESCE(cntrb_email, '') || '/' || COALESCE(cntrb_canonical, ''), ' ' ORDER BY cntrb_login)
			        FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]))
			    || ' aliases=' || (SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE cntrb_id = ANY($1::uuid[]) OR alias_email = ANY($2))`,
			ids, addresses).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	want := login + "=" + own + "/" + own + " " + nbrLogin + "=" + nbrOwn + "/" + nbrOwn + " aliases=0"
	if got := state(); got != want {
		t.Fatalf("before any stranger: %s, want %s", got, want)
	}

	// Two other people whose git author name is that login: the listing
	// that carries no identity, through the batch and through the resolver.
	// The listing's own shape: a name and an address, no Canonical (only
	// the profile enrichment sends one, and it names its contributor by
	// the forge's id; a canonical that did come would replace the stored
	// one, as in every release to 0.29.215: 0.29.218, C11-1).
	batch(model.Contributor{Login: login, Email: strangerA, FullName: login})
	if got, err := NewContributorResolver(store).Resolve(ctx, 2, 0, login, login, strangerB, "", "", "", ""); err != nil || got != id {
		t.Fatalf("Resolve by the name alone = (%q, %v), want the row of that login", got, err)
	}
	if got := state(); got != want {
		t.Errorf("after two strangers' addresses came under the name alone:\n got  %s\n want %s", got, want)
	}
	for _, addr := range []string{strangerA, strangerB} {
		if l, err := store.FindLoginByEmail(ctx, addr); err != nil || l != "" {
			t.Errorf("FindLoginByEmail(%s) = (%q, %v), want no login: the address is nobody's here", addr, l, err)
		}
	}
	// The same contributor named by its forge id, with another address:
	// the stored one stands here too, and nothing else is written.
	batch(byID(login, uid, later))
	if _, err := NewContributorResolver(store).Resolve(ctx, 1, uid, login, "", later, "", "", "", ""); err != nil {
		t.Fatal(err)
	}
	if got := state(); got != want {
		t.Errorf("after another address for the contributor named by its forge id:\n got  %s\n want %s", got, want)
	}
}

// The two writers that take a GitHub user id beside UpsertContributorFull
// refuse one that is not positive (0.29.217, S10-3): bound as it came, 0
// would be stored as gh_user_id over a NULL, which fill-only then keeps,
// and would make the identity row (1, 0) every login-only reference would
// collapse onto.
func TestTheWritersOfAGitHubUserIDRefuseOneThatIsNotPositive(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	const (
		id    = "0a000000-0000-4000-8000-00000000a201"
		nbrID = "0a000000-0000-4000-8000-00000000a202"
		login = "_av_nz_login"
		nbr   = "_av_nz_neighbour"
	)
	ids := []string{id, nbrID}
	clean := func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]) OR cntrb_login = ANY($2)`, ids, []string{login, nbr, login + "_new"})
	}
	clean()
	t.Cleanup(clean)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login) VALUES ($1::uuid, $2, $2), ($3::uuid, $4, $4)`,
		id, login, nbrID, nbr)
	state := func() string {
		t.Helper()
		var s string
		if err := store.pool.QueryRow(ctx, `
			SELECT string_agg(cntrb_login || '/' || COALESCE(gh_login, '') || '/' || COALESCE(gh_user_id::text, 'NULL'), ' ' ORDER BY cntrb_login)
			       || ' identities=' || (SELECT count(*) FROM aveloxis_data.contributor_identities WHERE cntrb_id = ANY($1::uuid[]))
			       || ' history=' || (SELECT count(*) FROM aveloxis_data.contributor_login_history WHERE cntrb_id = ANY($1::uuid[]))
			FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[])`, ids).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := state()
	for _, bad := range []int64{0, -1} {
		if err := store.LinkContributorToGitHubUser(ctx, id, login, bad); err == nil {
			t.Errorf("LinkContributorToGitHubUser with user id %d returned no error", bad)
		}
		if err := store.RenameContributorGhLogin(ctx, id, login+"_new", bad); err == nil {
			t.Errorf("RenameContributorGhLogin with user id %d returned no error", bad)
		}
	}
	if after := state(); after != before {
		t.Errorf("a refused user id changed the rows:\n before %s\n after  %s", before, after)
	}
	// A positive id is taken, by both.
	const uid = int64(99887765)
	if err := store.LinkContributorToGitHubUser(ctx, id, login, uid); err != nil {
		t.Fatalf("LinkContributorToGitHubUser with a real id: %v", err)
	}
	if err := store.RenameContributorGhLogin(ctx, id, login+"_new", uid); err != nil {
		t.Fatalf("RenameContributorGhLogin with a real id: %v", err)
	}
	var got string
	if err := store.pool.QueryRow(ctx, `SELECT gh_login || '/' || gh_user_id::text FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if want := login + "_new/99887765"; got != want {
		t.Errorf("after a link and a rename with a real id: %q, want %q", got, want)
	}
}
