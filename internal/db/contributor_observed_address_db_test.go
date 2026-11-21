// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The operator's rule (2026-11-21): a contributor's stored address is never
// replaced by a newly observed one, and the newly observed one is kept as a
// contributors_aliases row (R5). 0.29.216 made the first half true and, until
// these paths wrote the alias, dropped the observed address: each path that
// sees an address for a contributor who has another one is driven here.

type observedAddressFixture struct {
	t     *testing.T
	ctx   context.Context
	store *PostgresStore
	logs  *bytes.Buffer
	ids   []string
}

func newObservedAddressFixture(t *testing.T, ids, logins, addresses []string) *observedAddressFixture {
	t.Helper()
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	f := &observedAddressFixture{t: t, ctx: ctx, store: store, logs: &bytes.Buffer{}, ids: ids}
	store.logger = slog.New(slog.NewTextHandler(&lockedWriter{w: f.logs}, nil))
	clean := func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = ANY($1::uuid[])`, ids)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id = ANY($1::uuid[]) OR alias_email = ANY($2)`, ids, addresses)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]) OR cntrb_login = ANY($2)`, ids, logins)
	}
	clean()
	t.Cleanup(clean)
	return f
}

func (f *observedAddressFixture) seed(id, login, email string) {
	f.t.Helper()
	mustExecRetry(f.ctx, f.t, f.store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, cntrb_email, cntrb_canonical)
		VALUES ($1::uuid, $2, $2, $3, $3)`, id, login, email)
}

func (f *observedAddressFixture) email(id string) string {
	f.t.Helper()
	var e string
	if err := f.store.pool.QueryRow(f.ctx, `SELECT COALESCE(cntrb_email, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&e); err != nil {
		f.t.Fatal(err)
	}
	return e
}

// aliases returns "alias_email>canonical_email" of every alias the
// contributor owns, in order.
func (f *observedAddressFixture) aliases(id string) string {
	f.t.Helper()
	rows, err := f.store.pool.Query(f.ctx, `SELECT alias_email || '>' || canonical_email FROM aveloxis_data.contributors_aliases
		WHERE cntrb_id = $1::uuid ORDER BY alias_email`, id)
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

func TestAnAddressObservedForAContributorWhoHasAnotherIsKeptAsAnAlias(t *testing.T) {
	const (
		batID    = "0a000000-0000-4000-8000-00000000a101"
		otherID  = "0a000000-0000-4000-8000-00000000a102"
		nbrID    = "0a000000-0000-4000-8000-00000000a103"
		emptyID  = "0a000000-0000-4000-8000-00000000a104"
		loginID  = "0a000000-0000-4000-8000-00000000a105"
		batLogin = "_av_oa_batch"
		othLogin = "_av_oa_other"
		nbrLogin = "_av_oa_neighbour"
		empLogin = "_av_oa_empty"
		logLogin = "_av_oa_bylogin"
		renOld   = "_av_oa_rename_old"
		renNew   = "_av_oa_rename_new"
		idLogin  = "_av_oa_byidentity"
		first    = "_av_oa_first@example.org"
		later    = "_av_oa_later@example.org"
		third    = "_av_oa_third@example.org"
		theirs   = "_av_oa_theirs@example.org"
		filled   = "_av_oa_filled@example.org"
		renUID   = int64(99887721)
		idUID    = int64(99887722)
	)
	renID := PlatformUUID(1, renUID).String()
	idID := PlatformUUID(1, idUID).String()
	f := newObservedAddressFixture(t,
		[]string{batID, otherID, nbrID, emptyID, loginID, renID, idID},
		[]string{batLogin, othLogin, nbrLogin, empLogin, logLogin, renOld, renNew, idLogin},
		[]string{first, later, third, theirs, filled})
	f.seed(batID, batLogin, first)
	f.seed(otherID, othLogin, "_av_oa_other_first@example.org")
	f.seed(nbrID, nbrLogin, first)
	f.seed(emptyID, empLogin, "")
	f.seed(loginID, logLogin, first)
	f.seed(renID, renOld, first)
	f.seed(idID, idLogin, first)
	// Another contributor already owns `theirs` as an alias.
	mustExecRetry(f.ctx, t, f.store, `INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email, cntrb_active)
		VALUES ($1::uuid, $2, $2, 1)`, otherID, theirs)
	mustExecRetry(f.ctx, t, f.store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login)
		VALUES ($1::uuid, 1, $2, $3)`, idID, idUID, idLogin)

	batch := func(c model.Contributor) {
		t.Helper()
		if err := f.store.UpsertContributorBatch(f.ctx, []model.Contributor{c}); err != nil {
			t.Fatalf("UpsertContributorBatch(%s): %v", c.Login, err)
		}
	}
	untouched := func(when string) {
		t.Helper()
		if got := f.email(nbrID); got != first {
			t.Errorf("%s: the neighbour's cntrb_email = %q", when, got)
		}
		if got := f.aliases(nbrID); got != "" {
			t.Errorf("%s: the neighbour has aliases %q, want none", when, got)
		}
		if got := f.aliases(otherID); got != theirs+">"+theirs {
			t.Errorf("%s: the other contributor's aliases = %q, want its one", when, got)
		}
	}

	t.Run("the batch upsert", func(t *testing.T) {
		batch(model.Contributor{Login: batLogin, Email: later})
		if got := f.email(batID); got != first {
			t.Errorf("cntrb_email = %q, want the stored %q", got, first)
		}
		if got, want := f.aliases(batID), later+">"+first; got != want {
			t.Errorf("aliases = %q, want %q (the alias, under the contributor's canonical address)", got, want)
		}
		// The same address again, and the stored one, add nothing.
		batch(model.Contributor{Login: batLogin, Email: later})
		batch(model.Contributor{Login: batLogin, Email: first})
		batch(model.Contributor{Login: batLogin, Email: strings.ToUpper(first)})
		batch(model.Contributor{Login: batLogin, Email: ""})
		if got, want := f.aliases(batID), later+">"+first; got != want {
			t.Errorf("after the same, the stored and an empty address: aliases = %q, want %q", got, want)
		}
		untouched("the batch upsert")
	})

	t.Run("an address that fills an empty one is not an alias", func(t *testing.T) {
		batch(model.Contributor{Login: empLogin, Email: filled})
		if got := f.email(emptyID); got != filled {
			t.Errorf("cntrb_email = %q, want %q filled", got, filled)
		}
		if got := f.aliases(emptyID); got != "" {
			t.Errorf("aliases = %q, want none: the address is the row's own", got)
		}
	})

	t.Run("another contributor's alias is left where it is, and said", func(t *testing.T) {
		f.logs.Reset()
		batch(model.Contributor{Login: batLogin, Email: theirs})
		if got, want := f.aliases(batID), later+">"+first; got != want {
			t.Errorf("aliases = %q, want %q unchanged", got, want)
		}
		untouched("an address another contributor owns")
		logged := f.logs.String()
		for _, want := range []string{"level=WARN", batID, otherID} {
			if !strings.Contains(logged, want) {
				t.Errorf("the log does not hold %q:\n%s", want, logged)
			}
		}
	})

	t.Run("the batch upsert's rename recovery", func(t *testing.T) {
		batch(model.Contributor{Login: renNew, Email: later + ".ren",
			Identities: []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: renUID, Login: renNew}}})
		t.Cleanup(func() {
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, later+".ren")
		})
		if got := f.email(renID); got != first {
			t.Errorf("cntrb_email = %q, want the stored %q", got, first)
		}
		if got, want := f.aliases(renID), later+".ren>"+first; got != want {
			t.Errorf("aliases = %q, want %q", got, want)
		}
		untouched("the rename recovery")
	})

	t.Run("the resolver, a row found by its login", func(t *testing.T) {
		id, err := NewContributorResolver(f.store).Resolve(f.ctx, 1, 0, logLogin, "", third, "", "", "", "")
		if err != nil || id != loginID {
			t.Fatalf("Resolve = (%q, %v)", id, err)
		}
		if got := f.email(loginID); got != first {
			t.Errorf("cntrb_email = %q, want the stored %q", got, first)
		}
		if got, want := f.aliases(loginID), third+">"+first; got != want {
			t.Errorf("aliases = %q, want %q", got, want)
		}
		// Again, from a resolver with a cold cache: nothing more.
		if _, err := NewContributorResolver(f.store).Resolve(f.ctx, 1, 0, logLogin, "", third, "", "", "", ""); err != nil {
			t.Fatal(err)
		}
		if _, err := NewContributorResolver(f.store).Resolve(f.ctx, 1, 0, logLogin, "", "", "", "", "", ""); err != nil {
			t.Fatal(err)
		}
		// The row's own address, in another case, is not its alias.
		if _, err := NewContributorResolver(f.store).Resolve(f.ctx, 1, 0, logLogin, "", strings.ToUpper(first), "", "", "", ""); err != nil {
			t.Fatal(err)
		}
		if got, want := f.aliases(loginID), third+">"+first; got != want {
			t.Errorf("after the same and an empty address: aliases = %q, want %q", got, want)
		}
		untouched("the resolver by login")
	})

	t.Run("the resolver, a row found by its identity", func(t *testing.T) {
		const idLater = "_av_oa_id_later@example.org"
		t.Cleanup(func() {
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, idLater)
		})
		id, err := NewContributorResolver(f.store).Resolve(f.ctx, 1, idUID, idLogin, "", idLater, "", "", "", "")
		if err != nil || id != idID {
			t.Fatalf("Resolve = (%q, %v)", id, err)
		}
		if got := f.email(idID); got != first {
			t.Errorf("cntrb_email = %q, want the stored %q", got, first)
		}
		if got, want := f.aliases(idID), idLater+">"+first; got != want {
			t.Errorf("aliases = %q, want %q", got, want)
		}
		untouched("the resolver by identity")
	})

	t.Run("a merge's deleted loser is given no alias", func(t *testing.T) {
		const (
			delID    = "0a000000-0000-4000-8000-00000000a106"
			delLogin = "_av_oa_deleted"
			delLater = "_av_oa_del_later@example.org"
		)
		clean := func() {
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, delLater)
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, delID)
		}
		clean()
		t.Cleanup(clean)
		f.seed(delID, delLogin, first)
		mustExecRetry(f.ctx, t, f.store, `UPDATE aveloxis_data.contributors SET cntrb_deleted = 1 WHERE cntrb_id = $1::uuid`, delID)
		// Not found by login (deleted rows are not), so the insert conflicts
		// on cntrb_login and lands on the deleted row; the batch reaches it
		// the same way.
		if id, err := NewContributorResolver(f.store).Resolve(f.ctx, 1, 0, delLogin, "", delLater, "", "", "", ""); err != nil || id != delID {
			t.Fatalf("Resolve = (%q, %v)", id, err)
		}
		batch(model.Contributor{Login: delLogin, Email: delLater})
		if got := f.aliases(delID); got != "" {
			t.Errorf("aliases = %q, want none: an alias owned by a deleted row resolves nothing", got)
		}
		var n int
		if err := f.store.pool.QueryRow(f.ctx, `SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, delLater).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("alias rows for the address = %d, want 0", n)
		}
		untouched("a deleted row")
	})

	t.Run("the resolver, a row its insert collides with", func(t *testing.T) {
		const (
			raceUID   = int64(99887723)
			raceLater = "_av_oa_race_later@example.org"
		)
		raceID := PlatformUUID(1, raceUID).String()
		clean := func() {
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, raceLater)
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid`, raceID)
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = $1::uuid`, raceID)
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, raceID)
		}
		clean()
		t.Cleanup(clean)
		// The row is there under another login and with no identity row:
		// no lookup of Resolve finds it, and the insert conflicts on cntrb_id.
		f.seed(raceID, "_av_oa_race_old", first)
		t.Cleanup(func() {
			cleanupExecRetry(context.Background(), f.store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = '_av_oa_race_old'`)
		})
		id, err := NewContributorResolver(f.store).Resolve(f.ctx, 1, raceUID, "_av_oa_race_new", "", raceLater, "", "", "", "")
		if err != nil || id != raceID {
			t.Fatalf("Resolve = (%q, %v)", id, err)
		}
		if got := f.email(raceID); got != first {
			t.Errorf("cntrb_email = %q, want the stored %q", got, first)
		}
		if got, want := f.aliases(raceID), raceLater+">"+first; got != want {
			t.Errorf("aliases = %q, want %q", got, want)
		}
		untouched("the resolver's conflict")
	})
}
