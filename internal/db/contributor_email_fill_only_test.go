// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestAnObservedAddressNeverReplacesAContributorsStoredOne: cntrb_email is
// fill-only (operator, 2026-11-21; SR-11; cntrb_canonical is not: the batch
// takes an incoming canonical, 0.29.218, see
// TestTheProfilesAddressReplacesACommitAddressAsTheCanonical): which of a
// person's addresses the row holds is the one first observed, and a later
// one is never written over it (nor kept elsewhere by these paths: the
// alias step 0.29.216 added for it was taken out in 0.29.217). Before 0.29.216 three
// conflict arms preferred the INCOMING address (the EXCLUDED value first in
// the COALESCE, the stored one second): the two of ContributorResolver.Resolve and the one
// of the batch upsert. Each is driven here, with a neighbour row that must
// not change, and an empty stored address that must still be filled.
func TestAnObservedAddressNeverReplacesAContributorsStoredOne(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const (
		uid       = int64(99887711)
		raceLogin = "_av_feo_race_old"
		newLogin  = "_av_feo_race_new"
		delLogin  = "_av_feo_deleted"
		batLogin  = "_av_feo_batch"
		emptyLog  = "_av_feo_empty"
		neighbour = "_av_feo_neighbour"
		delID     = "0a000000-0000-4000-8000-00000000f011"
		batID     = "0a000000-0000-4000-8000-00000000f012"
		emptyID   = "0a000000-0000-4000-8000-00000000f013"
		nbrID     = "0a000000-0000-4000-8000-00000000f014"
		stored    = "_av_feo_first@example.org"
		incoming  = "_av_feo_later@example.org"
	)
	raceID := PlatformUUID(1, uid).String()
	ids := []string{raceID, delID, batID, emptyID, nbrID}
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = ANY($1::uuid[])`, ids)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = ANY($1::uuid[])`, ids)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id = ANY($1::uuid[])`, ids)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]) OR cntrb_login = ANY($2)`,
			ids, []string{raceLogin, newLogin, delLogin, batLogin, emptyLog, neighbour})
	}
	clean()
	t.Cleanup(clean)
	seed := func(id, login, email string, deleted int) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email, cntrb_canonical, cntrb_deleted)
			VALUES ($1::uuid, $2, $3, $3, $4)`, id, login, email, deleted); err != nil {
			t.Fatal(err)
		}
	}
	seed(raceID, raceLogin, stored, 0) // found by no lookup of Resolve: the insert conflicts on cntrb_id
	seed(delID, delLogin, stored, 1)   // a merge's loser: not found by login, the insert conflicts on cntrb_login
	seed(batID, batLogin, stored, 0)
	seed(emptyID, emptyLog, "", 0)
	seed(nbrID, neighbour, stored, 0)
	row := func(id string) (email, canonical string) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, `SELECT COALESCE(cntrb_email, ''), COALESCE(cntrb_canonical, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&email, &canonical); err != nil {
			t.Fatal(err)
		}
		return
	}

	if id, err := NewContributorResolver(store).Resolve(ctx, 1, uid, newLogin, "", incoming, "", "", "", ""); err != nil || id != raceID {
		t.Fatalf("Resolve onto the deterministic id = (%q, %v)", id, err)
	}
	if email, _ := row(raceID); email != stored {
		t.Errorf("Resolve, the conflict on cntrb_id: cntrb_email = %q, want the stored %q", email, stored)
	}
	if id, err := NewContributorResolver(store).Resolve(ctx, 1, 0, delLogin, "", incoming, "", "", "", ""); err != nil || id != delID {
		t.Fatalf("Resolve onto the login's row = (%q, %v)", id, err)
	}
	if email, _ := row(delID); email != stored {
		t.Errorf("Resolve, the conflict on cntrb_login: cntrb_email = %q, want the stored %q", email, stored)
	}
	batch := func(login string) {
		t.Helper()
		if err := store.UpsertContributorBatch(ctx, []model.Contributor{{Login: login, Email: incoming, Canonical: incoming}}); err != nil {
			t.Fatalf("UpsertContributorBatch(%s): %v", login, err)
		}
	}
	batch(batLogin)
	if email, canonical := row(batID); email != stored || canonical != incoming {
		t.Errorf("the batch upsert: (cntrb_email, cntrb_canonical) = (%q, %q), want the stored email %q and the incoming canonical %q", email, canonical, stored, incoming)
	}
	// An empty one is filled.
	batch(emptyLog)
	if email, canonical := row(emptyID); email != incoming || canonical != incoming {
		t.Errorf("the batch upsert of a row with no address: (%q, %q), want %q filled in both", email, canonical, incoming)
	}
	if email, canonical := row(nbrID); email != stored || canonical != stored {
		t.Errorf("the neighbour changed: (%q, %q)", email, canonical)
	}
}
