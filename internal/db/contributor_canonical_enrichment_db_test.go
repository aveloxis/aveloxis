// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The profile's address replaces a commit address as cntrb_canonical
// (0.29.218, review round 11, C11-1).
//
// A contributor's first resolved commit writes the commit's author address
// as cntrb_canonical (UpsertContributorFull; no filter, so often a
// users.noreply.github.com address). The profile enrichment later brings
// the forge's public address through the batch upsert as Canonical, and it
// is the one writer that leaves noreply addresses out. In 0.29.215 and in
// the released 0.29.146 the batch took the incoming canonical; 0.29.216
// made that expression stored-first with cntrb_email's, so the commit
// address stayed for good and the affiliation lookups, which go by the
// canonical's domain, lost the organisation. The operator's rule was about
// cntrb_email only. An empty incoming canonical never clears a stored one,
// and cntrb_email is not touched by any of it.
func TestTheProfilesAddressReplacesACommitAddressAsTheCanonical(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	const (
		login     = "_av_canon_person"
		nbrLogin  = "_av_canon_neighbour"
		uid       = int64(99887781)
		nbrUID    = int64(99887782)
		noreply   = "99887781+_av_canon_person@users.noreply.github.com"
		profile   = "_av_canon_person@corp.example"
		nbrStored = "_av_canon_neighbour@other.example"
	)
	id, nbrID := GithubUUID(uid).String(), GithubUUID(nbrUID).String()
	ids := []string{id, nbrID}
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = ANY($1::uuid[])`, ids)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = ANY($1::uuid[])`, ids)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id = ANY($1::uuid[])`, ids)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]) OR cntrb_login = ANY($2)`, ids, []string{login, nbrLogin})
	}
	clean()
	t.Cleanup(clean)
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, cntrb_email, cntrb_canonical)
		VALUES ($1::uuid, $2, $2, $3, $3)`, nbrID, nbrLogin, nbrStored); err != nil {
		t.Fatal(err)
	}
	row := func(rid string) (email, canonical string) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, `SELECT COALESCE(cntrb_email, ''), COALESCE(cntrb_canonical, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, rid).Scan(&email, &canonical); err != nil {
			t.Fatal(err)
		}
		return
	}

	// The commit resolver's write: the row is made with the commit address.
	if created, got, err := store.UpsertContributorFull(ctx, id, login, uid, noreply); err != nil || !created || got != id {
		t.Fatalf("UpsertContributorFull = (%v, %q, %v), want the row created", created, got, err)
	}
	if _, canonical := row(id); canonical != noreply {
		t.Fatalf("after the commit resolver's write cntrb_canonical = %q, want the commit address %q (the test's premise)", canonical, noreply)
	}
	enrich := func(c model.Contributor) {
		t.Helper()
		c.Login = login
		c.Identities = []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: uid, Login: login}}
		if err := store.UpsertContributorBatch(ctx, []model.Contributor{c}); err != nil {
			t.Fatalf("UpsertContributorBatch: %v", err)
		}
	}
	// The enrichment with nothing to say (a profile with no public
	// address) clears nothing.
	enrich(model.Contributor{})
	if _, canonical := row(id); canonical != noreply {
		t.Errorf("an enrichment with no address left cntrb_canonical = %q, want the stored %q: an empty value must never clear it", canonical, noreply)
	}
	// The enrichment with the profile's public address.
	enrich(model.Contributor{Email: profile, Canonical: profile})
	email, canonical := row(id)
	if canonical != profile {
		t.Errorf("after the profile enrichment cntrb_canonical = %q, want the profile's %q: the commit address must not block it", canonical, profile)
	}
	if email != profile {
		// The row had no cntrb_email (UpsertContributorFull writes none),
		// so the first one observed fills it.
		t.Errorf("after the profile enrichment cntrb_email = %q, want %q filled into the empty column", email, profile)
	}
	// A later enrichment with no address still clears nothing.
	enrich(model.Contributor{})
	if e, c := row(id); e != profile || c != profile {
		t.Errorf("a later enrichment with no address left (%q, %q), want %q in both", e, c, profile)
	}
	// The other order (review round 12, C12-3): once the profile's address
	// is the canonical, no later commit address takes its place. The commit
	// resolver reaches the row two ways, and each fills only an empty
	// canonical: UpsertContributorFull (here its row-found-by-id arm) and
	// SetContributorCanonical. Until this round only the static policy
	// check held these two; a writer made incoming-first passed every
	// behaviour test.
	const laterCommit = "_av_canon_person@later-commit.example"
	if created, got, err := store.UpsertContributorFull(ctx, id, login, uid, laterCommit); err != nil || created || got != id {
		t.Fatalf("UpsertContributorFull on the stored row = (%v, %q, %v), want it found by its id", created, got, err)
	}
	if e, c := row(id); e != profile || c != profile {
		t.Errorf("after a later commit through UpsertContributorFull the row reads (%q, %q), want the profile's %q in both: a commit address must not replace a profile-set canonical", e, c, profile)
	}
	if err := store.SetContributorCanonical(ctx, id, laterCommit); err != nil {
		t.Fatal(err)
	}
	if e, c := row(id); e != profile || c != profile {
		t.Errorf("after SetContributorCanonical with a later commit address the row reads (%q, %q), want the profile's %q in both", e, c, profile)
	}
	// The same through the batch's rename recovery (the forge user under a
	// new login): its update fills an empty address and an empty canonical
	// and replaces neither. That includes a canonical the batch itself
	// brings: on this one arm the stored canonical stays, as it did in
	// every release (the arm is the released text).
	const renamed = "_av_canon_person_renamed"
	t.Cleanup(func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, renamed)
	})
	if err := store.UpsertContributorBatch(ctx, []model.Contributor{{
		Login: renamed, Email: laterCommit, Canonical: laterCommit,
		Identities: []model.ContributorIdentity{{Platform: model.PlatformGitHub, UserID: uid, Login: renamed}},
	}}); err != nil {
		t.Fatalf("UpsertContributorBatch under the new login: %v", err)
	}
	if e, c := row(id); e != profile || c != profile {
		t.Errorf("after the rename recovery the row reads (%q, %q), want the stored %q in both", e, c, profile)
	}
	var ghLogin string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gh_login, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&ghLogin); err != nil {
		t.Fatal(err)
	}
	if ghLogin != renamed {
		t.Errorf("gh_login = %q after the batch under the new login, want %q: the rename recovery did not run, so the assertion above did not test it", ghLogin, renamed)
	}
	if e, c := row(nbrID); e != nbrStored || c != nbrStored {
		t.Errorf("the neighbour changed: (%q, %q), want %q in both", e, c, nbrStored)
	}
}
