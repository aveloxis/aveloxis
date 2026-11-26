// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
)

// A platform user ID between 2^31 and 2^32-1 is inside the range both
// candidate queries admit (`<= 4294967295`), and each computed its target
// cntrb_id with `::bigint::int`, which PostgreSQL refuses for such a value
// ("integer out of range"). One such identity failed the whole query, and
// with it `migrate-cntrb-ids` and the collision merge, for every other
// contributor (found by the 0.29.214 statement-coverage work). The target
// must be what PlatformUUID computes.
func TestCntrbIDCandidatesAcceptAUserIDAboveTwoToTheThirtyOne(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const login, oldID = "_av_wide_uid", "5a5a5a5a-1111-4222-8333-444455556666"
	const userID = int64(3000000000)
	want := PlatformUUID(1, userID).String()
	drop := func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN ($1::uuid, $2::uuid)`, oldID, want)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id IN ($1::uuid, $2::uuid)`, oldID, want)
	}
	drop()
	t.Cleanup(drop)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login) VALUES ($1::uuid, $2)`, oldID, login)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.contributor_identities
			(cntrb_id, platform_id, platform_user_id, login, name, email, avatar_url, profile_url, node_id, user_type, is_admin)
		VALUES ($1::uuid, 1, $2, $3, '', '', '', '', '', '', FALSE)`, oldID, userID, login)

	counts, err := CountCntrbIDMigrationCandidates(ctx, store)
	if err != nil {
		t.Fatalf("counting migration candidates with a user ID of %d: %v", userID, err)
	}
	if counts.Total < 1 || counts.Safe < 1 {
		t.Errorf("counts = %+v; the seeded contributor is a safe candidate", counts)
	}
	var got string
	if err := store.pool.QueryRow(ctx, candidateSetSQL+` SELECT new_id::text FROM candidates WHERE old_id = $1::uuid`, oldID).Scan(&got); err != nil {
		t.Fatalf("reading the candidate's target: %v", err)
	}
	if got != want {
		t.Errorf("the target cntrb_id is %s, PlatformUUID gives %s", got, want)
	}

	// The collision query computes the same target: give it a row that
	// already holds it, and it must name the pair.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login) VALUES ($1::uuid, $2)`, want, login+"_holder")
	n, err := CountCntrbIDCollisions(ctx, store)
	if err != nil {
		t.Fatalf("counting collisions with a user ID of %d: %v", userID, err)
	}
	pairs, err := SampleCntrbIDCollisions(ctx, store, 1000)
	if err != nil {
		t.Fatalf("sampling collisions: %v", err)
	}
	found := false
	for _, p := range pairs {
		if p.LoserLogin == login {
			found = true
		}
	}
	if n < 1 || !found {
		t.Errorf("collisions = %d, the seeded pair found = %v", n, found)
	}
}
