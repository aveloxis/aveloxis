// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestStaleAliasDoesNotStrandSender (Copilot round 29, PR #193, inline):
// the sender-resolve candidate anti-join excluded a sender whenever ANY
// alias row existed — even one pointing at a soft-deleted merge loser
// (reachable via the merge path, which never repoints the loser's own
// alias rows). The resolver's alias arm requires an ACTIVE owner, so such
// an alias resolves nothing while its existence removed the sender from
// the retry pool forever. Three contracts:
//  1. a stale alias does NOT exclude the sender from the candidate pool;
//  2. CreateEmailOnlyContributor REASSIGNS the stale alias to the new
//     active contributor (returning the active id, never the dead one);
//  3. an ACTIVE owner's alias is never stolen.
func TestStaleAliasDoesNotStrandSender(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round29-stale-alias")
	tag := time.Now().UnixNano()
	email := fmt.Sprintf("avr29-stale+%d@example.org", tag)

	// One human email_message row so the sender qualifies as a candidate.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message
		(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class)
		VALUES ($1, 6, $2, $3, 'a human thread', NOW(), 'discuss')`,
		repoID, fmt.Sprintf("<avr29-%d@x>", tag), email)

	// A soft-deleted merge loser owning the sender's alias.
	var deadID string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, gh_login, cntrb_email, cntrb_canonical, cntrb_deleted, tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', '', '', '', 1, 'test', 'Mailing List', NOW())
		RETURNING cntrb_id::text`, &deadID)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors_aliases
		(cntrb_id, canonical_email, alias_email, cntrb_active, tool_source, data_source, data_collection_date)
		VALUES ($1::uuid, $2, $2, 1, 'test', 'Mailing List', NOW())`, deadID, email)

	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.email_message WHERE sender_email = $1`, email)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text = $1`, deadID)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_email = $1`, email)
	})

	inCandidates := func() bool {
		cands, err := store.GetMailingListSenderResolveCandidates(ctx, 1, 3600, 1000000)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range cands {
			if c.SenderEmail == email {
				return true
			}
		}
		return false
	}

	// 1. The stale alias must NOT exclude the sender.
	if !inCandidates() {
		t.Fatal("a sender whose only alias points at a SOFT-DELETED contributor must stay in the candidate pool — the resolver cannot use that alias")
	}

	// 2. Create repairs: new ACTIVE contributor, alias reassigned to it.
	gotID, err := store.CreateEmailOnlyContributor(ctx, email)
	if err != nil {
		t.Fatal(err)
	}
	if gotID == "" || gotID == deadID {
		t.Fatalf("CreateEmailOnlyContributor must return an ACTIVE contributor, got %q (dead=%s)", gotID, deadID)
	}
	var owner string
	var ownerDeleted int
	if err := store.pool.QueryRow(ctx, `
		SELECT a.cntrb_id::text, COALESCE(c.cntrb_deleted, 0)
		FROM aveloxis_data.contributors_aliases a
		JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb_id
		WHERE a.alias_email = $1`, email).Scan(&owner, &ownerDeleted); err != nil {
		t.Fatal(err)
	}
	if owner != gotID || ownerDeleted != 0 {
		t.Fatalf("the stale alias must be REASSIGNED to the new active contributor %s; owner=%s deleted=%d", gotID, owner, ownerDeleted)
	}

	// The sender now leaves the candidate pool (active-owned alias).
	if inCandidates() {
		t.Error("after repair the sender must be excluded — the alias now resolves")
	}

	// 3. An ACTIVE owner's alias is never stolen.
	activeEmail := fmt.Sprintf("avr29-active+%d@example.org", tag)
	var activeID string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, gh_login, cntrb_email, cntrb_canonical, tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', '', $1, $1, 'test', 'Mailing List', NOW())
		RETURNING cntrb_id::text`, &activeID, activeEmail)
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, activeEmail)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text = $1`, activeID)
	})
	if err := store.EnsureContributorAlias(ctx, activeID, activeEmail, "test", "Mailing List"); err != nil {
		t.Fatal(err)
	}
	got2, err := store.CreateEmailOnlyContributor(ctx, activeEmail)
	if err != nil {
		t.Fatal(err)
	}
	if got2 != activeID {
		t.Fatalf("an ACTIVE owner keeps their alias: want %s, got %s", activeID, got2)
	}
	var owner2 string
	if err := store.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, activeEmail).Scan(&owner2); err != nil {
		t.Fatal(err)
	}
	if owner2 != activeID {
		t.Fatalf("the ACTIVE owner's alias must never be reassigned: want %s, got %s", activeID, owner2)
	}

	// 3b. The DIRECT steal attempt: EnsureContributorAlias called with a
	// DIFFERENT contributor for an alias an ACTIVE owner holds must be a
	// no-op — the DO UPDATE's dead-owner guard is what prevents it (the
	// probe in CreateEmailOnlyContributor cannot cover this path).
	var rivalID string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, gh_login, tool_source, data_source, data_collection_date)
		VALUES (gen_random_uuid(), '', '', 'test', 'Mailing List', NOW())
		RETURNING cntrb_id::text`, &rivalID)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text = $1`, rivalID)
	})
	if err := store.EnsureContributorAlias(ctx, rivalID, activeEmail, "test", "Mailing List"); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, activeEmail).Scan(&owner2); err != nil {
		t.Fatal(err)
	}
	if owner2 != activeID {
		t.Fatalf("EnsureContributorAlias with a rival contributor STOLE an active owner's alias: want %s, got %s", activeID, owner2)
	}
}
