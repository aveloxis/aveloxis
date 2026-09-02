// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
)

// TestLinkJiraIdentityReturnsPersistedWinner (Copilot round 11 on
// PR #193): linkJiraIdentity's fill-empty guard means a concurrent
// resolver (live drain vs identity backfill) can link first — the
// pre-fix void return then let the LOSER attribute its issue/comment
// to its locally selected contributor while the table held the
// winner's, splitting attribution for one jira_name. The link must
// return the PERSISTED (cntrb_id, match_method) pair in both the
// won and the lost race, exactly as MintJiraContributor's conflict
// path already does.
func TestLinkJiraIdentityReturnsPersistedWinner(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	cleanup := func() {
		c := context.Background()
		store.pool.Exec(c, `DELETE FROM aveloxis_data.jira_identities WHERE jira_name LIKE '_avjr11-%'`)
		store.pool.Exec(c, `DELETE FROM aveloxis_data.contributors WHERE cntrb_full_name LIKE '_avjr11 %'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	var xID, yID string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_full_name)
		VALUES ('', '_avjr11 X') RETURNING cntrb_id::text`).Scan(&xID); err != nil {
		t.Fatalf("seed X: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_full_name)
		VALUES ('', '_avjr11 Y') RETURNING cntrb_id::text`).Scan(&yID); err != nil {
		t.Fatalf("seed Y: %v", err)
	}

	// Bank the raw identity unlinked (matches nothing → row created,
	// cntrb_id stays NULL) — the state both racers observe.
	name := "_avjr11-link"
	if cntrb, _, ambiguous, err := store.ResolveJiraIdentity(ctx, name, "", ""); err != nil || cntrb != "" || ambiguous {
		t.Fatalf("bank unlinked: cntrb=%q ambiguous=%v err=%v", cntrb, ambiguous, err)
	}

	// Winner links first.
	w, wm, err := store.linkJiraIdentity(ctx, name, xID, "login")
	if err != nil || w != xID || wm != "login" {
		t.Fatalf("winner link: got (%q,%q,%v), want (%q,\"login\",nil)", w, wm, err, xID)
	}

	// The LOSER of the race: its candidate is Y, but the persisted
	// identity is X — it must return X/login, and the table must
	// still hold X.
	l, lm, err := store.linkJiraIdentity(ctx, name, yID, "display")
	if err != nil {
		t.Fatalf("loser link: %v", err)
	}
	if l != xID || lm != "login" {
		t.Fatalf("loser returned (%q,%q), want the PERSISTED winner (%q,\"login\") — a locally selected candidate here splits attribution for the jira_name", l, lm, xID)
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `
		SELECT cntrb_id::text FROM aveloxis_data.jira_identities WHERE jira_name = $1`,
		name).Scan(&stored); err != nil || stored != xID {
		t.Fatalf("persisted cntrb = %q err=%v, want %q untouched", stored, err, xID)
	}
}
