// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestSenderResolvePassCoolsDownANoIdentitySender drives the sender resolver
// at RUNTIME (review round 8 of items 16–19; the source pins on the loop were
// respelled around): a human sender at an ID-less
// login@users.noreply.github.com address whose login has no contributor row
// is COOLED DOWN (resolved=false, last_attempt_at set, no login recorded,
// nothing written) — through v0.29.67 it was stamped resolved, terminal,
// with the login and no alias, and counted linked. Once the login has a
// contributor row and the cooldown is over, the same pass links it: the
// terminal stamp with the login, and the alias row.
func TestSenderResolvePassCoolsDownANoIdentitySender(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	const login = "avnoidprobe"
	const email = login + "@users.noreply.github.com"
	pool := store.Pool()
	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_sender_resolve WHERE sender_email = $1`, email)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE sender_email = $1`, email)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE gh_login = $1 OR cntrb_login = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	for i := 0; i < mailingListSenderResolveMinMessages; i++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_data.email_message
				(message_id_header, sender_email, data_source, platform_id, ml_system)
			VALUES ($1, $2, 'dev@_avnoid.invalid', 6, 'apache_ponymail')
			ON CONFLICT (message_id_header) DO NOTHING`,
			email+"-"+time.Now().Format("150405.000000000")+"-"+strconv.Itoa(i), email); err != nil {
			t.Fatal(err)
		}
	}
	// No GitHub keys: the API tail is withheld; the noreply parse needs none.
	s := &Scheduler{store: store, logger: logger}
	if stop := s.senderResolvePass(ctx); stop {
		t.Fatal("the pass reported a stop on a live context")
	}
	var resolved bool
	var attempted *time.Time
	var resolvedLogin string
	row := func() {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT resolved, last_attempt_at, COALESCE(resolved_login, '') FROM aveloxis_ops.mailing_list_sender_resolve WHERE sender_email = $1`, email).Scan(&resolved, &attempted, &resolvedLogin); err != nil {
			t.Fatalf("the sender has no resolve row after the pass: %v", err)
		}
	}
	row()
	if resolved || attempted == nil || resolvedLogin != "" {
		t.Errorf("a login with no contributor row: resolved=%v last_attempt_at=%v login=%q; want the 30-day cooldown only (false, set, \"\")", resolved, attempted, resolvedLogin)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors WHERE gh_login = $1`, login).Scan(&n); err != nil || n != 0 {
		t.Errorf("a contributor row was written for the no-identity sender (n=%d, err=%v)", n, err)
	}
	// The login gains a row; the cooldown is over: the pass links it.
	if _, _, err := store.UpsertContributorFull(ctx, db.GithubUUID(424242424).String(), login, 424242424, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE aveloxis_ops.mailing_list_sender_resolve SET last_attempt_at = NULL WHERE sender_email = $1`, email); err != nil {
		t.Fatal(err)
	}
	if stop := s.senderResolvePass(ctx); stop {
		t.Fatal("the pass reported a stop on a live context")
	}
	row()
	if !resolved || resolvedLogin != login {
		t.Errorf("once the login has a row: resolved=%v login=%q; want the terminal stamp with the login", resolved, resolvedLogin)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email).Scan(&n); err != nil || n != 1 {
		t.Errorf("the link must leave one alias row for the sender (n=%d, err=%v)", n, err)
	}
}
