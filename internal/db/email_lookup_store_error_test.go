// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestEmailLookupsReturnAStoreError pins worklist item 17 at the store
// (review round 1 of the batch): FindLoginByEmail and
// ResolveContributorIDByEmail collapsed EVERY error into "not in the DB"
// ("", nil), so the callers' new error arms could never fire against the
// real store — a closed pool or a cancelled context still sent an email the
// store knew to the API, and on a no-hit to the email-only create. Only "no
// row" is "not in the DB"; a failed lookup is returned as itself.
func TestEmailLookupsReturnAStoreError(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	const unknown = "_avlookup_nobody@example.invalid"
	if login, err := store.FindLoginByEmail(ctx, unknown); err != nil || login != "" {
		t.Errorf("an unknown email = (%q, %v); want (\"\", nil)", login, err)
	}
	if id, ok, err := store.ResolveContributorIDByEmail(ctx, unknown); err != nil || ok || id != "" {
		t.Errorf("an unknown email = (%q, %v, %v); want (\"\", false, nil)", id, ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.FindLoginByEmail(canceled, unknown); err == nil {
		t.Error("FindLoginByEmail on a cancelled context = nil error; want the store's failure, not \"not in the DB\"")
	}
	if _, ok, err := store.ResolveContributorIDByEmail(canceled, unknown); err == nil || ok {
		t.Error("ResolveContributorIDByEmail on a cancelled context = no error; want the store's failure, not \"no contributor\"")
	}
	// The third sibling (review round 4).
	if id, err := store.FindContributorIDByLogin(ctx, "_avlookup_nobody"); err != nil || id != "" {
		t.Errorf("an unknown login = (%q, %v); want (\"\", nil)", id, err)
	}
	if _, err := store.FindContributorIDByLogin(canceled, "_avlookup_nobody"); err == nil {
		t.Error("FindContributorIDByLogin on a cancelled context = nil error; want the store's failure, not \"no contributor\"")
	}
	// The lookup's OTHER caller (review round 5): the login-only branch of
	// LinkMailingListSender collapsed the lookup's error into ("", nil), and
	// the sender-resolve wiring reads a nil error as "linked" — the item-18
	// stamp on a store failure, one layer down.
	if id, err := store.LinkMailingListSender(canceled, unknown, "_avlookup_nobody", 0); err == nil || id != "" {
		t.Errorf("LinkMailingListSender (login only) on a cancelled context = (%q, %v); want the lookup's failure, not \"no stable identity\"", id, err)
	}
	// Review round 7: the skip is TYPED — ("", nil) read as a link and the
	// wiring stamped the sender resolved (terminal) and counted it linked.
	if id, err := store.LinkMailingListSender(ctx, unknown, "_avlookup_nobody", 0); !errors.Is(err, ErrNoStableIdentity) || id != "" {
		t.Errorf("LinkMailingListSender (login only, unknown login) = (%q, %v); want ErrNoStableIdentity: nothing was written, so the caller must not record a link", id, err)
	}
}

// TestSenderResolveAuditSQLCounts makes the deploy checklist's audit step
// executable (review round 9 of items 16–19): a sender stamped resolved
// with a login and no active alias counts; one with an active alias does
// not; a soft-deleted alias owner counts again; and (round 10, which
// found two of the audit's three clauses unpinned) a cooled-down sender
// (resolved = false, its earlier login kept) and a login-less terminal
// stamp (a bot) do not count.
func TestSenderResolveAuditSQLCounts(t *testing.T) {
	store, ctx := openSR5Store(t)
	const email = "_avaudit_probe@example.invalid"
	const login = "avauditprobe"
	clean := func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, email)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.mailing_list_sender_resolve WHERE sender_email = $1`, email)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.contributors WHERE gh_login = $1 OR cntrb_login = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	count := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, SenderResolveAuditSQL()+" AND r.sender_email = $1", email).Scan(&n); err != nil {
			t.Fatalf("the audit SQL does not run: %v\n%s", err, SenderResolveAuditSQL())
		}
		return n
	}
	if err := store.MarkSenderResolveAttempt(ctx, email, true, "noreply", login); err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 1 {
		t.Errorf("a resolved sender with a login and no alias counts %d; want 1", n)
	}
	_, id, err := store.UpsertContributorFull(ctx, GithubUUID(424242425).String(), login, 424242425, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureContributorAlias(ctx, id, email, MailingListToolSource, "Mailing List"); err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 0 {
		t.Errorf("with an active alias the sender counts %d; want 0", n)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.contributors SET cntrb_deleted = 1 WHERE cntrb_id = $1`, id)
	if n := count(); n != 1 {
		t.Errorf("with the alias owner soft-deleted the sender counts %d; want 1 (the candidate query's own notion of an active alias)", n)
	}
	// The premise each assertion rests on is asserted too (review round
	// 11: the cooldown pin was disarmed by a mutant that made the cooldown
	// CLEAR the login, and a store change blamed the audit SQL).
	row := func() (resolved bool, resolvedLogin string) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, `SELECT resolved, resolved_login FROM aveloxis_ops.mailing_list_sender_resolve WHERE sender_email = $1`, email).Scan(&resolved, &resolvedLogin); err != nil {
			t.Fatalf("reading the resolve row: %v", err)
		}
		return resolved, resolvedLogin
	}
	// A cooldown stamp keeps the earlier login (the DO UPDATE's ELSE arm)
	// but flips resolved off: not a link, not counted. The order matters:
	// the bot stamp below would leave the login empty, and a cooldown after
	// it would keep the empty login, so the cooldown must come first.
	if err := store.MarkSenderResolveAttempt(ctx, email, false, "", ""); err != nil {
		t.Fatal(err)
	}
	if resolved, got := row(); resolved || got != login {
		t.Fatalf("after a cooldown stamp the row is (resolved=%v, resolved_login=%q); want (false, %q) — a cooldown flips resolved off and the store's ELSE arm keeps the earlier login", resolved, got, login)
	}
	if n := count(); n != 0 {
		t.Errorf("a cooled-down (resolved = false) sender counts %d; want 0 — the audit's `r.resolved` clause", n)
	}
	// A terminal stamp without a login (a bot address) is resolved but
	// links nobody: not counted.
	if err := store.MarkSenderResolveAttempt(ctx, email, true, "bot", ""); err != nil {
		t.Fatal(err)
	}
	if resolved, got := row(); !resolved || got != "" {
		t.Fatalf("after a bot stamp the row is (resolved=%v, resolved_login=%q); want (true, \"\") — the store's THEN arm takes the stamp's empty login", resolved, got)
	}
	if n := count(); n != 0 {
		t.Errorf("a terminal stamp without a login counts %d; want 0 — the audit's `resolved_login <> ''` clause", n)
	}
}
