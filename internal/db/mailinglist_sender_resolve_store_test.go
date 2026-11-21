// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSchemaDeclaresSenderResolveTable pins the Phase 2 cooldown/outcome table.
func TestSchemaDeclaresSenderResolveTable(t *testing.T) {
	src := readSchema(t)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.mailing_list_sender_resolve",
		"sender_email     TEXT PRIMARY KEY",
		"resolved         BOOLEAN NOT NULL DEFAULT FALSE",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must contain %q for the Phase 2 sender-resolve layer", needle)
		}
	}
}

// TestSenderResolveCandidatesAndStamp exercises the candidate query + the
// cooldown/outcome stamp end-to-end against a live DB: a >=threshold sender the
// DB can't resolve appears; stamping resolved=true drops it; a sub-threshold
// sender never appears; a DB-resolvable sender never appears.
func TestSenderResolveCandidatesAndStamp(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const (
		hot      = "_av_srhot@example.org"   // 6 messages, unresolvable → candidate
		cold     = "_av_srcold@example.org"  // 2 messages → below threshold
		known    = "_av_srknown@example.org" // 6 messages but DB-resolvable
		listAddr = "dev@_av_sr.apache.org"
		rgls     = int64(778899001)
	)
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE data_source=$1`, listAddr)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_sender_resolve WHERE sender_email IN ($1,$2,$3)`, hot, cold, known)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_email=$1`, known)
	}
	clean()
	t.Cleanup(clean)

	// A contributor that already resolves `known`.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email)
		VALUES (gen_random_uuid(), '_av_srknown', $1)`, known); err != nil {
		t.Fatalf("seed known contributor: %v", err)
	}

	seed := func(email string, n int) {
		for i := 0; i < n; i++ {
			if _, err := store.pool.Exec(ctx, `
				INSERT INTO aveloxis_data.email_message
					(message_id_header, sender_email, data_source, platform_id, ml_system)
				VALUES ($1, $2, $3, 6, 'apache_ponymail')
				ON CONFLICT (message_id_header) DO NOTHING`,
				email+"-"+time.Now().Format("150405.000000000")+"-"+strconv.Itoa(i), email, listAddr); err != nil {
				t.Fatalf("seed email_message: %v", err)
			}
		}
	}
	seed(hot, 6)
	seed(cold, 2)
	seed(known, 6)

	cands, err := store.GetMailingListSenderResolveCandidates(ctx, 6, (30 * 24 * time.Hour).Seconds(), 100)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	got := map[string]bool{}
	for _, c := range cands {
		got[c.SenderEmail] = true
	}
	if !got[hot] {
		t.Error("a >=6-message unresolvable sender must be a candidate")
	}
	if got[cold] {
		t.Error("a sub-threshold sender must NOT be a candidate")
	}
	if got[known] {
		t.Error("a DB-resolvable sender must NOT be a candidate")
	}

	// Stamp resolved=true → drops out.
	if err := store.MarkSenderResolveAttempt(ctx, hot, true, "commit-search", "_av_srhot"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	cands2, _ := store.GetMailingListSenderResolveCandidates(ctx, 6, (30 * 24 * time.Hour).Seconds(), 100)
	for _, c := range cands2 {
		if c.SenderEmail == hot {
			t.Error("a resolved sender must drop out of the candidate set")
		}
	}
}

// TestLinkMailingListSenderCreatesContributorAndAlias pins the link helper:
// a resolved (login, ghUserID) materializes a contributor (deterministic UUID)
// and records the sender email as an alias.
func TestLinkMailingListSenderCreatesContributorAndAlias(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const (
		email = "_av_srlink@example.org"
		login = "_av_srlink"
		ghID  = int64(99887766)
	)
	want := GithubUUID(ghID).String()
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email=$1`, email)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id=$1::uuid OR gh_login=$2`, want, login)
	}
	clean()
	t.Cleanup(clean)

	id, err := store.LinkMailingListSender(ctx, email, login, ghID)
	if err != nil {
		t.Fatalf("LinkMailingListSender: %v", err)
	}
	if id != want {
		t.Errorf("cntrb_id = %q, want deterministic %q", id, want)
	}
	var n int
	store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors WHERE cntrb_id=$1::uuid AND gh_login=$2`, want, login).Scan(&n)
	if n != 1 {
		t.Errorf("expected the contributor row created with login %q; got %d", login, n)
	}
	store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE alias_email=$1 AND cntrb_id=$2::uuid`, email, want).Scan(&n)
	if n != 1 {
		t.Errorf("expected an alias row mapping %q → %q; got %d", email, want, n)
	}
}

// TestCreateEmailOnlyContributor pins Phase 4: an email-only contributor is
// created (random cntrb_id, cntrb_email set, gh_user_id NULL → eligible for the
// convergence ticker), idempotent on re-call, and gets an alias row.
func TestCreateEmailOnlyContributor(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const email = "_av_eoc_human@example.org"
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email=$1`, email)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_email=$1`, email)
	}
	clean()
	t.Cleanup(clean)

	id, err := store.CreateEmailOnlyContributor(ctx, email)
	if err != nil || id == "" {
		t.Fatalf("create: id=%q err=%v", id, err)
	}
	var login, gh string
	var ghUserID *int64
	store.pool.QueryRow(ctx, `SELECT cntrb_login, gh_login, gh_user_id FROM aveloxis_data.contributors WHERE cntrb_id=$1::uuid`, id).Scan(&login, &gh, &ghUserID)
	if login != "" || gh != "" || ghUserID != nil {
		t.Errorf("email-only contributor must have empty login + NULL gh_user_id (convergence precondition); got login=%q gh=%q ghUserID=%v", login, gh, ghUserID)
	}
	var canon string
	store.pool.QueryRow(ctx, `SELECT cntrb_canonical FROM aveloxis_data.contributors WHERE cntrb_id=$1::uuid`, id).Scan(&canon)
	if canon != email {
		t.Errorf("cntrb_canonical = %q, want %q (so the convergence ticker can resolve it later)", canon, email)
	}
	// Idempotent: a second call returns the same row (no duplicate).
	id2, err := store.CreateEmailOnlyContributor(ctx, email)
	if err != nil || id2 != id {
		t.Errorf("re-create must return the same id (idempotent); got %q vs %q err=%v", id2, id, err)
	}
	var n int
	store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors WHERE cntrb_email=$1`, email).Scan(&n)
	if n != 1 {
		t.Errorf("expected exactly one email-only contributor, got %d", n)
	}
	var aliases int
	store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors_aliases WHERE alias_email=$1`, email).Scan(&aliases)
	if aliases != 1 {
		t.Errorf("expected an alias row for convergence, got %d", aliases)
	}
}

// TestMarkSenderResolveAttemptCooldownKeepsTheEarlierOutcome pins the stamp's
// per-column contract at the store (batch 3 review round 12: it was pinned
// only inside the audit test, and resolved_source's ELSE arm nowhere): a
// terminal stamp takes source and login; a cooldown flips resolved off,
// advances last_attempt_at and KEEPS the earlier source and login (the DO
// UPDATE's ELSE arms); a later terminal stamp with no login takes its source
// and empties the login.
func TestMarkSenderResolveAttemptCooldownKeepsTheEarlierOutcome(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	const email = "_av_srkeep@example.org"
	clean := func() {
		if _, err := store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.mailing_list_sender_resolve WHERE sender_email = $1`, email); err != nil {
			t.Logf("cleanup: %v", err)
		}
	}
	clean()
	t.Cleanup(clean)
	row := func() (resolved bool, source, login string, at time.Time) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, `SELECT resolved, resolved_source, resolved_login, last_attempt_at FROM aveloxis_ops.mailing_list_sender_resolve WHERE sender_email = $1`, email).Scan(&resolved, &source, &login, &at); err != nil {
			t.Fatalf("reading the resolve row: %v", err)
		}
		return
	}
	if err := store.MarkSenderResolveAttempt(ctx, email, true, "noreply", "srkeep"); err != nil {
		t.Fatal(err)
	}
	resolved, source, login, first := row()
	if !resolved || source != "noreply" || login != "srkeep" {
		t.Fatalf("after a terminal stamp: (%v, %q, %q); want (true, noreply, srkeep)", resolved, source, login)
	}
	if err := store.MarkSenderResolveAttempt(ctx, email, false, "", ""); err != nil {
		t.Fatal(err)
	}
	resolved, source, login, second := row()
	if resolved || source != "noreply" || login != "srkeep" || !second.After(first) {
		t.Errorf("after a cooldown stamp: (%v, %q, %q, advanced=%v); want (false, noreply kept, srkeep kept, true) — the DO UPDATE's ELSE arms keep the earlier outcome", resolved, source, login, second.After(first))
	}
	if err := store.MarkSenderResolveAttempt(ctx, email, true, "bot", ""); err != nil {
		t.Fatal(err)
	}
	if resolved, source, login, _ := row(); !resolved || source != "bot" || login != "" {
		t.Errorf("after a terminal stamp with no login: (%v, %q, %q); want (true, bot, \"\") — the THEN arms take the stamp's values", resolved, source, login)
	}
}

// TestLinkMailingListSenderNeverWritesZeroAsTheForgeID: a sender linked by
// login alone (no numeric id from the forge) leaves gh_user_id NULL, and a
// later link that has the id fills it. Before 0.29.216 the id-less link sent
// 0 to COALESCE(gh_user_id, $n), which stored 0; the column is fill-only
// with NULL as its one empty value (SR-11), so nothing could ever fill it
// again. Production held no such row on 2026-11-21 (review round 9), so
// there is nothing to repair: this closes the one path that could write it.
// Every arm of UpsertContributorFull is driven, since each sends the id.
func TestLinkMailingListSenderNeverWritesZeroAsTheForgeID(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const (
		email    = "_av_srzero@example.org"
		login    = "_av_srzero"
		newLogin = "_av_srzero_new"
		ghID     = int64(99887701)
		rowID    = "0a000000-0000-4000-8000-00000000f001"
		otherID  = "0a000000-0000-4000-8000-00000000f002"
		freshID  = "0a000000-0000-4000-8000-00000000f003"
	)
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email=$1`, email)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = ANY($1::uuid[]) OR cntrb_login = ANY($2)`,
			[]string{rowID, otherID, freshID, GithubUUID(ghID).String()}, []string{login, newLogin})
	}
	clean()
	t.Cleanup(clean)
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login) VALUES ($1::uuid, $2, $2)`, rowID, login); err != nil {
		t.Fatal(err)
	}
	stored := func(id string) string {
		t.Helper()
		var v *int64
		if err := store.pool.QueryRow(ctx, `SELECT gh_user_id FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&v); err != nil {
			t.Fatalf("contributor %s: %v", id, err)
		}
		if v == nil {
			return "NULL"
		}
		return strconv.FormatInt(*v, 10)
	}

	// The row is found by its id (the arm that updates it in place).
	if id, err := store.LinkMailingListSender(ctx, email, login, 0); err != nil || id != rowID {
		t.Fatalf("LinkMailingListSender by login alone = (%q, %v), want the existing row", id, err)
	}
	if got := stored(rowID); got != "NULL" {
		t.Fatalf("after a link with no forge id, gh_user_id = %s, want NULL", got)
	}
	// The login is found under another id (the arm that updates that row).
	if _, id, err := store.UpsertContributorFull(ctx, otherID, login, 0, email); err != nil || id != rowID {
		t.Fatalf("UpsertContributorFull under another id = (%q, %v), want the login's row", id, err)
	}
	if got := stored(rowID); got != "NULL" {
		t.Fatalf("after an upsert of the login under another id with no forge id, gh_user_id = %s, want NULL", got)
	}
	// Neither is found (the arm that inserts).
	if created, id, err := store.UpsertContributorFull(ctx, freshID, newLogin, 0, ""); err != nil || !created || id != freshID {
		t.Fatalf("UpsertContributorFull of a new row = (%v, %q, %v)", created, id, err)
	}
	if got := stored(freshID); got != "NULL" {
		t.Fatalf("a row inserted with no forge id has gh_user_id = %s, want NULL", got)
	}
	// The id, once the forge gives it, fills the row: it was left empty.
	if id, err := store.LinkMailingListSender(ctx, email, login, ghID); err != nil || id != rowID {
		t.Fatalf("LinkMailingListSender with the id = (%q, %v), want the login's row", id, err)
	}
	if got := stored(rowID); got != strconv.FormatInt(ghID, 10) {
		t.Fatalf("after a link with the forge id, gh_user_id = %s, want %d", got, ghID)
	}
	// And is not replaced, by 0 or by another.
	for _, later := range []int64{0, ghID + 1} {
		if _, _, err := store.UpsertContributorFull(ctx, rowID, login, later, email); err != nil {
			t.Fatal(err)
		}
		if got := stored(rowID); got != strconv.FormatInt(ghID, 10) {
			t.Fatalf("after an upsert with id %d, gh_user_id = %s, want it kept at %d", later, got, ghID)
		}
	}
}
