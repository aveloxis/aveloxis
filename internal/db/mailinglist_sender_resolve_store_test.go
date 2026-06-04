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
	defer store.Close()

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
	defer store.Close()

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
