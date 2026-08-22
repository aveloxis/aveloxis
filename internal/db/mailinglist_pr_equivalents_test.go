// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
	"time"
)

// TestMailingListPREquivalentsDeclaredNotRefreshed pins Phase C: the view is a
// plain VIEW and is NOT in the matview refresh list (refreshing a plain view
// errors every cycle). v0.27.115: the declaration MOVED from matviews.sql to
// views.sql — the always-run base-table-views file — because matviews.sql is
// structurally unreachable on populated fleets (the 2026-08-20 drift audit
// found the view missing on production for exactly that reason).
func TestMailingListPREquivalentsDeclaredNotRefreshed(t *testing.T) {
	src := readSourceFile(t, "views.sql")
	if !strings.Contains(src, "CREATE OR REPLACE VIEW aveloxis_data.mailing_list_pr_equivalents AS") {
		t.Error("views.sql must declare mailing_list_pr_equivalents as a plain VIEW")
	}
	// The special-case rationale must be documented inline (forge-less / kernel).
	if !strings.Contains(src, "forge-less") || !strings.Contains(src, "source='mailing_list'") {
		t.Error("views.sql must document the forge-less special case + the mail-derived source label")
	}
	mv := readSourceFile(t, "matviews.go")
	if strings.Contains(mv, `"aveloxis_data.mailing_list_pr_equivalents"`) {
		t.Error("mailing_list_pr_equivalents is a plain VIEW — it must NOT appear in matviewNames (refresh would error)")
	}
}

// TestMailingListPREquivalents seeds kernel-style [PATCH] thread mail and
// asserts the view surfaces it as one PR-equivalent row — while Apache-class
// mail (discuss/issue_event) produces ZERO rows (the forge-less filter).
func TestMailingListPREquivalents(t *testing.T) {
	store, ctx := emConnect(t) // runs Migrate → creates the view
	t.Cleanup(store.Close)

	const repoGit = "https://github.com/_av_pre/repo"
	const list = "linux-pci@_av_pre.kernel.org"
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE list_address=$1`, list)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	clean()
	t.Cleanup(clean)

	var repoID int64
	store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name) VALUES (1,$1,'_av_pre','repo') RETURNING repo_id`, repoGit).Scan(&repoID)

	em := func(node, thread, class, subject, sender string, t0 time.Time) {
		store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.email_message
				(repo_id, platform_id, ml_system, message_id_header, list_address, subject, sender_email, sent_at, thread_root_id, msg_class)
			VALUES ($1, 6, 'lore_public_inbox', $2, $3, $4, $5, $6, $7, $8)`,
			repoID, node, list, subject, sender, t0, thread, class)
	}
	base := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	em("p0@k", "", "patch_submission", "[PATCH 0/2] add pci driver", "dev1@kernel.org", base)
	em("r1@k", "p0@k", "review", "Re: [PATCH 0/2] add pci driver", "dev2@kernel.org", base.Add(time.Hour))
	em("r2@k", "p0@k", "review", "Re: [PATCH 0/2] add pci driver", "dev1@kernel.org", base.Add(2*time.Hour))
	// Apache-class mail on the same repo — must NOT appear in the view.
	em("d1@k", "", "discuss", "[DISCUSS] roadmap", "dev3@kernel.org", base.Add(3*time.Hour))

	var thread, title, author, source string
	var patches, reviews, participants int
	err := store.pool.QueryRow(ctx, `
		SELECT thread_key, title, author_email, patch_count, review_count, participant_count, source
		FROM aveloxis_data.mailing_list_pr_equivalents WHERE repo_id=$1`, repoID).
		Scan(&thread, &title, &author, &patches, &reviews, &participants, &source)
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	if thread != "p0@k" {
		t.Errorf("thread_key = %q, want p0@k", thread)
	}
	if title != "[PATCH 0/2] add pci driver" || author != "dev1@kernel.org" {
		t.Errorf("root title/author wrong: %q / %q", title, author)
	}
	if patches != 1 || reviews != 2 || participants != 2 || source != "mailing_list" {
		t.Errorf("counts/source wrong: patch=%d review=%d participants=%d source=%q", patches, reviews, participants, source)
	}

	// Exactly one PR-equivalent (the discuss row is excluded).
	var n int
	store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.mailing_list_pr_equivalents WHERE repo_id=$1`, repoID).Scan(&n)
	if n != 1 {
		t.Errorf("expected exactly 1 PR-equivalent (discuss excluded), got %d", n)
	}
}

// TestMailingListPREquivalentsOneRowPerThread — v0.27.117 (Copilot
// round 11, active): the pre-fix plain contributors join multiplied
// threads — neither email column is unique, and an EMPTY sender_email
// equaled every empty-email contributor row (the scratch DB carries
// plenty). Three shapes, each must yield EXACTLY one row per thread:
// ambiguous email (2 contributors share it → author_cntrb_id NULL),
// unique email (resolves), empty sender (NULL, and above all not
// multiplied).
func TestMailingListPREquivalentsOneRowPerThread(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	const repoGit = "https://github.com/_av_preamb/repo"
	const list = "dev@_av_preamb.kernel.org"
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE list_address=$1`, list)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_av_preamb%'`)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	clean()
	t.Cleanup(clean)

	var repoID int64
	store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name) VALUES (1,$1,'_av_preamb','repo') RETURNING repo_id`, repoGit).Scan(&repoID)

	// Two contributors SHARING one email (the ambiguity), one with a
	// unique email.
	for _, c := range []struct{ login, email string }{
		{"_av_preamb_a", "shared@_av_preamb.example"},
		{"_av_preamb_b", "shared@_av_preamb.example"},
		{"_av_preamb_c", "unique@_av_preamb.example"},
	} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email)
			VALUES (gen_random_uuid(), $1, $2)`, c.login, c.email); err != nil {
			t.Fatalf("seed contributor %s: %v", c.login, err)
		}
	}

	em := func(node, thread, sender string, t0 time.Time) {
		store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.email_message
				(repo_id, platform_id, ml_system, message_id_header, list_address, subject, sender_email, sent_at, thread_root_id, msg_class)
			VALUES ($1, 6, 'lore_public_inbox', $2, $3, '[PATCH] x', $4, $5, $6, 'patch_submission')`,
			repoID, node, list, sender, t0, thread)
	}
	base := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	em("amb@k", "", "shared@_av_preamb.example", base) // ambiguous → NULL
	em("uni@k", "", "unique@_av_preamb.example", base) // unique → resolves
	em("emp@k", "", "", base)                          // empty sender → NULL, ONE row

	rows, err := store.pool.Query(ctx, `
		SELECT thread_key, author_cntrb_id IS NULL
		FROM aveloxis_data.mailing_list_pr_equivalents WHERE repo_id=$1`, repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var key string
		var isNull bool
		if err := rows.Scan(&key, &isNull); err != nil {
			t.Fatal(err)
		}
		if _, dup := got[key]; dup {
			t.Fatalf("thread %q appears MORE THAN ONCE — the one-row-per-thread contract broke (the pre-v0.27.117 multiplication)", key)
		}
		got[key] = isNull
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected exactly 3 threads, got %d: %v", len(got), got)
	}
	if !got["amb@k"] {
		t.Error("ambiguous shared email must yield author_cntrb_id NULL, not an arbitrary pick")
	}
	if got["uni@k"] {
		t.Error("unique email must resolve author_cntrb_id")
	}
	if !got["emp@k"] {
		t.Error("empty sender must yield author_cntrb_id NULL")
	}
}
