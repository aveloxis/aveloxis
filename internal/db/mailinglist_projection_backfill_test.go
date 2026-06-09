// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"io"
	"log/slog"
	"testing"
)

// TestBackfillMailingListProjection drives the Phase 5 three-step backfill over
// seeded pre-projection email_message rows: a keyed issue_event projects to an
// issue (+ bridges its body), a threaded discuss reply inherits that issue, a
// lone discuss is marked mailing_list_only — and a re-run is a no-op.
func TestBackfillMailingListProjection(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()

	const (
		repoGit = "https://github.com/_av_bf/repo"
		list    = "_av_bf_list"
	)
	clean := func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE data_source=$1`, list)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git=$1)`, repoGit)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE data_source=$1`, list)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git=$1`, repoGit)
	}
	clean()
	t.Cleanup(clean)

	var repoID int64
	store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (platform_id, repo_git, repo_owner, repo_name) VALUES (1,$1,'_av_bf','repo') RETURNING repo_id`, repoGit).Scan(&repoID)

	// Seed body rows (messages) + email_message rows, projected_kind='' (pre-projection).
	seedBody := func(node string, pmid int64) {
		store.pool.Exec(ctx, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, node_id, msg_text, data_source) VALUES ($1,$2,6,$3,'b',$4)`, repoID, pmid, node, list)
	}
	seedEmail := func(node, class, key, thread string) {
		store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.email_message
				(repo_id, platform_id, ml_system, message_id_header, list_address, subject,
				 msg_class, linked_external_key, thread_root_id, data_source)
			VALUES ($1, 6, 'apache_ponymail', $2, $3, $4, $5, $6, $7, $3)`,
			repoID, node, list, "[jira] [Created] ("+key+") fix it", class, key, thread)
	}
	seedBody("a@x", -9001)
	seedBody("b@x", -9002)
	seedBody("c@x", -9003)
	seedEmail("a@x", "issue_event", "BF-1", "") // keyed root
	seedEmail("b@x", "discuss", "", "a@x")      // threaded reply (inherits)
	seedEmail("c@x", "discuss", "", "")         // lone discussion

	// The backfill steps are GLOBAL (operator one-shot over the whole DB), so
	// assert per-row OUTCOMES for this test's rows, not exact global return
	// counts (a shared scratch DB may hold other tests' rows). Loop each step
	// to completion the way the CLI does.
	runToZero := func(name string, step func() (int, error)) {
		for {
			n, err := step()
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if n == 0 {
				return
			}
		}
	}
	runToZero("keyed", func() (int, error) { return store.BackfillKeyedIssueProjection(ctx, 100) })
	runToZero("thread", func() (int, error) { return store.BackfillThreadInheritance(ctx, 100) })
	if _, err := store.BackfillMarkRemainingProjected(ctx); err != nil {
		t.Fatalf("mark: %v", err)
	}

	// Assertions: A + B → issue (same issue), C → mailing_list_only.
	get := func(node string) (string, *int64) {
		var kind string
		var iss *int64
		store.pool.QueryRow(ctx, `SELECT projected_kind, linked_issue_id FROM aveloxis_data.email_message WHERE message_id_header=$1`, node).Scan(&kind, &iss)
		return kind, iss
	}
	ka, ia := get("a@x")
	kb, ib := get("b@x")
	kc, ic := get("c@x")
	if ka != "issue" || ia == nil {
		t.Errorf("a@x must be projected to an issue; got kind=%q issue=%v", ka, ia)
	}
	if kb != "issue" || ib == nil || *ib != *ia {
		t.Errorf("b@x must inherit a@x's issue; got kind=%q issue=%v (want %v)", kb, ib, ia)
	}
	if kc != "mailing_list_only" || ic != nil {
		t.Errorf("c@x must be mailing_list_only; got kind=%q issue=%v", kc, ic)
	}
	// Issue BF-1 has both A and B bridged as comments.
	var cc int
	store.pool.QueryRow(ctx, `SELECT comment_count FROM aveloxis_data.issues WHERE issue_id=$1`, *ia).Scan(&cc)
	if cc != 2 {
		t.Errorf("issue comment_count = %d, want 2 (a@x + b@x bridged)", cc)
	}
	// Idempotent: after the full pass above marked every '' row globally, a
	// re-run of each step is a no-op.
	if k2, _ := store.BackfillKeyedIssueProjection(ctx, 100); k2 != 0 {
		t.Errorf("keyed re-run after completion = %d, want 0", k2)
	}
	if t2, _ := store.BackfillThreadInheritance(ctx, 100); t2 != 0 {
		t.Errorf("thread re-run after completion = %d, want 0", t2)
	}
}

func TestMlBackfillTitle(t *testing.T) {
	cases := []struct{ subj, key, want string }{
		{"[jira] [Created] (BF-1) fix it", "BF-1", "fix it"},
		{"[BF-2] lock files", "BF-2", "lock files"},
		{"no key here", "BF-3", "no key here"},
	}
	for _, c := range cases {
		if got := mlBackfillTitle(c.subj, c.key); got != c.want {
			t.Errorf("mlBackfillTitle(%q,%q)=%q want %q", c.subj, c.key, got, c.want)
		}
	}
}

// TestEnsureMailingListProjectionIndexes verifies the helper runs cleanly
// (idempotent) and the two indexes exist afterward.
func TestEnsureMailingListProjectionIndexes(t *testing.T) {
	store, ctx := emConnect(t)
	defer store.Close()
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := store.EnsureMailingListProjectionIndexes(ctx, lg); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}
	// Idempotent second call.
	if err := store.EnsureMailingListProjectionIndexes(ctx, lg); err != nil {
		t.Fatalf("ensure indexes (re-run): %v", err)
	}
	for _, idx := range []string{"idx_messages_node_id", "idx_email_message_thread_root"} {
		var n int
		store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname='aveloxis_data' AND indexname=$1`, idx).Scan(&n)
		if n != 1 {
			t.Errorf("index %s must exist after EnsureMailingListProjectionIndexes; found %d", idx, n)
		}
	}
}
