// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"regexp"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
)

// Worklist 70: every recollection rewrote every commit_messages row
// (ON CONFLICT ... DO UPDATE with no WHERE), so each vacuum of the
// ~200M-row table removed 9.3-14.9M dead tuples of unchanged text. Both
// writers (the batch and the single-row path) now skip the UPDATE when
// cmt_msg is unchanged. These tests compare the tuple's xmin across a
// re-upsert: an unchanged message keeps its tuple (no dead row), a changed
// message is rewritten.

func seedCommitMessageNoopRepo(t *testing.T, s *PostgresStore, slug string) int64 {
	t.Helper()
	ctx := t.Context()
	var repoID int64
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ('https://github.com/avtest-w70/'||$1, 'avtest-w70', $1, 1, 1)
		RETURNING repo_id`, slug).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	t.Cleanup(func() {
		cctx := context.Background() // never t.Context() in a cleanup
		cleanupExecRetry(cctx, s, `DELETE FROM aveloxis_data.commit_messages WHERE repo_id = $1`, repoID)
		cleanupExecRetry(cctx, s, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	return repoID
}

func commitMessageXmin(t *testing.T, s *PostgresStore, repoID int64, hash string) (xmin, msg string) {
	t.Helper()
	if err := s.pool.QueryRow(t.Context(), `
		SELECT xmin::text, cmt_msg FROM aveloxis_data.commit_messages
		WHERE repo_id = $1 AND cmt_hash = $2`, repoID, hash).Scan(&xmin, &msg); err != nil {
		t.Fatalf("read commit message %s: %v", hash, err)
	}
	return xmin, msg
}

func TestUpsertCommitMessageBatchSkipsUnchangedRows(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)
	repoID := seedCommitMessageNoopRepo(t, store, "batch")

	msgs := []*model.CommitMessage{
		{RepoID: repoID, Hash: "w70aaaa", Message: "first message"},
		{RepoID: repoID, Hash: "w70bbbb", Message: "second message"},
	}
	if err := store.UpsertCommitMessageBatch(ctx, msgs); err != nil {
		t.Fatalf("first batch: %v", err)
	}
	xa1, _ := commitMessageXmin(t, store, repoID, "w70aaaa")
	xb1, _ := commitMessageXmin(t, store, repoID, "w70bbbb")

	// Re-collection: a unchanged, b changed.
	msgs2 := []*model.CommitMessage{
		{RepoID: repoID, Hash: "w70aaaa", Message: "first message"},
		{RepoID: repoID, Hash: "w70bbbb", Message: "second message, amended"},
	}
	if err := store.UpsertCommitMessageBatch(ctx, msgs2); err != nil {
		t.Fatalf("second batch: %v", err)
	}
	xa2, ma := commitMessageXmin(t, store, repoID, "w70aaaa")
	xb2, mb := commitMessageXmin(t, store, repoID, "w70bbbb")

	if xa2 != xa1 {
		t.Errorf("unchanged message was rewritten (xmin %s -> %s): a re-upsert of identical cmt_msg must not create a dead tuple", xa1, xa2)
	}
	if ma != "first message" {
		t.Errorf("unchanged message text = %q", ma)
	}
	if xb2 == xb1 {
		t.Errorf("changed message was NOT rewritten (xmin stayed %s)", xb1)
	}
	if mb != "second message, amended" {
		t.Errorf("changed message text = %q, want the amended text", mb)
	}
}

func TestUpsertCommitMessageSkipsUnchangedRow(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)
	repoID := seedCommitMessageNoopRepo(t, store, "single")

	m := &model.CommitMessage{RepoID: repoID, Hash: "w70cccc", Message: "single message"}
	if err := store.UpsertCommitMessage(ctx, m); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	x1, _ := commitMessageXmin(t, store, repoID, "w70cccc")

	if err := store.UpsertCommitMessage(ctx, &model.CommitMessage{RepoID: repoID, Hash: "w70cccc", Message: "single message"}); err != nil {
		t.Fatalf("identical upsert: %v", err)
	}
	x2, _ := commitMessageXmin(t, store, repoID, "w70cccc")
	if x2 != x1 {
		t.Errorf("unchanged message was rewritten (xmin %s -> %s)", x1, x2)
	}

	if err := store.UpsertCommitMessage(ctx, &model.CommitMessage{RepoID: repoID, Hash: "w70cccc", Message: "single message v2"}); err != nil {
		t.Fatalf("changed upsert: %v", err)
	}
	x3, msg := commitMessageXmin(t, store, repoID, "w70cccc")
	if x3 == x2 {
		t.Errorf("changed message was NOT rewritten (xmin stayed %s)", x2)
	}
	if msg != "single message v2" {
		t.Errorf("changed message text = %q, want %q", msg, "single message v2")
	}
}

// TestCommitMessagesDoUpdateIsGuarded pins the one shape for every writer:
// any DO UPDATE on commit_messages in this package's non-test sources must
// carry the unchanged-row guard, so a new writer without it fails here (the
// runtime tests above cover the two current writers' behavior). The batch
// writer concatenates its VALUES list, so its ON CONFLICT tail is a separate
// literal that sqlscan cannot tie to the table (its documented blind spot);
// the tail is recognized by the commit_messages arbiter (repo_id, cmt_hash),
// which no other table in the package uses.
func TestCommitMessagesDoUpdateIsGuarded(t *testing.T) {
	const guard = "WHERE commit_messages.cmt_msg IS DISTINCT FROM EXCLUDED.cmt_msg"
	stmts := sqlscan.Statements(srctest.PackageFiles(t, "internal/db", 30))
	arbiter := regexp.MustCompile(`(?is)ON\s+CONFLICT\s*\(\s*repo_id\s*,\s*cmt_hash\s*\)`)
	doUpdate := regexp.MustCompile(`(?is)DO\s+UPDATE`)
	writes := map[string]bool{}
	for _, w := range sqlscan.FindWrites(stmts, "aveloxis_data.commit_messages") {
		writes[w.File+"\x00"+w.SQL] = true
	}
	examined := 0
	for _, st := range stmts {
		if !writes[st.File+"\x00"+st.SQL] && !arbiter.MatchString(st.SQL) {
			continue
		}
		if !doUpdate.MatchString(st.SQL) {
			continue
		}
		examined++
		if !srctest.ContainsNormalized(st.SQL, guard) {
			t.Errorf("%s: commit_messages DO UPDATE without %q; every recollection would rewrite unchanged rows (worklist 70):\n%s", st.File, guard, st.SQL)
		}
	}
	srctest.MinCount(t, "commit_messages DO UPDATE statements (UpsertCommitMessage, UpsertCommitMessageBatch tail)", examined, 2)
}
