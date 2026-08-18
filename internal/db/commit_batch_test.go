// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.27.97 — summary/21 F2: the facade wrote commits one row per statement.
// The 11-day production snapshot measured the per-row path at ~405 h of DB
// time (INSERT commits: 296.8 h / 293.5M calls; commit_messages: 39.5 h /
// 58.8M calls). These batch methods collapse the round trips ~500×.

// TestUpsertCommitBatchShape pins the load-bearing SQL properties.
func TestUpsertCommitBatchShape(t *testing.T) {
	src := readFileForTest(t, "commit_batch.go")

	if !strings.Contains(src, "func (s *PostgresStore) UpsertCommitBatch(ctx context.Context, commits []*model.Commit) error") {
		t.Fatal("UpsertCommitBatch missing — the F2 batch write path")
	}
	// Same arbiter as the single-row UpsertCommit — the v0.7-era dedup
	// index. DO NOTHING is intra-statement-duplicate safe (unlike DO
	// UPDATE), which is why the commits batch needs no in-batch dedup.
	if !strings.Contains(src, "ON CONFLICT (repo_id, cmt_commit_hash, cmt_filename) DO NOTHING") {
		t.Error("UpsertCommitBatch must keep UpsertCommit's exact arbiter " +
			"(repo_id, cmt_commit_hash, cmt_filename) DO NOTHING")
	}
	if !strings.Contains(src, "commitBatchChunk") {
		t.Error("UpsertCommitBatch must chunk (22 params/row × 65535-param " +
			"protocol limit; named constant commitBatchChunk)")
	}
	if !strings.Contains(src, "withRetry") {
		t.Error("batch chunks must go through withRetry (the v0.18.14 " +
			"prepared-statement-retry contract, same as the single-row path)")
	}

	if !strings.Contains(src, "func (s *PostgresStore) UpsertCommitMessageBatch(ctx context.Context, msgs []*model.CommitMessage) error") {
		t.Fatal("UpsertCommitMessageBatch missing")
	}
	// Messages use DO UPDATE — one statement affecting the same
	// (repo_id, cmt_hash) twice errors with "cannot affect row a second
	// time", so the batch MUST dedup by hash first.
	if !strings.Contains(src, "ON CONFLICT (repo_id, cmt_hash) DO UPDATE") {
		t.Error("UpsertCommitMessageBatch must keep UpsertCommitMessage's DO UPDATE clause")
	}
	if !strings.Contains(src, "SanitizeText") {
		t.Error("UpsertCommitMessageBatch must sanitize message text like the single-row path")
	}
}

// TestUpsertCommitBatchEndToEnd proves batch behavior on a live database:
// multi-file commits land, intra-batch duplicate file rows are tolerated,
// and a re-run is a no-op (idempotent re-collection, R9-adjacent).
func TestUpsertCommitBatchEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)

	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ('https://github.com/avtest-cb/repo', 'avtest-cb', 'repo', 1, 1)
		RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	cleanup := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.commit_messages WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	}
	t.Cleanup(cleanup)

	ts := time.Now().UTC()
	mk := func(hash, file string) *model.Commit {
		c := &model.Commit{
			RepoID: repoID, Hash: hash, AuthorName: "AV Test",
			AuthorRawEmail: "avtest-cb@example.org", AuthorEmail: "avtest-cb@example.org",
			AuthorDate: "2026-08-18", CommitterName: "AV Test",
			CommitterRawEmail: "avtest-cb@example.org", CommitterEmail: "avtest-cb@example.org",
			CommitterDate: "2026-08-18", Filename: file,
			LinesAdded: 1, LinesRemoved: 0,
			AuthorTimestamp: &ts, CommitterTimestamp: &ts,
			Origin: model.DataOrigin{ToolSource: "aveloxis-facade", DataSource: "git"},
		}
		return c
	}
	batch := []*model.Commit{
		mk("avtestcbhash1", "a.go"), mk("avtestcbhash1", "b.go"),
		mk("avtestcbhash2", "a.go"),
		mk("avtestcbhash2", "a.go"), // intra-batch duplicate — DO NOTHING must tolerate
	}
	if err := store.UpsertCommitBatch(ctx, batch); err != nil {
		t.Fatalf("UpsertCommitBatch: %v", err)
	}

	var rows int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 3 {
		t.Errorf("expected 3 commit-file rows (dup collapsed), got %d", rows)
	}

	// Idempotent re-run.
	if err := store.UpsertCommitBatch(ctx, batch); err != nil {
		t.Fatalf("re-run UpsertCommitBatch: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID).Scan(&rows); err != nil {
		t.Fatalf("recount: %v", err)
	}
	if rows != 3 {
		t.Errorf("re-run must be a no-op, got %d rows", rows)
	}

	// Message batch: intra-batch duplicate hash must dedup, and the DO
	// UPDATE must land the latest message.
	msgs := []*model.CommitMessage{
		{RepoID: repoID, Hash: "avtestcbhash1", Message: "first"},
		{RepoID: repoID, Hash: "avtestcbhash1", Message: "second wins"},
		{RepoID: repoID, Hash: "avtestcbhash2", Message: "solo"},
	}
	if err := store.UpsertCommitMessageBatch(ctx, msgs); err != nil {
		t.Fatalf("UpsertCommitMessageBatch: %v", err)
	}
	var got string
	if err := store.pool.QueryRow(ctx, `
		SELECT cmt_msg FROM aveloxis_data.commit_messages
		WHERE repo_id = $1 AND cmt_hash = 'avtestcbhash1'`, repoID).Scan(&got); err != nil {
		t.Fatalf("read message: %v", err)
	}
	if got != "second wins" {
		t.Errorf("intra-batch dedup must keep the LAST message for a hash, got %q", got)
	}
}
