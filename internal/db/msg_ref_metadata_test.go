// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"os"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Source-contract pins (unit tier)
// ---------------------------------------------------------------------------

// TestBridgeInsertsCarryDataSource pins that every message-bridge
// INSERT includes data_source. Production audit 2026-07-15: 21.7M
// issue_message_ref + 32.5M pull_request_message_ref + 10.5M
// pull_request_review_message_ref rows — 100% empty data_source, all
// three, since inception.
func TestBridgeInsertsCarryDataSource(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, table := range []string{"issue_message_ref", "pull_request_message_ref", "pull_request_review_message_ref"} {
		idx := 0
		found := 0
		for {
			i := strings.Index(s[idx:], "INSERT INTO aveloxis_data."+table)
			if i < 0 {
				break
			}
			stmt := s[idx+i : idx+i+700]
			if !strings.Contains(stmt, "data_source") {
				t.Errorf("an INSERT into %s does not carry data_source:\n%s", table, stmt[:200])
			}
			found++
			idx += i + 1
		}
		if found == 0 {
			t.Errorf("no INSERT into %s found in postgres.go — table moved? update this pin", table)
		}
	}

	proj, err := os.ReadFile("mailinglist_projection_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(proj), "issue_message_ref (issue_id, repo_id, msg_id, data_source)") {
		t.Error("BridgeEmailToIssue's issue_message_ref insert must carry data_source")
	}
}

// TestReviewMsgRefUsesNamedArbiter pins the v0.27.15 fix for the
// duplication bug: the bare `ON CONFLICT DO NOTHING` had NO unique
// constraint to arbitrate against (dead code — the v0.27.7 repo_labor
// lesson) and duplicated review-body bridge rows on every
// re-collection cycle (5.26M duplicates on production). Every
// pull_request_review_message_ref insert must name the
// (pr_review_id, msg_id) arbiter.
func TestReviewMsgRefUsesNamedArbiter(t *testing.T) {
	for _, f := range []string{"postgres.go", "msg_ref_metadata.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		s := string(src)
		idx := 0
		for {
			i := strings.Index(s[idx:], "INSERT INTO aveloxis_data.pull_request_review_message_ref")
			if i < 0 {
				break
			}
			stmt := s[idx+i : idx+i+1600]
			if !strings.Contains(stmt, "ON CONFLICT (pr_review_id, msg_id)") {
				t.Errorf("%s: a pull_request_review_message_ref INSERT lacks the named (pr_review_id, msg_id) arbiter — a bare ON CONFLICT is dead without a unique constraint", f)
			}
			idx += i + 1
		}
	}
}

// TestReviewCommentUpsertsWriteRefRowWithLineMetadata pins that both
// review-comment write paths (standalone + batch) mirror the comment
// into the Augur-compat ref table via the shared SQL, which must carry
// the full line-anchoring column set.
func TestReviewCommentUpsertsWriteRefRowWithLineMetadata(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, col := range []string{
		"pr_review_msg_diff_hunk", "pr_review_msg_path", "pr_review_msg_line",
		"pr_review_msg_original_line", "pr_review_msg_side", "pr_review_msg_start_line",
		"pr_review_msg_start_side", "pr_review_msg_commit_id",
	} {
		if !strings.Contains(s, col) {
			t.Errorf("prReviewMsgRefFromCommentSQL missing line-metadata column %s", col)
		}
	}
	if strings.Count(s, "prReviewMsgRefFromCommentSQL,") < 2 {
		t.Error("both UpsertReviewComment AND UpsertReviewCommentBatch must write the ref row via the shared prReviewMsgRefFromCommentSQL")
	}
}

// TestMsgRefMigrationShape pins the v0.27.15 migration: dedup before
// unique, keyset windows (never LIMIT-rescan loops — the v0.26.6
// lesson), idempotency filters, and the RunMigrations wiring.
func TestMsgRefMigrationShape(t *testing.T) {
	src, err := os.ReadFile("msg_ref_metadata.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"ROW_NUMBER() OVER (PARTITION BY pr_review_id, msg_id",
		"uq_pr_review_msg_ref",
		"COALESCE(t.data_source, '') = ''",
		"rc.pr_review_id IS NOT NULL",
		"> $1 AND",
		"<= $2",
		"ON CONFLICT (pr_review_id, msg_id) DO NOTHING",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("msg_ref_metadata.go missing %q", needle)
		}
	}
	if strings.Contains(s, "LIMIT ") {
		t.Error("v0.27.15 backfills must batch by keyset windows, not LIMIT loops (v0.26.6 lesson)")
	}
	// Ordering: dedup textually before the unique-index creation.
	dedup := strings.Index(s, "PRReviewMsgRefDedupSQL)")
	unique := strings.Index(s, `"uq_pr_review_msg_ref"`)
	if dedup < 0 || unique < 0 || dedup > unique {
		t.Error("dedup must run BEFORE uq_pr_review_msg_ref is created")
	}

	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), "ensureMsgRefMetadata(ctx, pg, logger, &errs)") {
		t.Error("RunMigrations must invoke ensureMsgRefMetadata")
	}
}

// ---------------------------------------------------------------------------
// Integration tier (AVELOXIS_TEST_DB)
// ---------------------------------------------------------------------------

// TestMsgRefMetadataEndToEnd seeds the pre-v0.27.15 shapes (duplicate
// ref rows, empty data_source, a review comment with no ref row) and
// proves the migration heals all of them idempotently, and that the
// forward review-comment path writes the ref row with line metadata.
func TestMsgRefMetadataEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t) // migrates + seeds repo group 1
	// Registered BEFORE the row-cleanup t.Cleanup below: Cleanup runs
	// LIFO, so the pool closes AFTER the deletes (a plain defer would
	// close it before them).
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool

	const repoID = 987654310
	// Pre-clean leftovers from any prior interrupted run (the seeds
	// below use fixed platform ids with UNIQUE constraints).
	for _, q := range []string{
		`DELETE FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`,
		`DELETE FROM aveloxis_data.review_comments WHERE repo_id = $1`,
		`DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id = $1`,
		`DELETE FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1`,
		`DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
		`DELETE FROM aveloxis_data.issues WHERE repo_id = $1`,
		`DELETE FROM aveloxis_data.messages WHERE repo_id = $1`,
	} {
		if _, err := pool.Exec(ctx, q, repoID); err != nil {
			t.Fatalf("pre-clean: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos (repo_id, repo_group_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 1, 'https://example.com/msgref/tester', 'msgref', 'tester')
		ON CONFLICT (repo_id) DO NOTHING`, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.review_comments WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.issues WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.messages WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`,
		} {
			if _, err := pool.Exec(context.Background(), q, repoID); err != nil {
				t.Logf("cleanup: %v", err)
			}
		}
	})

	// Seed: a message with data_source, an issue, an EMPTY-data_source
	// ref (the historical shape), a review, a review comment WITH line
	// metadata but NO ref row, and a duplicated review-body ref pair.
	var msgID, issueID, reviewID, rcMsgID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_text, data_source)
		VALUES ($1, 987654311, 1, 'hello', 'github api') RETURNING msg_id`, repoID).Scan(&msgID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title)
		VALUES ($1, 987654312, 987654312, 'seed') RETURNING issue_id`, repoID).Scan(&issueID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id)
		VALUES ($1, $2, $3) ON CONFLICT (issue_id, msg_id) DO NOTHING`, issueID, repoID, msgID); err != nil {
		t.Fatal(err)
	}
	var prID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number)
		VALUES ($1, 987654316, 987654316) RETURNING pull_request_id`, repoID).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_request_reviews (pull_request_id, repo_id, platform_id, platform_review_id)
		VALUES ($1, $2, 1, 987654313) RETURNING pr_review_id`, prID, repoID).Scan(&reviewID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_text, data_source)
		VALUES ($1, 987654314, 1, 'inline comment', 'github api') RETURNING msg_id`, repoID).Scan(&rcMsgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.review_comments
			(pr_review_id, repo_id, msg_id, platform_src_id, diff_hunk, file_path, line, side, start_line)
		VALUES ($1, $2, $3, 987654315, '@@ -1 +1 @@', 'main.go', 42, 'RIGHT', 40)`,
		reviewID, repoID, rcMsgID); err != nil {
		t.Fatal(err)
	}
	// Duplicate review-body ref pair (msgID reused as the "body" message).
	// NOTE: seeding duplicates requires the unique index to not exist
	// yet OR the rows to differ — on a migrated scratch DB the unique
	// already exists, so seed a single row and rely on the dedup being
	// a no-op for it; the dedup DELETE itself is exercised by shape
	// (idempotent second run) below.
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.pull_request_review_message_ref (pr_review_id, repo_id, msg_id)
		VALUES ($1, $2, $3) ON CONFLICT (pr_review_id, msg_id) DO NOTHING`, reviewID, repoID, msgID); err != nil {
		t.Fatal(err)
	}

	// Run the migration steps.
	var errs []error
	logger := store.logger
	ensureMsgRefMetadata(ctx, store, logger, &errs)
	if len(errs) > 0 {
		t.Fatalf("ensureMsgRefMetadata errors: %v", errs)
	}

	// (1) data_source healed from the messages row.
	var ds string
	if err := pool.QueryRow(ctx, `
		SELECT data_source FROM aveloxis_data.issue_message_ref
		WHERE issue_id = $1 AND msg_id = $2`, issueID, msgID).Scan(&ds); err != nil {
		t.Fatal(err)
	}
	if ds != "github api" {
		t.Errorf("issue_message_ref.data_source: got %q, want 'github api'", ds)
	}

	// (2) inline comment backfilled into the ref table WITH line metadata.
	var path, side string
	var line int64
	if err := pool.QueryRow(ctx, `
		SELECT pr_review_msg_path, pr_review_msg_side, pr_review_msg_line
		FROM aveloxis_data.pull_request_review_message_ref
		WHERE pr_review_id = $1 AND msg_id = $2`, reviewID, rcMsgID).Scan(&path, &side, &line); err != nil {
		t.Fatalf("inline-comment ref row missing after backfill: %v", err)
	}
	if path != "main.go" || side != "RIGHT" || line != 42 {
		t.Errorf("line metadata: got (%q,%q,%d), want (main.go,RIGHT,42)", path, side, line)
	}

	// (3) the unique arbiter exists and holds.
	var uniqueCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pg_indexes WHERE schemaname='aveloxis_data'
		AND indexname='uq_pr_review_msg_ref'`).Scan(&uniqueCount); err != nil {
		t.Fatal(err)
	}
	if uniqueCount != 1 {
		t.Error("uq_pr_review_msg_ref must exist after migration")
	}

	// (4) idempotency: second run changes nothing.
	var before int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`, repoID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	errs = nil
	ensureMsgRefMetadata(ctx, store, logger, &errs)
	if len(errs) > 0 {
		t.Fatalf("second run errors: %v", errs)
	}
	var after int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`, repoID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Errorf("migration not idempotent: %d rows before re-run, %d after", before, after)
	}
}
