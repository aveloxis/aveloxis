// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Integration tests for the v0.25.32 dedup-repos merge. Gated on
// AVELOXIS_TEST_DB (scratch DB only). The merge itself is exercised via
// dedupOnePair with a hand-built pair so the test can never touch other
// rows in a shared scratch database; the candidate query is exercised
// read-only via SampleCaseVariantRepoDups.
//
// Seeded shape (both sides "collected", like 1,216 of the 1,220
// production pairs):
//
//	winner (lower repo_id)  — issue (same platform_issue_id), queue row
//	loser                   — issue + PR + commits + a MESSAGE OWNED BY
//	                          THE LOSER bridged via issue_message_ref,
//	                          user_repos link, queue row, staging row
//
// The loser-owned message is the load-bearing case: messages are
// globally unique (the pair shares one row), so the merge must REPOINT
// it to the winner — deleting it would destroy the only copy.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDedupOnePairEndToEnd(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avdedup_pair"
	cleanupDedupRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupDedupRepos(ctx, t, store, slug) })

	winnerURL := "https://github.com/" + slug + "_Org/Repo"
	loserURL := strings.ToLower(winnerURL)
	winnerID, loserID := seedDedupPair(ctx, t, store, slug, winnerURL, loserURL)

	// The candidate query must surface exactly this pair.
	pair := findPairByLowerGit(ctx, t, store, strings.ToLower(winnerURL))
	if pair == nil {
		t.Fatal("candidate query did not surface the seeded pair")
	}
	if pair.WinnerID != winnerID || pair.LoserID != loserID {
		t.Fatalf("candidate pair = (winner %d, loser %d), want (%d, %d) — MIN(repo_id) rule",
			pair.WinnerID, pair.LoserID, winnerID, loserID)
	}
	if pair.Collecting {
		t.Fatal("pair should not be flagged collecting")
	}

	if err := dedupOnePair(ctx, store, *pair); err != nil {
		t.Fatalf("dedupOnePair: %v", err)
	}

	// Loser gone, winner intact.
	var n int
	store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id = $1`, loserID).Scan(&n)
	if n != 0 {
		t.Error("loser repos row must be deleted")
	}
	store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id = $1`, winnerID).Scan(&n)
	if n != 1 {
		t.Error("winner repos row must survive")
	}

	// The shared message was repointed to the winner (not deleted).
	var msgRepo int64
	if err := store.pool.QueryRow(ctx, `
		SELECT repo_id FROM aveloxis_data.messages WHERE platform_msg_id = 909090901 AND platform_id = 1`,
	).Scan(&msgRepo); err != nil {
		t.Fatalf("shared message must survive the merge: %v", err)
	}
	if msgRepo != winnerID {
		t.Errorf("shared message repo_id = %d, want winner %d (repointed)", msgRepo, winnerID)
	}

	// The loser's issue/PR/commit trees are gone; the winner's issue
	// survives.
	for _, q := range []string{
		`SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_data.commits WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_data.issue_message_ref WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_ops.staging WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE repo_id = $1`,
	} {
		store.pool.QueryRow(ctx, q, loserID).Scan(&n)
		if n != 0 {
			t.Errorf("loser rows must be gone: %q left %d rows", q, n)
		}
	}
	store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1`, winnerID).Scan(&n)
	if n != 1 {
		t.Errorf("winner's issue must survive, got %d", n)
	}

	// user_repos repointed: the group that referenced the loser now
	// references the winner.
	store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE repo_id = $1`, winnerID).Scan(&n)
	if n != 1 {
		t.Errorf("winner must carry the repointed user_repos link, got %d", n)
	}

	// Idempotent: the pair no longer appears in the candidate set.
	if p := findPairByLowerGit(ctx, t, store, strings.ToLower(winnerURL)); p != nil {
		t.Error("merged pair must drop out of the candidate set")
	}
}

func TestDedupOnePairSkipsCollectingLoser(t *testing.T) {
	ctx, store := caseConnect(t)
	const slug = "_avdedup_coll"
	cleanupDedupRepos(ctx, t, store, slug)
	t.Cleanup(func() { cleanupDedupRepos(ctx, t, store, slug) })

	winnerURL := "https://github.com/" + slug + "_Org/Repo"
	loserURL := strings.ToLower(winnerURL)
	winnerID, loserID := seedDedupPair(ctx, t, store, slug, winnerURL, loserURL)

	// The candidate query flags the pair.
	if _, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_ops.collection_queue SET status = 'collecting' WHERE repo_id = $1`,
		loserID); err != nil {
		t.Fatal(err)
	}
	pair := findPairByLowerGit(ctx, t, store, strings.ToLower(winnerURL))
	if pair == nil {
		t.Fatal("candidate query did not surface the pair")
	}
	if !pair.Collecting {
		t.Error("candidate query must flag the pair as collecting")
	}

	// And the in-transaction recheck refuses even when the caller
	// ignores the flag (the claim can land between query and tx).
	pair.Collecting = false
	err := dedupOnePair(ctx, store, *pair)
	if !errors.Is(err, errPairCollecting) {
		t.Fatalf("dedupOnePair on a collecting loser = %v, want errPairCollecting", err)
	}

	// Nothing was touched.
	var n int
	store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`,
		winnerID, loserID).Scan(&n)
	if n != 2 {
		t.Fatalf("collecting pair must be left fully intact, found %d of 2 rows", n)
	}
}

// seedDedupPair inserts the winner/loser pair with the full child shape
// described in the file header and returns their ids.
func seedDedupPair(ctx context.Context, t *testing.T, store *PostgresStore, slug, winnerURL, loserURL string) (winnerID, loserID int64) {
	t.Helper()
	gid := defaultRepoGroup(ctx, t, store)

	// Seeding a duplicate pair requires the pre-dedup fleet state — the
	// unique backstop cannot coexist with the duplicates it prevents.
	dropCaseUniqueIndex(ctx, t, store)

	mustScan := func(dst *int64, sql string, args ...any) {
		t.Helper()
		if err := store.pool.QueryRow(ctx, sql, args...).Scan(dst); err != nil {
			t.Fatalf("seed %q: %v", sql[:min(60, len(sql))], err)
		}
	}
	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed %q: %v", sql[:min(60, len(sql))], err)
		}
	}

	// Winner first — MIN(repo_id) makes it the winner.
	mustScan(&winnerID, `
		INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
		VALUES ($1, 1, $2, 'Repo', $3) RETURNING repo_id`, gid, winnerURL, slug+"_Org")
	mustScan(&loserID, `
		INSERT INTO aveloxis_data.repos (repo_group_id, platform_id, repo_git, repo_name, repo_owner)
		VALUES ($1, 1, $2, 'repo', $3) RETURNING repo_id`, gid, loserURL, strings.ToLower(slug+"_Org"))

	// Both sides collected the same issue (per-repo duplicated data).
	mustExec(`INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number)
		VALUES ($1, 424242421, 42)`, winnerID)
	var loserIssueID int64
	mustScan(&loserIssueID, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number)
		VALUES ($1, 424242421, 42) RETURNING issue_id`, loserID)

	// The SHARED comment: one messages row, owned by the loser (it
	// collected first), bridged from the loser's issue.
	var msgID int64
	mustScan(&msgID, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id)
		VALUES ($1, 909090901, 1) RETURNING msg_id`, loserID)
	mustExec(`INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id)
		VALUES ($1, $2, $3)`, loserIssueID, loserID, msgID)

	// Loser-side PR + commits + staging.
	mustExec(`INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number)
		VALUES ($1, 565656561, 56)`, loserID)
	mustExec(`INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename)
		VALUES ($1, 'deadbeef'||$2, 'main.go')`, loserID, slug)
	mustExec(`INSERT INTO aveloxis_ops.staging (repo_id, platform_id, entity_type, payload)
		VALUES ($1, 1, 'issue', '{}'::jsonb)`, loserID)

	// Queue rows: both collected (the dominant production shape).
	for _, id := range []int64{winnerID, loserID} {
		mustExec(`INSERT INTO aveloxis_ops.collection_queue (repo_id, status, due_at, last_collected)
			VALUES ($1, 'queued', NOW(), $2) ON CONFLICT (repo_id) DO NOTHING`, id, time.Now())
	}

	// A user group referencing the loser (must be repointed).
	var userID int64
	mustScan(&userID, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ($1, TRUE)
		ON CONFLICT (login_name) DO UPDATE SET admin = TRUE
		RETURNING user_id`, slug+"_user")
	var groupID int64
	mustScan(&groupID, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, $2, 'approved')
		RETURNING group_id`, userID, slug+"_grp")
	mustExec(`INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, groupID, loserID)

	return winnerID, loserID
}

// findPairByLowerGit scans the candidate query output for the test's
// pair — scoped so shared scratch DBs with unrelated duplicates don't
// perturb assertions.
func findPairByLowerGit(ctx context.Context, t *testing.T, store *PostgresStore, lowerGit string) *RepoDupPair {
	t.Helper()
	pairs, err := SampleCaseVariantRepoDups(ctx, store, 10000)
	if err != nil {
		t.Fatalf("SampleCaseVariantRepoDups: %v", err)
	}
	for i := range pairs {
		if pairs[i].LowerGit == lowerGit {
			return &pairs[i]
		}
	}
	return nil
}

// cleanupDedupRepos removes everything a prior run of these tests may
// have left, child tables first.
func cleanupDedupRepos(ctx context.Context, t *testing.T, store *PostgresStore, slug string) {
	t.Helper()
	like := "%" + slug + "%"
	for _, sql := range []string{
		`DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_data.messages WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_data.issues WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_data.pull_requests WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_data.commits WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_ops.staging WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN
		   (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
		`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE $1`,
		`DELETE FROM aveloxis_ops.user_groups WHERE name ILIKE $1`,
		`DELETE FROM aveloxis_ops.users WHERE login_name ILIKE $1`,
	} {
		if _, err := store.pool.Exec(ctx, sql, like); err != nil {
			t.Logf("cleanup (non-fatal): %v", err)
		}
	}

	// Belt-and-suspenders: the shared test message is keyed globally
	// (platform_msg_id), so also remove it by that key in case a prior
	// run left it repointed at a repo outside the slug scope. Runs after
	// the loop so its bridge rows are already gone (RESTRICT).
	if _, err := store.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.messages WHERE platform_msg_id = 909090901 AND platform_id = 1`); err != nil {
		t.Logf("cleanup shared message (non-fatal): %v", err)
	}
}
