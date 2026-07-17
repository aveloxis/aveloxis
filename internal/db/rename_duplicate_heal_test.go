// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.22 rename-duplicate self-heal tests. Live incident 2026-07-17:
// four adds by old names (danswer-ai/danswer → onyx-dot-app/onyx etc.)
// created dataless duplicate rows whose group links pointed at nothing
// while the collected winners sat one FindRepoByURL-miss away.

package db

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Source-contract pins (unit tier)
// ---------------------------------------------------------------------------

// TestHealRenamedDuplicateShape pins the load-bearing pieces: the
// never-collected gate, both repoints (links AND stars), the full
// delete list, and the FK-violation fail-safe (the repos FKs are
// DEFERRABLE, so the violation surfaces at COMMIT — both Exec and
// Commit must route through the check).
func TestHealRenamedDuplicateShape(t *testing.T) {
	body := extractFunctionBody(t, "rename_duplicate_heal.go", "HealRenamedDuplicate")
	for _, needle := range []string{
		"last_collected IS NOT NULL", // never-collected gate
		"user_repo_stars",
		"user_repos",
		"collection_status",
		"staging",
		"collection_queue",
		"DELETE FROM aveloxis_data.repos WHERE repo_id = $1",
		"isFKViolation",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("HealRenamedDuplicate missing %q", needle)
		}
	}
	// The commit error must ALSO route through the FK fail-safe —
	// DEFERRABLE FKs violate at COMMIT, not at Exec.
	commitIdx := strings.Index(body, "tx.Commit(ctx)")
	if commitIdx < 0 || !strings.Contains(body[commitIdx:], "isFKViolation") {
		t.Error("the Commit error path must treat SQLSTATE 23503 as healed=false " +
			"(DEFERRABLE FKs surface child-row violations at COMMIT)")
	}
}

// TestPrelimWiresRenameDuplicateHeal pins prelim's duplicate branch:
// heal first; legacy skip+dequeue only as the fallback.
func TestPrelimWiresRenameDuplicateHeal(t *testing.T) {
	src, err := os.ReadFile("../collector/prelim.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "store.HealRenamedDuplicate(ctx, repo.ID, existingID)") {
		t.Fatal("prelim's duplicate branch must attempt HealRenamedDuplicate before the legacy dequeue")
	}
	healIdx := strings.Index(s, "HealRenamedDuplicate")
	deqIdx := strings.Index(s[healIdx:], "DequeueRepo")
	if deqIdx < 0 {
		t.Error("the legacy DequeueRepo fallback must survive for the healed=false case")
	}
}

// ---------------------------------------------------------------------------
// Integration tier (AVELOXIS_TEST_DB)
// ---------------------------------------------------------------------------

// TestHealRenamedDuplicateEndToEnd reproduces the 2026-07-17 shape:
// a collected winner and a never-collected duplicate whose group link
// + star point at the dataless row. The heal must land every link on
// the winner, delete the duplicate everywhere, refuse to touch a
// COLLECTED duplicate, and error on a degenerate pair.
func TestHealRenamedDuplicateEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', TRUE) RETURNING user_id`,
		fmt.Sprintf("_avheal_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var groupID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
		VALUES ($1, $2, 'approved') RETURNING group_id`,
		userID, fmt.Sprintf("heal-%d", suffix)).Scan(&groupID); err != nil {
		t.Fatal(err)
	}
	mkRepo := func(name string) int64 {
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
			VALUES ($1, '_avheal', $2, 1, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/_avheal/%s%d", name, suffix),
			fmt.Sprintf("%s%d", name, suffix)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	winner := mkRepo("winner")
	dup := mkRepo("dup")
	t.Cleanup(func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_ops.user_repo_stars WHERE repo_id = ANY($1::bigint[])`,
			`DELETE FROM aveloxis_ops.user_repos WHERE repo_id = ANY($1::bigint[])`,
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = ANY($1::bigint[])`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = ANY($1::bigint[])`,
		} {
			pool.Exec(ctx, q, []int64{winner, dup})
		}
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})

	// Winner: collected. Duplicate: queued, never collected, linked +
	// starred (the exact live shape).
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at, last_collected)
		VALUES ($1, 'queued', 100, NOW(), NOW())`, winner); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at)
		VALUES ($1, 'queued', 100, NOW())`, dup); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, groupID, dup); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id) VALUES ($1, $2)`, userID, dup); err != nil {
		t.Fatal(err)
	}

	healed, err := store.HealRenamedDuplicate(ctx, dup, winner)
	if err != nil || !healed {
		t.Fatalf("heal: healed=%v err=%v", healed, err)
	}

	var n int
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE group_id = $1 AND repo_id = $2`,
		groupID, winner).Scan(&n)
	if n != 1 {
		t.Error("group link must land on the winner")
	}
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.user_repo_stars WHERE user_id = $1 AND repo_id = $2`,
		userID, winner).Scan(&n)
	if n != 1 {
		t.Error("star must land on the winner")
	}
	for _, q := range []string{
		`SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE repo_id = $1`,
		`SELECT COUNT(*) FROM aveloxis_ops.user_repo_stars WHERE repo_id = $1`,
	} {
		pool.QueryRow(ctx, q, dup).Scan(&n)
		if n != 0 {
			t.Errorf("duplicate residue remains: %s → %d", q, n)
		}
	}

	// A COLLECTED duplicate must be refused (healed=false, retained).
	dup2 := mkRepo("dup2")
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, dup2)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, dup2)
	})
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at, last_collected)
		VALUES ($1, 'queued', 100, NOW(), NOW())`, dup2); err != nil {
		t.Fatal(err)
	}
	healed, err = store.HealRenamedDuplicate(ctx, dup2, winner)
	if err != nil || healed {
		t.Fatalf("collected duplicate must be retained: healed=%v err=%v", healed, err)
	}
	pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_id = $1`, dup2).Scan(&n)
	if n != 1 {
		t.Error("collected duplicate's repos row must survive")
	}

	// Degenerate pairs error.
	if _, err := store.HealRenamedDuplicate(ctx, winner, winner); err == nil {
		t.Error("dup==winner must error")
	}
}
