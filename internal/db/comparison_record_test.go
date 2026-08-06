// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// comparison_record_test.go — v0.27.86: Comparisons becomes the
// persistent record of every repo the caller has compared. The
// operator report (2026-08-05): "The comparisons group not having any
// saved anymore is problematic" — for admins (unscoped) and in-scope
// selections, the v0.27.14 out-of-scope auto-add never fired, so
// comparing left no trace at all. Verified on production: user 1
// (admin) had NO Comparisons group despite heavy compare use.

package db

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRecordComparisonReposSourceContract(t *testing.T) {
	src := readSourceFile(t, "home_store.go")
	for _, needle := range []string{
		"func (s *PostgresStore) RecordComparisonRepos(",
		// links must be idempotent — compare fires per metric section
		"ON CONFLICT DO NOTHING",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("home_store.go must contain %q", needle)
		}
	}
	// The insert must join repos so garbage ids (admin URLs are
	// unverified) can never FK-violate or link phantom rows.
	fn := src[strings.Index(src, "func (s *PostgresStore) RecordComparisonRepos("):]
	if end := strings.Index(fn[1:], "\nfunc "); end > 0 {
		fn = fn[:end+1]
	}
	if !strings.Contains(fn, "FROM aveloxis_data.repos") {
		t.Error("RecordComparisonRepos must join aveloxis_data.repos so nonexistent ids are skipped")
	}
	if !strings.Contains(fn, "findOrCreateNamedGroup(") {
		t.Error("RecordComparisonRepos must route group creation through findOrCreateNamedGroup (the shared implicit-group rules)")
	}
}

func TestRecordComparisonReposEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	suffix := time.Now().UnixNano()

	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', TRUE) RETURNING user_id`,
		fmt.Sprintf("_avcmprec_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	mkRepo := func(name string) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, $3, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/_avcmprec%d/%s", suffix, name),
			fmt.Sprintf("_avcmprec%d", suffix), name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	r1, r2 := mkRepo("one"), mkRepo("two")
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, r1, r2)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})

	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// (1) First record: group born approved, both repos linked.
	added, err := store.RecordComparisonRepos(ctx, userID, []int64{r1, r2})
	if err != nil {
		t.Fatalf("RecordComparisonRepos: %v", err)
	}
	if added != 2 {
		t.Errorf("first record must create 2 links, got %d", added)
	}
	var status string
	if err := store.pool.QueryRow(ctx, `
		SELECT status FROM aveloxis_ops.user_groups WHERE user_id = $1 AND name = $2`,
		userID, ComparisonsGroupName).Scan(&status); err != nil {
		t.Fatalf("Comparisons group missing: %v", err)
	}
	if status != "approved" {
		t.Errorf("Comparisons group must be born 'approved', got %q", status)
	}

	// (2) Idempotent re-record.
	added, err = store.RecordComparisonRepos(ctx, userID, []int64{r1, r2})
	if err != nil {
		t.Fatal(err)
	}
	if added != 0 {
		t.Errorf("re-record must add 0 new links, got %d", added)
	}
	if n := count(`
		SELECT COUNT(*) FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_ops.user_groups g USING (group_id)
		WHERE g.user_id = $1 AND g.name = $2`, userID, ComparisonsGroupName); n != 2 {
		t.Errorf("expected exactly 2 links after re-record, got %d", n)
	}

	// (3) Garbage ids are skipped; mixed input links only real repos.
	var maxRepo int64
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(MAX(repo_id), 0) FROM aveloxis_data.repos`).Scan(&maxRepo); err != nil {
		t.Fatal(err)
	}
	added, err = store.RecordComparisonRepos(ctx, userID, []int64{maxRepo + 999999})
	if err != nil || added != 0 {
		t.Errorf("garbage-only record must be a clean no-op, got added=%d err=%v", added, err)
	}

	// (4) Garbage-only for a FRESH user must not leave an empty group.
	var freshID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', FALSE) RETURNING user_id`,
		fmt.Sprintf("_avcmprec_fresh_%d", suffix)).Scan(&freshID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, freshID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, freshID)
	})
	if _, err := store.RecordComparisonRepos(ctx, freshID, []int64{maxRepo + 999999}); err != nil {
		t.Fatal(err)
	}
	if n := count(`SELECT COUNT(*) FROM aveloxis_ops.user_groups WHERE user_id = $1`, freshID); n != 0 {
		t.Errorf("garbage-only record must not create a group, found %d", n)
	}

	// (5) SAFETY: recording never touches collection machinery.
	if n := count(`SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id IN ($1, $2)`, r1, r2); n != 0 {
		t.Errorf("recording a comparison must NEVER create queue rows, found %d", n)
	}
}
