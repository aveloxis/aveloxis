// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.50 — archived visibility + last-activity ceiling. Triggered by
// broadinstitute/gatk-protected (archived 2017): the dashboard showed
// all-time totals over empty charts with no explanation, because (a)
// the forge's archived bit reached repo_info.status but no API exposed
// it, and (b) repos.repo_archived was only ever set by prelim's
// dead-repo path — 17,665 forge-archived repos carried the flag FALSE.

package db

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestRepoStatsCarriesArchivedAndLastActivity(t *testing.T) {
	src, err := os.ReadFile("repo_stats.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, needle := range []string{
		"`json:\"archived\"`",
		"`json:\"last_activity_at,omitempty\"`",
		`COALESCE(status, '')`,
		`status == "Archived"`,
		"s.LastActivityAt(ctx",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("repo_stats.go missing v0.27.50 wiring %q — the GUI chip and the "+
				"historical chart window read these fields", needle)
		}
	}
}

// TestLastActivityAtShape pins the deliberate differences from its
// FirstActivityAt mirror: GREATEST over MAX timestamps, NO
// repos.created_at (creation is not activity), and NO process-lifetime
// cache (last activity ADVANCES as a repo collects — the v0.27.24
// first-activity cache is safe only because first activity is
// immutable).
func TestLastActivityAtShape(t *testing.T) {
	src, err := os.ReadFile("analytics_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	at := strings.Index(code, "func (s *PostgresStore) LastActivityAt(")
	if at < 0 {
		t.Fatal("LastActivityAt not found")
	}
	end := strings.Index(code[at:], "\nfunc ")
	body := code[at : at+end]
	if !strings.Contains(body, "GREATEST(") {
		t.Error("LastActivityAt must take the GREATEST across issue/PR/commit maxima")
	}
	if strings.Contains(body, "FROM aveloxis_data.repos") {
		t.Error("LastActivityAt must NOT read repos.created_at — creation is not activity " +
			"(FirstActivityAt includes it as a floor; the ceiling must not)")
	}
}

// Integration: the full chain against the real schema.
func TestArchivedStatusAndLastActivityEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("integration test; set AVELOXIS_TEST_DB")
	}
	store, ctx := v0251Connect(t)
	const slug = "_avarchived_e2e"
	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
			`DELETE FROM aveloxis_data.repo_info WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE $1`,
		} {
			_, _ = store.pool.Exec(ctx, sql, "%"+slug+"%")
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'org', $2, 1) RETURNING repo_id`,
		"https://github.com/org/"+slug, slug).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	// Latest snapshot says Archived; an old commit is the last activity.
	lastCommit := time.Date(2017, 6, 26, 14, 32, 40, 0, time.UTC)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_info (repo_id, status, commit_count, data_collection_date)
		VALUES ($1, 'Archived', 814, NOW())`, repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp)
		VALUES ($1, 'aaaa000011112222333344445555666677778888', 'f.go', $2)`, repoID, lastCommit); err != nil {
		t.Fatal(err)
	}

	st, err := store.GetRepoStats(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Archived {
		t.Error("repo_info.status='Archived' must surface as stats.archived=true")
	}
	if st.LastActivityAt == nil || !st.LastActivityAt.Equal(lastCommit) {
		t.Errorf("last_activity_at = %v, want the 2017 commit timestamp %v", st.LastActivityAt, lastCommit)
	}

	// (b) reconciliation write path: UpdateRepoMetadata propagates
	// archived BOTH directions onto repos.repo_archived.
	if err := store.UpdateRepoMetadata(ctx, repoID, "d", "Go", nil, true); err != nil {
		t.Fatal(err)
	}
	var flag bool
	if err := store.pool.QueryRow(ctx,
		`SELECT COALESCE(repo_archived, FALSE) FROM aveloxis_data.repos WHERE repo_id=$1`, repoID).Scan(&flag); err != nil {
		t.Fatal(err)
	}
	if !flag {
		t.Error("UpdateRepoMetadata(archived=true) must set repos.repo_archived")
	}
	if err := store.UpdateRepoMetadata(ctx, repoID, "d", "Go", nil, false); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT COALESCE(repo_archived, FALSE) FROM aveloxis_data.repos WHERE repo_id=$1`, repoID).Scan(&flag); err != nil {
		t.Fatal(err)
	}
	if flag {
		t.Error("un-archiving on the forge must flip repos.repo_archived back to false")
	}

	// Empty repo set: honest ok=false, not a zero time.
	if _, ok, err := store.LastActivityAt(ctx, []int64{}); err != nil || ok {
		t.Errorf("empty repo set must return ok=false, got ok=%v err=%v", ok, err)
	}
}

// TestArchivedReconcileMigration pins the one-shot backfill: latest
// repo_info snapshot per repo, IS DISTINCT FROM idempotency, both
// directions via the boolean expression (not a one-way TRUE-setter).
func TestArchivedReconcileMigration(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, needle := range []string{
		"v0.27.50 reconcile repo_archived from forge status",
		"SET repo_archived = (latest.status = 'Archived')",
		"DISTINCT ON (repo_id)",
		"IS DISTINCT FROM (latest.status = 'Archived')",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("migrate.go missing v0.27.50 reconcile needle %q", needle)
		}
	}
}

// TestMetadataCallersPassForgeArchived pins that both
// UpdateRepoMetadata callers derive archived from the forge status —
// the flag stays current in both directions going forward.
func TestMetadataCallersPassForgeArchived(t *testing.T) {
	for _, f := range []string{
		"../collector/staged.go",
		"../scheduler/repo_metadata_backfill.go",
	} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(src), `info.Status == "Archived"`) {
			t.Errorf("%s must pass info.Status == \"Archived\" to UpdateRepoMetadata — "+
				"without it, repos.repo_archived drifts from the forge again", f)
		}
	}
}
