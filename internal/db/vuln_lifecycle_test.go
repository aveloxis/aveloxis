// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.4 — vulnerability lifecycle (historical record + resolved
// marking). Source-contract pins + an AVELOXIS_TEST_DB integration
// test covering the full detect → resolve → reappear cycle.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func TestVulnSchemaHasLifecycleColumns(t *testing.T) {
	schema := mustReadFileStr(t, "schema.sql")
	for _, col := range []string{"first_detected_at", "last_seen_at", "resolved_at"} {
		if !strings.Contains(schema, col) {
			t.Errorf("schema.sql must declare repo_deps_vulnerabilities.%s", col)
		}
	}
	migrate := mustReadFileStr(t, "migrate.go")
	for _, needle := range []string{
		`"aveloxis_data.repo_deps_vulnerabilities", "first_detected_at"`,
		`"aveloxis_data.repo_deps_vulnerabilities", "last_seen_at"`,
		`"aveloxis_data.repo_deps_vulnerabilities", "resolved_at"`,
		"v0.27.4 create user_repo_stars",
	} {
		if !strings.Contains(migrate, needle) {
			t.Errorf("migrate.go missing v0.27.4 step %q", needle)
		}
	}
}

func TestVulnUpsertRefreshesLifecycle(t *testing.T) {
	src := mustReadFileStr(t, "vulnerability_store.go")
	// Both upserts must un-resolve a reappearing vuln and stamp last_seen.
	if strings.Count(src, "last_seen_at = NOW()") < 2 || strings.Count(src, "resolved_at = NULL") < 2 {
		t.Error("both vulnerability upserts must SET last_seen_at = NOW(), resolved_at = NULL on conflict — a reappearing vuln is current again")
	}
	// Counts feed dashboards — they must show live exposure only.
	if strings.Count(src, "resolved_at IS NULL") < 3 {
		t.Error("CountRepoVulnerabilities queries must filter resolved_at IS NULL (live exposure), and GetRepoVulnerabilities must order current-first")
	}
}

func TestScanMarksStaleResolved(t *testing.T) {
	src, err := os.ReadFile("../collector/vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	n := strings.Count(string(src), "MarkStaleVulnerabilitiesResolved(")
	if n < 2 {
		t.Errorf("ScanVulnerabilities must call MarkStaleVulnerabilitiesResolved after a complete scan AND on the zero-purl path (deps table is current truth); found %d call sites", n)
	}
	// The marking must come AFTER the OSV query error return — a partial
	// view must never resolve anything. Pin by order: the queryOSVBatch
	// error return appears before the seen-keys loop.
	s := string(src)
	if strings.Index(s, "OSV batch query failed") > strings.Index(s, "seen := make") {
		t.Error("resolved-marking must happen after the OSV query succeeded, never on the error path")
	}
}

func TestVulnLifecycleEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	suffix := time.Now().UnixNano()
	var repoID int64
	err = store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_avvuln', $2, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avvuln/r%d", suffix),
		fmt.Sprintf("r%d", suffix)).Scan(&repoID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	mk := func(id, purl, sev string, score float64) *VulnerabilityRow {
		return &VulnerabilityRow{VulnID: id, PackageName: "p", PackagePurl: purl,
			Severity: sev, CVSSScore: score, Source: "osv.dev"}
	}
	// Scan 1: two findings.
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*VulnerabilityRow{
		mk("GHSA-aaaa", "pkg:npm/a@1", "CRITICAL", 9.8),
		mk("GHSA-bbbb", "pkg:npm/b@1", "LOW", 2.0),
	}); err != nil {
		t.Fatal(err)
	}
	// Scan 2 reports only GHSA-aaaa → GHSA-bbbb resolves.
	n, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, []string{"GHSA-aaaa|pkg:npm/a@1"})
	if err != nil || n != 1 {
		t.Fatalf("expected exactly 1 resolved, got n=%d err=%v", n, err)
	}
	total, critical, _ := store.CountRepoVulnerabilities(ctx, repoID)
	if total != 1 || critical != 1 {
		t.Errorf("counts must exclude resolved rows: total=%d critical=%d (want 1/1)", total, critical)
	}
	rows, err := store.GetRepoVulnerabilities(ctx, repoID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("historical record must be kept: %d rows err=%v", len(rows), err)
	}
	if rows[0].VulnID != "GHSA-aaaa" || rows[0].ResolvedAt != nil {
		t.Errorf("current findings must sort first; got %s resolved=%v", rows[0].VulnID, rows[0].ResolvedAt)
	}
	if rows[1].ResolvedAt == nil {
		t.Error("GHSA-bbbb must carry resolved_at")
	}
	// Scan 3: GHSA-bbbb reappears → the upsert un-resolves it.
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*VulnerabilityRow{
		mk("GHSA-bbbb", "pkg:npm/b@1", "LOW", 2.0),
	}); err != nil {
		t.Fatal(err)
	}
	rows, _ = store.GetRepoVulnerabilities(ctx, repoID)
	for _, v := range rows {
		if v.VulnID == "GHSA-bbbb" && v.ResolvedAt != nil {
			t.Error("a reappearing vulnerability must be un-resolved by the upsert")
		}
	}
	// Empty scan resolves everything.
	if _, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, nil); err != nil {
		t.Fatal(err)
	}
	total, _, _ = store.CountRepoVulnerabilities(ctx, repoID)
	if total != 0 {
		t.Errorf("after an empty complete scan every finding must be resolved, got %d current", total)
	}
}

func TestStarAndHomeRepos(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	suffix := time.Now().UnixNano()
	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email)
		VALUES ($1, 'github', '') RETURNING user_id`,
		fmt.Sprintf("_avhome_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_avhome', $2, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avhome/r%d", suffix),
		fmt.Sprintf("r%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repo_stars WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// Starred repos appear on the home list even with no group linkage.
	if err := store.StarRepo(ctx, userID, repoID); err != nil {
		t.Fatal(err)
	}
	if err := store.StarRepo(ctx, userID, repoID); err != nil {
		t.Fatalf("StarRepo must be idempotent: %v", err)
	}
	repos, err := store.GetHomeRepos(ctx, userID, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, h := range repos {
		if h.RepoID == repoID {
			found = true
			if !h.Starred {
				t.Error("home row must carry starred=true")
			}
		}
	}
	if !found {
		t.Error("a starred repo must always appear on the home list")
	}
	if err := store.UnstarRepo(ctx, userID, repoID); err != nil {
		t.Fatal(err)
	}
	repos, _ = store.GetHomeRepos(ctx, userID, 10)
	for _, h := range repos {
		if h.RepoID == repoID {
			t.Error("unstarred repo with no group linkage must drop off the home list")
		}
	}
}

func mustReadFileStr(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestFindOrCreateStarredGroup(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	suffix := time.Now().UnixNano()
	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email)
		VALUES ($1, 'github', '') RETURNING user_id`,
		fmt.Sprintf("_avstargrp_%d", suffix)).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_avstargrp', $2, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avstargrp/r%d", suffix),
		fmt.Sprintf("r%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	// First call creates; second call reuses — never a duplicate group.
	g1, err := store.FindOrCreateStarredGroup(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	g2, err := store.FindOrCreateStarredGroup(ctx, userID)
	if err != nil || g1 != g2 {
		t.Fatalf("Starred group must be reused: %d vs %d (err=%v)", g1, g2, err)
	}

	// The star auto-add flow: repo added to the Starred group → in scope
	// even though the group is PENDING (non-admin user, v0.19.0 rules).
	if _, err := store.AddRepoToGroupByID(ctx, g1, repoID); err != nil {
		t.Fatal(err)
	}
	scope, err := store.GetUserRepoScope(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range scope {
		if id == repoID {
			found = true
		}
	}
	if !found {
		t.Error("a repo starred into the (pending) Starred group must be in scope — approval gates collection, not visibility")
	}
}
