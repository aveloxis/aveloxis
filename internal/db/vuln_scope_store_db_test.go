// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestVulnerabilityWritersStoreTheScopeWord — v0.29.71 (the 2026-09-28
// log: the v0.27.51 dependency_scope backfill scanned the whole table on
// every migrate, 7–61 s). The column's rule — a non-self finding stores the
// WORD 'runtime', never "" — was applied by the callers (model.StoredScope),
// so nothing proved "" could not come back and the backfill had to run
// every time. The store now applies it itself (SR-18), both insert paths;
// a project's own advisory ('self') keeps "". With that, the backfill is a
// ledgered one-shot.
func TestVulnerabilityWritersStoreTheScopeWord(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)
	var repoID int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ('https://github.com/_avscope/repo', 'repo', '_avscope', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	row := func(id, purl, kind string) *VulnerabilityRow {
		return &VulnerabilityRow{VulnID: id, PackageName: "p", PackagePurl: purl, Ecosystem: "npm", DependencyKind: kind, DependencyScope: ""}
	}
	if err := store.InsertVulnerability(ctx, repoID, row("GHSA-single", "pkg:npm/p@1.0.0", "direct")); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*VulnerabilityRow{
		row("GHSA-batch", "pkg:npm/p@1.0.0", "transitive"),
		row("GHSA-self", "pkg:npm/p@1.0.0", "self"),
	}); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"GHSA-single": "runtime", "GHSA-batch": "runtime", "GHSA-self": ""} {
		var got string
		if err := store.pool.QueryRow(ctx, `SELECT dependency_scope FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1 AND vuln_id = $2`, repoID, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: dependency_scope = %q, want %q", id, got, want)
		}
	}
}
