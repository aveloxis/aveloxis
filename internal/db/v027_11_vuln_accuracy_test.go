// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.11 — vulnerability version-resolution accuracy. Source pins on
// the schema/migration/upsert contracts + AVELOXIS_TEST_DB integration
// tests for the lockfile snapshot store, the self-set query, and the
// always-refresh classification semantics.

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

// ---------- source pins ----------

func TestVulnSchemaHasResolutionColumns(t *testing.T) {
	schema := mustReadFileStr(t, "schema.sql")
	for _, col := range []string{"declared_requirement", "version_resolution"} {
		if !strings.Contains(schema, col) {
			t.Errorf("schema.sql must declare repo_deps_vulnerabilities.%s", col)
		}
	}
	migrate := mustReadFileStr(t, "migrate.go")
	for _, needle := range []string{
		`"aveloxis_data.repo_deps_vulnerabilities", "declared_requirement"`,
		`"aveloxis_data.repo_deps_vulnerabilities", "version_resolution"`,
		"v0.27.11 create repo_lockfiles",
		"v0.27.11 create repo_lockfile_packages",
	} {
		if !strings.Contains(migrate, needle) {
			t.Errorf("migrate.go missing v0.27.11 step %q", needle)
		}
	}
}

func TestSchemaDeclaresLockfileTables(t *testing.T) {
	schema := mustReadFileStr(t, "schema.sql")
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_data.repo_lockfiles",
		"CREATE TABLE IF NOT EXISTS aveloxis_data.repo_lockfile_packages",
		"UNIQUE (repo_id, lockfile_path)",
		"UNIQUE (repo_id, lockfile_path, package_name, resolved_version)",
		"idx_repo_lockfiles_repo_id",
		"idx_repo_lockfile_packages_repo_id",
	} {
		if !strings.Contains(schema, needle) {
			t.Errorf("schema.sql missing %q", needle)
		}
	}
}

// The classification must track the CURRENT manifest — both upserts
// write declared_requirement/version_resolution unconditionally from
// EXCLUDED, never prefer-nonempty. A prefer-nonempty regression would
// freeze a finding's class at whatever the first post-v0.27.11 scan
// saw (e.g. 'range-floor' forever after the repo adds a lockfile).
func TestVulnUpsertAlwaysRefreshesClassification(t *testing.T) {
	src := mustReadFileStr(t, "vulnerability_store.go")
	if n := strings.Count(src, "declared_requirement = EXCLUDED.declared_requirement"); n < 2 {
		t.Errorf("both vulnerability upserts must SET declared_requirement = EXCLUDED.declared_requirement (found %d)", n)
	}
	if n := strings.Count(src, "version_resolution = EXCLUDED.version_resolution"); n < 2 {
		t.Errorf("both vulnerability upserts must SET version_resolution = EXCLUDED.version_resolution (found %d)", n)
	}
	for _, forbidden := range []string{
		"NULLIF(EXCLUDED.declared_requirement",
		"NULLIF(EXCLUDED.version_resolution",
	} {
		if strings.Contains(src, forbidden) {
			t.Errorf("classification columns must NOT be prefer-nonempty (%q found) — they track the current manifest", forbidden)
		}
	}
}

// The self-set signals: distribution (a), manifest declarations (b),
// and the four-variant name heuristic (c). Exact matches only.
func TestSelfSetQueryUnionsAllThreeSignals(t *testing.T) {
	src := normWS(mustReadFileStr(t, "lockfile_store.go"))
	for _, needle := range []string{
		"LOWER(package_name) FROM aveloxis_data.repo_distribution",
		"LOWER(package_name_declared) FROM aveloxis_data.repo_distribution_manifest",
		"LOWER(r.repo_name)",
		"LOWER(REPLACE(r.repo_name, '_', '-'))",
		"LOWER(r.repo_owner || '-' || r.repo_name)",
		"LOWER(REPLACE(r.repo_owner || '-' || r.repo_name, '_', '-'))",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("GetRepoSelfPackageNames must include signal %q", needle)
		}
	}
	if strings.Contains(src, "LIKE") && strings.Contains(src, "package_name || '%'") {
		t.Error("self-set matching must be EXACT — no prefix rules (distribution tables cover provider-style subpackages)")
	}
}

// HARD RULE tripwire: the vuln-accuracy work must not touch the
// libyear read path. GetRepoLibyearDeps stays byte-stable on its
// column list; the vuln scan reads through its own method.
func TestLibyearReadPathUntouched(t *testing.T) {
	src := mustReadFileStr(t, "analysis_store.go")
	if !strings.Contains(src, "SELECT name, current_version, package_manager, type, COALESCE(license,''), COALESCE(purl,'')") {
		t.Error("GetRepoLibyearDeps's SELECT changed — libyear/SBOM read path must stay untouched (v0.27.11 hard rule); the vuln scan reads via GetRepoDepsForVulnScan instead")
	}
	for _, forbidden := range []string{"repo_lockfile", "SelfPackageNames", "version_resolution"} {
		if strings.Contains(src, forbidden) {
			t.Errorf("analysis_store.go must not reference %q — lockfile/self-dep logic lives in lockfile_store.go, never on the libyear path", forbidden)
		}
	}
}

// ---------- integration (AVELOXIS_TEST_DB) ----------

func lockfileTestStore(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
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
	return store, ctx
}

func seedLockfileTestRepo(t *testing.T, ctx context.Context, store *PostgresStore, owner, name string) int64 {
	t.Helper()
	suffix := time.Now().UnixNano()
	var repoID int64
	err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, $3, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/%s/%s-%d", owner, name, suffix), owner, name).Scan(&repoID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, tbl := range []string{
			"aveloxis_data.repo_lockfile_packages", "aveloxis_data.repo_lockfiles",
			"aveloxis_data.repo_deps_libyear", "aveloxis_data.repo_distribution",
			"aveloxis_data.repo_distribution_manifest", "aveloxis_data.repo_deps_vulnerabilities",
		} {
			_, _ = store.pool.Exec(ctx, `DELETE FROM `+tbl+` WHERE repo_id = $1`, repoID)
		}
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	return repoID
}

func TestLockfileSnapshotRoundTrip(t *testing.T) {
	store, ctx := lockfileTestStore(t)
	repoID := seedLockfileTestRepo(t, ctx, store, "_avlock", "roundtrip")

	inv := []*RepoLockfileInfo{
		{Ecosystem: "npm", LockfilePath: "package-lock.json", LockfileKind: "package-lock.json", EntryCount: 100, DirectCount: 7},
		{Ecosystem: "pypi", LockfilePath: "svc/poetry.lock", LockfileKind: "poetry.lock", EntryCount: 40, DirectCount: 3},
	}
	pkgs := []*RepoLockfilePackage{
		{Ecosystem: "npm", PackageName: "express", ResolvedVersion: "4.19.2", LockfilePath: "package-lock.json", Direct: true},
		{Ecosystem: "pypi", PackageName: "flask", ResolvedVersion: "2.3.3", LockfilePath: "svc/poetry.lock", Direct: true},
		// Same package at a DIFFERENT version in a second lockfile is
		// legitimate (two apps in a monorepo) — both must survive.
		{Ecosystem: "pypi", PackageName: "flask", ResolvedVersion: "3.0.0", LockfilePath: "svc2/poetry.lock", Direct: true},
	}
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, inv, pkgs, nil); err != nil {
		t.Fatal(err)
	}
	locked, err := store.GetRepoLockedVersions(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(locked) != 3 {
		t.Fatalf("want 3 locked packages (distinct versions kept), got %d: %+v", len(locked), locked)
	}

	// Snapshot-replace: a second, smaller snapshot removes the old rows.
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID,
		[]*RepoLockfileInfo{{Ecosystem: "npm", LockfilePath: "package-lock.json", LockfileKind: "package-lock.json", EntryCount: 5, DirectCount: 1}},
		[]*RepoLockfilePackage{{Ecosystem: "npm", PackageName: "express", ResolvedVersion: "5.0.0", LockfilePath: "package-lock.json", Direct: true}},
		nil,
	); err != nil {
		t.Fatal(err)
	}
	locked, err = store.GetRepoLockedVersions(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(locked) != 1 || locked[0].ResolvedVersion != "5.0.0" {
		t.Fatalf("snapshot-replace must delete stale rows; got %+v", locked)
	}

	// Empty snapshot clears everything (last successful walk found none).
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	locked, _ = store.GetRepoLockedVersions(ctx, repoID)
	if len(locked) != 0 {
		t.Fatalf("empty snapshot must clear the tables, got %+v", locked)
	}
}

func TestSelfPackageNamesUnionEndToEnd(t *testing.T) {
	store, ctx := lockfileTestStore(t)
	repoID := seedLockfileTestRepo(t, ctx, store, "apache", "air_flow")

	// Signal (a): published package.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution (repo_id, ecosystem, package_name, source)
		VALUES ($1, 'pypi', 'Apache-Airflow-Providers-Amazon', 'deps.dev')`, repoID); err != nil {
		t.Fatal(err)
	}
	// Signal (b): manifest declaration.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_distribution_manifest (repo_id, manifest_path, manifest_type, package_name_declared)
		VALUES ($1, 'providers/google/pyproject.toml', 'pypi', 'apache-airflow-providers-google')`, repoID); err != nil {
		t.Fatal(err)
	}

	set, err := store.GetRepoSelfPackageNames(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"apache-airflow-providers-amazon", // (a) lowercased
		"apache-airflow-providers-google", // (b)
		"air_flow",                        // (c) repo_name
		"air-flow",                        // (c) '_'→'-'
		"apache-air_flow",                 // (c) owner-name
		"apache-air-flow",                 // (c) owner-name '_'→'-'
	} {
		if !set[want] {
			t.Errorf("self-set must contain %q; got %v", want, set)
		}
	}
	if set["apache-airflow-providers-amazon-extra"] {
		t.Error("self-set must be exact names only")
	}
}

func TestVulnClassificationAlwaysRefreshedEndToEnd(t *testing.T) {
	store, ctx := lockfileTestStore(t)
	repoID := seedLockfileTestRepo(t, ctx, store, "_avlock", "reclass")

	row := &VulnerabilityRow{VulnID: "GHSA-x", PackageName: "flask",
		PackagePurl: "pkg:pypi/flask@2.0.0", Severity: "HIGH", Source: "osv.dev",
		DeclaredRequirement: "flask>=2.0.0", VersionResolution: "range-floor"}
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*VulnerabilityRow{row}); err != nil {
		t.Fatal(err)
	}
	// The repo adds a lockfile; next scan reclassifies. The columns
	// must track the NEW classification (always-refresh), and going
	// back to '' must also stick (never prefer-nonempty).
	row.DeclaredRequirement = "flask>=2.0.0"
	row.VersionResolution = "locked"
	if err := store.InsertVulnerabilityBatch(ctx, repoID, []*VulnerabilityRow{row}); err != nil {
		t.Fatal(err)
	}
	rows, err := store.GetRepoVulnerabilities(ctx, repoID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	if rows[0].VersionResolution != "locked" || rows[0].DeclaredRequirement != "flask>=2.0.0" {
		t.Errorf("classification must refresh on upsert: %+v", rows[0])
	}
}

func TestLockfileCertaintyDerivation(t *testing.T) {
	store, ctx := lockfileTestStore(t)
	repoID := seedLockfileTestRepo(t, ctx, store, "_avlock", "certainty")

	// No dependencies at all → none.
	c, err := store.GetRepoLockfileCertainty(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if c.Overall != "none" || len(c.Ecosystems) != 0 {
		t.Fatalf("no deps must be overall=none, got %+v", c)
	}

	// npm + pypi deps, plus go deps (intrinsically locked).
	for _, ins := range []struct{ name, mgr string }{
		{"express", "npm"}, {"flask", "pypi"}, {"golang.org/x/text", "go"},
	} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_libyear (repo_id, name, package_manager, current_version)
			VALUES ($1, $2, $3, '1.0.0')`, repoID, ins.name, ins.mgr); err != nil {
			t.Fatal(err)
		}
	}

	// Only go covered → partial (go is locked by construction — MVS).
	c, err = store.GetRepoLockfileCertainty(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if c.Overall != "partial" {
		t.Fatalf("go-only coverage must be partial, got %+v", c)
	}
	foundGo := false
	for _, e := range c.Ecosystems {
		if e.Ecosystem == "go" && e.LockfileKind == "go.mod" && e.LockedPackages == 1 {
			foundGo = true
		}
	}
	if !foundGo {
		t.Errorf("go must be synthesized as locked-by-construction (kind go.mod): %+v", c.Ecosystems)
	}

	// Add npm + pypi lockfiles → full.
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID,
		[]*RepoLockfileInfo{
			{Ecosystem: "npm", LockfilePath: "package-lock.json", LockfileKind: "package-lock.json", EntryCount: 10, DirectCount: 1},
			{Ecosystem: "pypi", LockfilePath: "poetry.lock", LockfileKind: "poetry.lock", EntryCount: 10, DirectCount: 1},
		},
		[]*RepoLockfilePackage{
			{Ecosystem: "npm", PackageName: "express", ResolvedVersion: "4.19.2", LockfilePath: "package-lock.json", Direct: true},
			{Ecosystem: "pypi", PackageName: "flask", ResolvedVersion: "2.3.3", LockfilePath: "poetry.lock", Direct: true},
		}, nil); err != nil {
		t.Fatal(err)
	}
	c, err = store.GetRepoLockfileCertainty(ctx, repoID)
	if err != nil {
		t.Fatal(err)
	}
	if c.Overall != "full" {
		t.Fatalf("all ecosystems covered must be full, got %+v", c)
	}
}
