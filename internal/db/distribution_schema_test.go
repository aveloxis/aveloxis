// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.24.0 — Distribution Tracking is a new subsystem that captures
// evidence about whether each repo is distributed via package
// registries. It is INDEPENDENT of the existing dependency-collection
// machinery (`repo_deps_*`) — that tracks what a repo CONSUMES;
// this tracks what a repo PUBLISHES.
//
// Three columns are added to aveloxis_data.repos:
//   - distribution_last_run         TIMESTAMPTZ  — when the last successful scan completed
//   - distribution_failed_attempts  INTEGER      — consecutive failure counter (v0.21.4 pattern)
//   - distribution_last_failed_at   TIMESTAMPTZ  — most recent failure time (drives backoff gate)
//
// Four new tables are added under aveloxis_data:
//   - repo_distribution                  — current registry / GitHub Packages / release-asset evidence
//   - repo_distribution_history          — prior snapshots, rotated on rescan
//   - repo_distribution_manifest         — in-repo manifest evidence (declared package name parsed out)
//   - repo_distribution_manifest_history — prior manifest snapshots, rotated on rescan
//
// Plus a partial index supporting the worker's claim query.

func TestSchemaDeclaresDistributionColumns(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		"distribution_last_run",
		"distribution_failed_attempts",
		"distribution_last_failed_at",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare %s on aveloxis_data.repos — the v0.24.0 DistributionWorker depends on it to claim repos and apply per-row failure backoff", needle)
		}
	}
}

func TestSchemaDeclaresDistributionTables(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution",
		"CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution_history",
		"CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution_manifest",
		"CREATE TABLE IF NOT EXISTS aveloxis_data.repo_distribution_manifest_history",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare the v0.24.0 distribution tracking tables. Missing: %q", needle)
		}
	}
}

func TestSchemaDeclaresDistributionTableColumns(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Required columns on repo_distribution: the registry-fact table.
	// ecosystem + package_name + source are the natural key for upsert.
	// version_count + first/latest published timestamps are the
	// substantive fields. extra JSONB carries per-source detail we
	// don't promote to columns.
	for _, needle := range []string{
		"ecosystem",
		"package_name",
		"version_count",
		"first_published_at",
		"latest_published_at",
		// source distinguishes deps.dev / ecosyste.ms / github_packages /
		// github_release_asset rows — the same (repo, ecosystem,
		// package_name) tuple may appear from multiple sources and we
		// want to know which source said what.
		"source",
		"extra",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare repo_distribution.%s. Missing: %q", needle, needle)
		}
	}

	// Required columns on repo_distribution_manifest: the
	// packaging-intent table.
	for _, needle := range []string{
		"manifest_path",
		"manifest_type",
		"package_name_declared",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must declare repo_distribution_manifest.%s. Missing: %q", needle, needle)
		}
	}
}

func TestSchemaDeclaresDistributionUniqueConstraints(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The (repo_id, ecosystem, package_name, source) tuple must be
	// UNIQUE on repo_distribution so MarkDistributionComplete's
	// rotate-then-insert pattern can reliably DELETE+INSERT without
	// fearing duplicate rows. The same (repo_id, manifest_path) tuple
	// must be UNIQUE on repo_distribution_manifest so a re-scan
	// upserts cleanly even if it skipped the history rotation.
	if !strings.Contains(src, "UNIQUE (repo_id, ecosystem, package_name, source)") {
		t.Error("schema.sql must declare UNIQUE (repo_id, ecosystem, package_name, source) on repo_distribution — required by the store's rotation+upsert path. Without it, a re-scan could silently duplicate rows for repos seen by both deps.dev and ecosyste.ms.")
	}
	if !strings.Contains(src, "UNIQUE (repo_id, manifest_path)") {
		t.Error("schema.sql must declare UNIQUE (repo_id, manifest_path) on repo_distribution_manifest — one row per (repo, manifest_path). A monorepo with subdir/setup.py + setup.py gets two rows; a single file scanned twice in one run does not.")
	}
}

func TestMigrateAddsDistributionColumns(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	columnTypes := map[string]string{
		"distribution_last_run":        "TIMESTAMPTZ",
		"distribution_failed_attempts": "INTEGER DEFAULT 0",
		"distribution_last_failed_at":  "TIMESTAMPTZ",
	}
	for col, typ := range columnTypes {
		needle := `addColumnIfMissing(ctx, pg, logger, &errs, "aveloxis_data.repos", "` + col + `", "` + typ + `"`
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must call addColumnIfMissing for aveloxis_data.repos.%s (%s). Operators upgrading from <v0.24.0 need the column added automatically; missing it breaks DistributionWorker startup", col, typ)
		}
	}
}

func TestMigrateCreatesDistributionDueIndex(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Partial index excludes archived repos; orders NULLS FIRST so
	// never-scanned repos sort to the front of the claim query.
	// Mirrors the v0.21.0 idx_repos_scancode_due pattern exactly.
	for _, needle := range []string{
		"idx_repos_distribution_due",
		"distribution_last_run NULLS FIRST",
		"WHERE COALESCE(repo_archived, FALSE) = FALSE",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must create idx_repos_distribution_due (partial, NULLS FIRST, excludes archived). Missing needle: %q. Without the index, the DistributionWorker's claim query falls back to a sequential scan of repos every dispatcher tick", needle)
		}
	}
}
