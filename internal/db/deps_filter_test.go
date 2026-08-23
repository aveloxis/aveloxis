// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"strings"
	"testing"
)

// v0.28.1 (A3) — the license drill-down: DepsFiltered restricts the
// /deps rows to the SAME row universe the licenses aggregate counts
// (GetRepoLicensesScoped), so "click MIT (6)" lists exactly 6 rows.
// Source-contract pins first, then an AVELOXIS_TEST_DB behavioral
// test covering the normalization-matching trap (aggregate buckets
// are POST-normalization — "MIT License" rows must match the "MIT"
// bucket).

func depsFilteredSrc(t *testing.T) string {
	t.Helper()
	src := readSourceFile(t, "metrics.go")
	i := strings.Index(src, "func (s *PostgresStore) DepsFiltered(")
	if i < 0 {
		t.Fatal("DepsFiltered method missing from metrics.go")
	}
	j := strings.Index(src[i:], "\n}")
	if j < 0 {
		t.Fatal("cannot bound DepsFiltered body")
	}
	return src[i : i+j]
}

// The filtered view must mirror the aggregate's exclusions or the
// drill-down rows won't reconcile with the table counts: the
// githubactions exclusion applies in BOTH scopes, and runtime scope
// excludes the same non-runtime dep types.
func TestDepsFilteredMirrorsAggregateExclusions(t *testing.T) {
	// v0.28.4 (SR-17, review-lens finding): both queries must build
	// their row universe from the ONE shared licenseRowUniverseSQL
	// const — a second inline spelling is exactly the drift this
	// contract exists to prevent.
	body := depsFilteredSrc(t)
	if !strings.Contains(body, "licenseRowUniverseSQL") {
		t.Error("DepsFiltered must compose the shared licenseRowUniverseSQL const (SR-17) — not an inline copy of the aggregate's predicates")
	}
	ts := readSourceFile(t, "timeseries.go")
	if !strings.Contains(ts, "const licenseRowUniverseSQL") {
		t.Fatal("licenseRowUniverseSQL const missing from timeseries.go")
	}
	if !strings.Contains(ts, "package_manager <> 'githubactions'") {
		t.Error("licenseRowUniverseSQL must exclude githubactions rows (both scopes)")
	}
	if !strings.Contains(ts, "NOT IN ('dev','test','build','optional','peer')") {
		t.Error("licenseRowUniverseSQL's runtime scope must exclude the non-runtime types")
	}
	// The aggregate side must use the same const (two references in
	// timeseries.go: the declaration + the query composition).
	if strings.Count(ts, "licenseRowUniverseSQL") < 2 {
		t.Error("GetRepoLicensesScoped must compose licenseRowUniverseSQL too — the shared spelling is only shared if BOTH sides use it")
	}
	// License matching MUST happen post-normalization (Go side) —
	// the aggregate merges synonyms via normalizeLicense, so a raw
	// SQL string match would orphan synonym-form rows.
	if !strings.Contains(body, "normalizeLicense(") {
		t.Error("DepsFiltered must match licenses POST-normalization via normalizeLicense")
	}
}

// The plain Deps path stays byte-identical for existing consumers —
// no filters, full raw list.
func TestPlainDepsPathUnchanged(t *testing.T) {
	src := readSourceFile(t, "metrics.go")
	i := strings.Index(src, "func (s *PostgresStore) Deps(")
	if i < 0 {
		t.Fatal("Deps method missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\n}"); end > 0 {
		body = body[:end]
	}
	if strings.Contains(body, "githubactions") {
		t.Error("plain Deps must stay unfiltered — filters live in DepsFiltered only")
	}
}

// ─── Integration (AVELOXIS_TEST_DB) ─────────────────────────────

func TestDepsFilteredEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

	repoID := seedRepoForDeps(t, store, ctx, "_avdepflt", "fixture")

	seed := func(name, license, depType, mgr string) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_libyear
				(repo_id, name, requirement, type, package_manager, current_version, latest_version, libyear, license)
			VALUES ($1, $2, '*', $3, $4, '1.0.0', '1.0.0', 0, $5)`,
			repoID, name, depType, mgr, license); err != nil {
			t.Fatal(err)
		}
	}
	// Two MIT rows under DIFFERENT raw spellings — the aggregate
	// merges them into one "MIT" bucket, so the drill-down must too.
	seed("_avdepflt_a", "MIT", "", "npm")
	seed("_avdepflt_b", "MIT License", "", "pypi")
	// A dev-scoped MIT row: in the All bucket, out of Runtime.
	seed("_avdepflt_c", "MIT", "dev", "npm")
	// A githubactions row: excluded from the licenses view entirely.
	seed("_avdepflt_d", "MIT", "build", "githubactions")
	// A different license.
	seed("_avdepflt_e", "Apache-2.0", "", "npm")

	all, err := store.DepsFiltered(ctx, repoID, "MIT", false)
	if err != nil {
		t.Fatal(err)
	}
	names := func(rows []DepRow) map[string]bool {
		m := map[string]bool{}
		for _, r := range rows {
			m[r.Name] = true
		}
		return m
	}
	got := names(all)
	if len(all) != 3 || !got["_avdepflt_a"] || !got["_avdepflt_b"] || !got["_avdepflt_c"] {
		t.Errorf("All-scope MIT drill-down: want a+b+c (synonym merged, githubactions excluded), got %v", got)
	}

	runtime, err := store.DepsFiltered(ctx, repoID, "MIT", true)
	if err != nil {
		t.Fatal(err)
	}
	got = names(runtime)
	if len(runtime) != 2 || got["_avdepflt_c"] {
		t.Errorf("Runtime-scope MIT drill-down must drop the dev row, got %v", got)
	}

	// No license filter + runtime scope: the aggregate's runtime row
	// universe (githubactions still excluded).
	scopeOnly, err := store.DepsFiltered(ctx, repoID, "", true)
	if err != nil {
		t.Fatal(err)
	}
	got = names(scopeOnly)
	if got["_avdepflt_d"] || got["_avdepflt_c"] {
		t.Errorf("scope-only filter must exclude githubactions + dev rows, got %v", got)
	}
	if !got["_avdepflt_e"] {
		t.Error("scope-only filter must keep other-license runtime rows")
	}
}

// seedRepoForDeps creates a namespaced repo row + cleanup (deps rows
// cascade-clean via the repo delete's child order).
func seedRepoForDeps(t *testing.T, store *PostgresStore, ctx context.Context, owner, name string) int64 {
	t.Helper()
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, $3, 1)
		ON CONFLICT DO NOTHING
		RETURNING repo_id`,
		"https://github.com/"+owner+"/"+name, owner, name).Scan(&repoID); err != nil {
		// Row already exists from a prior interrupted run — fetch it.
		if err2 := store.pool.QueryRow(ctx, `
			SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1`,
			"https://github.com/"+owner+"/"+name).Scan(&repoID); err2 != nil {
			t.Fatalf("seed repo: %v / %v", err, err2)
		}
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	// Clear any residue from a prior interrupted run.
	if _, err := store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	return repoID
}
