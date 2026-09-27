// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
)

// The exposed-repositories table on the dependencies page listed each
// repository's versions as a bare comma list (16 versions of
// brace-expansion in one repository on the production fleet, many of them
// in a dozen lockfiles each) while OPEN FINDINGS was their sum. The per-
// version detail says, for each version, how many open findings it carries
// and how many of the repository's CURRENT lockfiles resolve the package
// to it (0 for a version no lockfile holds, such as a direct dependency
// scanned at its manifest floor), most findings first.

func seedExposureFinding(ctx context.Context, t *testing.T, store *PostgresStore, repo int64, eco, pkg, vuln, purl, kind string, resolved bool) {
	t.Helper()
	res := "NULL"
	if resolved {
		res = "NOW()"
	}
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repo_deps_vulnerabilities
		  (repo_id, vuln_id, package_name, package_purl, ecosystem, severity, cvss_score,
		   dependency_kind, first_detected_at, last_seen_at, resolved_at, tool_source, data_source)
		VALUES ($1, $2, $3, $4, $5, 'HIGH', 7.0, $6, NOW(), NOW(), `+res+`, 'aveloxis', 'test')`,
		repo, vuln, pkg, purl, eco, kind)
}

func seedLockfileRow(ctx context.Context, t *testing.T, store *PostgresStore, repo int64, eco, pkg, version, path string) {
	t.Helper()
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repo_lockfile_packages
		  (repo_id, ecosystem, package_name, resolved_version, lockfile_path, direct)
		VALUES ($1, $2, $3, $4, $5, FALSE)`, repo, eco, pkg, version, path)
}

func detailOf(t *testing.T, repos []PackageExposedRepo, repoID int64) PackageExposedRepo {
	t.Helper()
	for _, r := range repos {
		if r.RepoID == repoID {
			return r
		}
	}
	t.Fatalf("repository %d is not in the exposed list %+v", repoID, repos)
	return PackageExposedRepo{}
}

func TestPackageExposedRepoVersionDetail(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const pkg = "aveloxis-test-brace-expansion"
	cleanup := func(ctx context.Context) {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE package_name ILIKE 'aveloxis-test-brace-expansion%'`)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_lockfile_packages WHERE package_name ILIKE 'aveloxis-test-brace-expansion%'`)
	}
	cleanup(ctx)
	t.Cleanup(func() { cleanup(context.Background()) })

	rA := seedRepoForDeps(t, store, ctx, "aveloxis-it", "versions-detail-a")
	rB := seedRepoForDeps(t, store, ctx, "aveloxis-it", "versions-detail-many")
	purl := func(v string) string { return "pkg:npm/" + pkg + "@" + v }

	// Repository A: 1.1.11 and 2.0.1 carry three open findings each,
	// 1.1.9, 1.1.15 and 9.9.9 one each; a RESOLVED 1.1.11 finding and a
	// versionless finding are not per-version rows.
	for i := 1; i <= 3; i++ {
		seedExposureFinding(ctx, t, store, rA, "npm", pkg, fmt.Sprintf("GHSA-A%d", i), purl("1.1.11"), "transitive", false)
		seedExposureFinding(ctx, t, store, rA, "npm", pkg, fmt.Sprintf("GHSA-B%d", i), purl("2.0.1"), "transitive", false)
	}
	seedExposureFinding(ctx, t, store, rA, "npm", pkg, "GHSA-C1", purl("1.1.9"), "transitive", false)
	seedExposureFinding(ctx, t, store, rA, "npm", pkg, "GHSA-C2", purl("1.1.15"), "transitive", false)
	seedExposureFinding(ctx, t, store, rA, "npm", pkg, "GHSA-C3", purl("9.9.9"), "direct", false) // no lockfile holds it
	seedExposureFinding(ctx, t, store, rA, "npm", pkg, "GHSA-R1", purl("1.1.11"), "transitive", true)
	seedExposureFinding(ctx, t, store, rA, "npm", pkg, "GHSA-V1", "pkg:npm/"+pkg, "transitive", false)

	// Lockfiles: 1.1.11 in three paths (plus a second spelling of the name
	// in one of them, which is the same lockfile); 2.0.1 in one lockfile
	// whose ecosystem is stored in another case; 1.1.9 in two; 1.1.15 only
	// under ANOTHER ecosystem and under a longer name (neither counts).
	seedLockfileRow(ctx, t, store, rA, "npm", pkg, "1.1.11", "apps/a/package-lock.json")
	seedLockfileRow(ctx, t, store, rA, "npm", "Aveloxis-Test-Brace-Expansion", "1.1.11", "apps/a/package-lock.json")
	seedLockfileRow(ctx, t, store, rA, "npm", pkg, "1.1.11", "apps/b/package-lock.json")
	seedLockfileRow(ctx, t, store, rA, "npm", pkg, "1.1.11", "apps/c/yarn.lock")
	seedLockfileRow(ctx, t, store, rA, "NPM", pkg, "2.0.1", "apps/d/pnpm-lock.yaml")
	seedLockfileRow(ctx, t, store, rA, "npm", pkg, "1.1.9", "apps/a/package-lock.json")
	seedLockfileRow(ctx, t, store, rA, "npm", pkg, "1.1.9", "apps/e/package-lock.json")
	seedLockfileRow(ctx, t, store, rA, "pypi", pkg, "1.1.15", "tools/poetry.lock")
	seedLockfileRow(ctx, t, store, rA, "npm", pkg+"-extra", "1.1.15", "apps/a/package-lock.json")
	// A lockfile of ANOTHER repository never counts for A.
	seedLockfileRow(ctx, t, store, rB, "npm", pkg, "9.9.9", "package-lock.json")

	// Repository B: seventeen versions (the realistic width case), one
	// finding each, 1.0.i in i%3 lockfiles.
	for i := 0; i < 17; i++ {
		v := fmt.Sprintf("1.0.%d", i)
		seedExposureFinding(ctx, t, store, rB, "npm", pkg, "GHSA-M"+v, purl(v), "transitive", false)
		for j := 0; j < i%3; j++ {
			seedLockfileRow(ctx, t, store, rB, "npm", pkg, v, fmt.Sprintf("pkg%d/package-lock.json", j))
		}
	}

	type want struct {
		version             string
		findings, lockfiles int
	}
	wantA := []want{{"1.1.11", 3, 3}, {"2.0.1", 3, 1}, {"1.1.9", 1, 2}, {"1.1.15", 1, 0}, {"9.9.9", 1, 0}}
	checkA := func(label string, r PackageExposedRepo) {
		t.Helper()
		if len(r.VersionDetail) != len(wantA) {
			t.Fatalf("%s: version detail %+v, want %d versions", label, r.VersionDetail, len(wantA))
		}
		for i, w := range wantA {
			got := r.VersionDetail[i]
			if got.Version != w.version || got.FindingsUnresolved != w.findings || got.Lockfiles != w.lockfiles {
				t.Errorf("%s: detail[%d] = %+v, want %s with %d findings in %d lockfiles (most findings first, then the version compared numerically)", label, i, got, w.version, w.findings, w.lockfiles)
			}
		}
		if r.FindingsUnresolved != 10 {
			t.Errorf("%s: findings_unresolved=%d, want 10 (nine versioned + the versionless one; the resolved one is out)", label, r.FindingsUnresolved)
		}
		// The existing field keeps its shape: the distinct versions, text order.
		if fmt.Sprint(r.Versions) != "[1.1.11 1.1.15 1.1.9 2.0.1 9.9.9]" {
			t.Errorf("%s: versions=%v, want the unchanged text-ordered list", label, r.Versions)
		}
	}
	checkB := func(label string, r PackageExposedRepo) {
		t.Helper()
		if len(r.VersionDetail) != 17 {
			t.Fatalf("%s: %d versions, want 17", label, len(r.VersionDetail))
		}
		for i, d := range r.VersionDetail {
			if d.Version != fmt.Sprintf("1.0.%d", i) || d.FindingsUnresolved != 1 || d.Lockfiles != i%3 {
				t.Errorf("%s: detail[%d] = %+v, want 1.0.%d with 1 finding in %d lockfiles (1.0.2 before 1.0.10)", label, i, d, i, i%3)
			}
		}
	}

	// A cohort and the fleet give the same per-version detail.
	cohort, err := store.GetPackageExposedRepos(ctx, "npm", pkg, []int64{rA, rB}, 10)
	if err != nil {
		t.Fatal(err)
	}
	checkA("cohort", detailOf(t, cohort, rA))
	checkB("cohort", detailOf(t, cohort, rB))
	fleet, err := store.GetPackageExposedRepos(ctx, "npm", pkg, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	checkA("fleet", detailOf(t, fleet, rA))
	checkB("fleet", detailOf(t, fleet, rB))

	// A cohort without A: B alone, its lockfile for 9.9.9 does not leak.
	onlyB, err := store.GetPackageExposedRepos(ctx, "npm", pkg, []int64{rB}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(onlyB) != 1 || onlyB[0].RepoID != rB {
		t.Fatalf("cohort {B}: %+v, want B only", onlyB)
	}
	checkB("cohort {B}", onlyB[0])
}

// TestPackageExposedRepoLockfileKey — the lockfile rows are matched with
// the lockfile-graph key (LockfileGraphKey): the ecosystem alias fold
// (a finding stored under "gem" matches a Gemfile.lock row stored under
// "rubygems"), the lowercase name, and PyPI's PEP 503 separator fold.
func TestPackageExposedRepoLockfileKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	cleanup := func(ctx context.Context) {
		for _, n := range []string{"aveloxis-test-gemkey", "aveloxis_test.pep503", "Aveloxis_Test.PEP503", "aveloxis-test-pep503x", "aveloxis-test-pct%", "aveloxis-test-pct%-longer", `aveloxis-test-back\slash`, "aveloxis-test-kelvin", "aveloxis-test-\u212Aelvin", "aveloxis-test-hash"} {
			cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE package_name = $1`, n)
			cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_lockfile_packages WHERE package_name = $1`, n)
		}
	}
	cleanup(ctx)
	t.Cleanup(func() { cleanup(context.Background()) })
	r := seedRepoForDeps(t, store, ctx, "aveloxis-it", "versions-detail-key")

	seedExposureFinding(ctx, t, store, r, "gem", "aveloxis-test-gemkey", "GHSA-G1", "pkg:gem/aveloxis-test-gemkey@1.2.3", "direct", false)
	seedLockfileRow(ctx, t, store, r, "rubygems", "aveloxis-test-gemkey", "1.2.3", "Gemfile.lock")
	seedLockfileRow(ctx, t, store, r, "rubygems", "aveloxis-test-gemkey", "1.2.3", "sub/Gemfile.lock")

	seedExposureFinding(ctx, t, store, r, "pypi", "aveloxis_test.pep503", "GHSA-P1", "pkg:pypi/aveloxis-test-pep503@2.0", "direct", false)
	seedLockfileRow(ctx, t, store, r, "pypi", "Aveloxis_Test.PEP503", "2.0", "poetry.lock") // PEP 503: _ and . are -
	seedLockfileRow(ctx, t, store, r, "pypi", "aveloxis-test-pep503x", "2.0", "uv.lock")    // a different package

	// "%" in a name: the Go key, not the prefilter, rejects the longer name.
	seedExposureFinding(ctx, t, store, r, "npm", "aveloxis-test-pct%", "GHSA-X1", "pkg:npm/aveloxis-test-pct%25@1.0.0", "direct", false)
	seedLockfileRow(ctx, t, store, r, "npm", "aveloxis-test-pct%", "1.0.0", "a/package-lock.json")
	seedLockfileRow(ctx, t, store, r, "npm", "aveloxis-test-pct%-longer", "1.0.0", "b/package-lock.json")

	// The prefilter must never DROP a row the key accepts (review of the
	// 2026-09-23 table change): a backslash in a name is escaped (an
	// unescaped one made LIKE skip it and miss the row); a row name Go
	// lowercases to ASCII (the Kelvin sign) is kept; a version the purl
	// percent-escaped ('#' is %23) matches the lockfile's raw one.
	seedExposureFinding(ctx, t, store, r, "npm", `aveloxis-test-back\slash`, "GHSA-BS1", `pkg:npm/aveloxis-test-back\slash@1.0.0`, "direct", false)
	seedLockfileRow(ctx, t, store, r, "npm", `aveloxis-test-back\slash`, "1.0.0", "a/package-lock.json")
	seedExposureFinding(ctx, t, store, r, "npm", "aveloxis-test-kelvin", "GHSA-K1", "pkg:npm/aveloxis-test-kelvin@1.0.0", "direct", false)
	seedLockfileRow(ctx, t, store, r, "npm", "aveloxis-test-\u212Aelvin", "1.0.0", "a/package-lock.json")
	seedExposureFinding(ctx, t, store, r, "npm", "aveloxis-test-hash", "GHSA-H1", "pkg:npm/aveloxis-test-hash@1.0.0%23build", "transitive", false)
	seedLockfileRow(ctx, t, store, r, "npm", "aveloxis-test-hash", "1.0.0#build", "a/package-lock.json")

	for _, tc := range []struct {
		eco, name, version string
		lockfiles          int
	}{
		{"npm", `aveloxis-test-back\slash`, "1.0.0", 1},
		{"npm", "aveloxis-test-kelvin", "1.0.0", 1},
		{"npm", "aveloxis-test-hash", "1.0.0%23build", 1},
		{"gem", "aveloxis-test-gemkey", "1.2.3", 2},
		{"pypi", "aveloxis_test.pep503", "2.0", 1},
		{"npm", "aveloxis-test-pct%", "1.0.0", 1}, // not 2: the longer name below is another package
	} {
		repos, err := store.GetPackageExposedRepos(ctx, tc.eco, tc.name, []int64{r}, 10)
		if err != nil {
			t.Fatal(err)
		}
		got := detailOf(t, repos, r)
		if len(got.VersionDetail) != 1 || got.VersionDetail[0].Version != tc.version || got.VersionDetail[0].Lockfiles != tc.lockfiles {
			t.Errorf("%s/%s: detail %+v, want %s in %d lockfiles", tc.eco, tc.name, got.VersionDetail, tc.version, tc.lockfiles)
		}
	}
}
